/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.lucene.query.select;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.sun.org.apache.regexp.internal.RE;
import io.druid.lucene.LuceneDirectory;
import io.druid.lucene.query.groupby.LuceneCursor;
import io.druid.lucene.segment.DimensionSelector;
import io.druid.query.Result;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.Segment;
import io.druid.timeline.DataSegmentUtils;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.solr.search.DocSetCollector;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 */
public class SelectQueryEngine
{
  public Sequence<Result<SelectResultValue>> process(final SelectQuery query, final Segment segment)
  {
    final LuceneDirectory directory = segment.as(LuceneDirectory.class);
    if (directory == null) {
      throw new ISE(
          "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
      );
    }

    // at the point where this code is called, only one datasource should exist.
    String dataSource = Iterables.getOnlyElement(query.getDataSource().getNames());

    final Iterable<DimensionSpec> dims;
    if (query.getDimensions() == null || query.getDimensions().isEmpty()) {
      dims = DefaultDimensionSpec.toSpec(directory.getFieldTypes().keySet());
    } else {
      dims = query.getDimensions();
    }

    List<Interval> intervals = query.getQuerySegmentSpec().getIntervals();
    Preconditions.checkArgument(intervals.size() == 1, "Can only handle a single interval, got[%s]", intervals);

    // should be rewritten with given interval
    final String segmentId = DataSegmentUtils.withInterval(dataSource, segment.getIdentifier(), intervals.get(0));

    try {
      IndexReader reader = directory.getIndexReader();
      QueryParser parser = new QueryParser("", new SimpleAnalyzer());
      final Query luceneQuery = parser.parse(query.getDimensionsFilter());
      Sequence<LeafReader> leafReaders = Sequences.map(
              Sequences.simple(reader.leaves()),
              new Function<LeafReaderContext, LeafReader>()
              {
                @Nullable
                @Override
                public LeafReader apply(@Nullable LeafReaderContext leafReaderContext) {
                  return leafReaderContext.reader();
                }
              }
      );
      final PagingOffset offset = query.getPagingOffset(segmentId);
      final ReferenceCount count = new ReferenceCount(offset.startDelta());
      return Sequences.concat(
              Sequences.map(
                      leafReaders,
                      new Function<LeafReader, Sequence<Result<SelectResultValue>>>() {
                        @Override
                        public Sequence<Result<SelectResultValue>> apply(final LeafReader leafReader) {
                          IndexSearcher searcher = new IndexSearcher(leafReader);
                          DocSetCollector collector = new DocSetCollector(1000, 100000);
                          try {
                            searcher.search(luceneQuery, collector);
                          } catch (IOException e) {

                          }
                          int lastOffset = offset.startOffset();
                          LuceneCursor cursor = new LuceneCursor(null, directory.getFieldTypes());
                          cursor.reset(collector.getDocSet().iterator());
                          int left = cursor.advanceTo(count.getCount());
                          count.setCount(left);
                          if (left != 0) {
                            return Sequences.simple(
                                    new ArrayList<Result<SelectResultValue>>()
                            );
                          }

                          final Map<String, DimensionSelector> dimSelectors = Maps.newHashMap();
                          for (DimensionSpec dim : dims) {
                            dimSelectors.put(dim.getDimension(), cursor.makeDimensionSelector(dim.getDimension()));
                          }

                          final SelectResultValueBuilder builder = new SelectResultValueBuilder(
                                  directory.getDataInterval().getStart(),
                                  query.getPagingSpec(),
                                  query.isDescending()
                          );
                          DimensionSelector timeSelector = cursor.makeTimestampSelector();
                          while(!cursor.isDone()) {
                            for (; !cursor.isDone() && offset.hasNext(); cursor.advance(), offset.next()) {
                              final Map<String, Object> theEvent = Maps.newLinkedHashMap();
                              theEvent.put(EventHolder.timestampKey, new DateTime(timeSelector.getValues().get(0)));

                              for (Map.Entry<String, DimensionSelector> dimSelector : dimSelectors.entrySet()) {
                                final String dim = dimSelector.getKey();
                                final DimensionSelector selector = dimSelector.getValue();

                                if (selector == null) {
                                  theEvent.put(dim, null);
                                } else {
                                  final List vals = selector.getValues();

                                  if (vals.size() == 1) {
                                    theEvent.put(dim, vals.get(0));
                                  } else {
                                    List<Object> dimVals = Lists.newArrayList();
                                    for (int i = 0; i < vals.size(); ++i) {
                                      dimVals.add(vals.get(i));
                                    }
                                    theEvent.put(dim, dimVals);
                                  }
                                }
                              }

                              builder.addEntry(
                                      new EventHolder(
                                              segmentId,
                                              lastOffset = offset.current(),
                                              theEvent
                                      )
                              );
                            }
                          }

                          builder.finished(segmentId, lastOffset);

                          return  Sequences.simple(
                                  ImmutableList.of( builder.build())
                          );
                        }
                      }
              ));
    } catch (IOException|ParseException e) {
      throw new IAE("");
    }
  }

  class ReferenceCount {
    private int count;

    ReferenceCount(int count) {
      this.count = count;
    }

    public void setCount(int count) {
      this.count = count;
    }

    public int getCount() {
      return count;
    }
  }
}
