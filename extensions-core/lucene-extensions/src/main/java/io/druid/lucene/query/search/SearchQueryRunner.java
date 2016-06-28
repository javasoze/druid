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

package io.druid.lucene.query.search;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.emitter.EmittingLogger;
import io.druid.lucene.LuceneDirectory;
import io.druid.lucene.query.LuceneCursor;
import io.druid.lucene.query.search.search.SearchHit;
import io.druid.lucene.query.search.search.SearchQuery;
import io.druid.lucene.segment.DimensionSelector;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.Result;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.Segment;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 */
public class SearchQueryRunner implements QueryRunner<Result<SearchResultValue>>
{
  private static final EmittingLogger log = new EmittingLogger(SearchQueryRunner.class);
  private final Segment segment;

  public SearchQueryRunner(Segment segment)
  {
    this.segment = segment;
  }

  @Override
  public Sequence<Result<SearchResultValue>> run(
      final Query<Result<SearchResultValue>> input,
      Map<String, Object> responseContext
  )
  {
    if (!(input instanceof SearchQuery)) {
      throw new ISE("Got a [%s] which isn't a %s", input.getClass(), SearchQuery.class);
    }

    final SearchQuery query = (SearchQuery) input;
    final String filter = query.getDimensionsFilter();
    final List<DimensionSpec> dimensions = query.getDimensions();
    final int limit = query.getLimit();
    final boolean descending = query.isDescending();

    final LuceneDirectory directory = segment.as(LuceneDirectory.class);
    try {
      IndexReader reader = directory.getIndexReader();
      QueryParser parser = new QueryParser("", new SimpleAnalyzer());
      final org.apache.lucene.search.Query luceneQuery = parser.parse(filter);
      final TreeMap<SearchHit, MutableInt> retVal = Maps.newTreeMap(query.getSort().getComparator());
      for(LeafReaderContext leafReaderContext : reader.leaves() ) {
        LeafReader leafReader = leafReaderContext.reader();
        LuceneCursor cursor = new LuceneCursor(luceneQuery, leafReader, directory.getFieldTypes());
        Map<String, DimensionSelector> dimensionSelectors = new HashMap<>(dimensions.size());
        for (DimensionSpec dimension : dimensions) {
          dimensionSelectors.put(dimension.getDimension(), cursor.makeDimensionSelector(dimension.getDimension()));
        }

        while (!cursor.isDone()) {
          for (String dim : dimensionSelectors.keySet()) {
            DimensionSelector selector = dimensionSelectors.get(dim);
            List<String> list = selector.getValues();
            for (String dimVal: list) {
              MutableInt counter = new MutableInt(1);
              MutableInt prev = retVal.put(new SearchHit(dim, dimVal), counter);
              if (prev != null) {
                counter.add(prev.intValue());
              }
              if (retVal.size() >= limit) {
                return makeReturnResult(limit, retVal);
              }
            }
          }
          cursor.advance();
        }
      }
      return makeReturnResult(limit, retVal);
    } catch (IOException|ParseException e) {
      throw new IAE("");
    }
  }

  private Sequence<Result<SearchResultValue>> makeReturnResult(
      int limit, TreeMap<SearchHit, MutableInt> retVal)
  {
    Iterable<SearchHit> source = Iterables.transform(
        retVal.entrySet(), new Function<Map.Entry<SearchHit, MutableInt>, SearchHit>()
        {
          @Override
          public SearchHit apply(Map.Entry<SearchHit, MutableInt> input)
          {
            SearchHit hit = input.getKey();
            return new SearchHit(hit.getDimension(), hit.getValue(), input.getValue().intValue());
          }
        }
    );
    return Sequences.simple(
        ImmutableList.of(
            new Result<SearchResultValue>(
                    segment.getDataInterval().getStart(),
                new SearchResultValue(
                    Lists.newArrayList(new FunctionalIterable<SearchHit>(source).limit(limit))
                )
            )
        )
    );
  }
}
