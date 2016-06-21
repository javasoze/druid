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
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.emitter.EmittingLogger;
import io.druid.lucene.LuceneDirectory;
import io.druid.lucene.query.search.search.SearchHit;
import io.druid.lucene.query.search.search.SearchQuery;
import io.druid.lucene.query.search.search.SearchQuerySpec;
import io.druid.query.Druids;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.Result;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.DimensionSelector;
import io.druid.segment.data.IndexedInts;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 */
public class SearchQueryRunner implements QueryRunner<Result<SearchResultValue>>
{
  private static final EmittingLogger log = new EmittingLogger(SearchQueryRunner.class);
  private final LuceneDirectory directory;

  public SearchQueryRunner(LuceneDirectory directory)
  {
    this.directory = directory;
  }

  @Override
  public Sequence<Result<SearchResultValue>> run(
      final Query<Result<SearchResultValue>> input,
      Map<String, Object> responseContext
  )
  {
//    if (!(input instanceof SearchQuery)) {
//      throw new ISE("Got a [%s] which isn't a %s", input.getClass(), SearchQuery.class);
//    }
//
//    final SearchQuery query = (SearchQuery) input;
//    final String filter = query.getDimensionsFilter();
//    final List<DimensionSpec> dimensions = query.getDimensions();
//    final SearchQuerySpec searchQuerySpec = query.getQuery();
//    final int limit = query.getLimit();
//    final boolean descending = query.isDescending();
//    final TreeMap<SearchHit, MutableInt> retVal = Maps.newTreeMap(query.getSort().getComparator());
//
//    try {
//      IndexReader reader = directory.getIndexReader();
//      Sequence<LeafReader> leafReaders = Sequences.map(
//              Sequences.simple(reader.leaves()),
//              new Function<LeafReaderContext, LeafReader>()
//              {
//                @Nullable
//                @Override
//                public LeafReader apply(@Nullable LeafReaderContext leafReaderContext) {
//                  return leafReaderContext.reader();
//                }
//              }
//      );
//      LeafReader leafReader = (LeafReader)reader;
//      Iterable<DimensionSpec> dimsToSearch;
//      if (dimensions == null || dimensions.isEmpty()) {
//        dimsToSearch = Iterables.transform(directory.getFieldTypes().keySet(), Druids.DIMENSION_IDENTITY);
//      } else {
//        dimsToSearch = dimensions;
//      }
//
//      Map<String, DimensionSelector> dimSelectors = Maps.newHashMap();
//      for (DimensionSpec dim : dimsToSearch) {
//        dimSelectors.put(
//                dim.getOutputName(),
//                cursor.makeDimensionSelector(dim)
//        );
//      }
//
//      while (!cursor.isDone()) {
//        for (Map.Entry<String, DimensionSelector> entry : dimSelectors.entrySet()) {
//          final DimensionSelector selector = entry.getValue();
//
//          if (selector != null) {
//            final IndexedInts vals = selector.getRow();
//            for (int i = 0; i < vals.size(); ++i) {
//              final String dimVal = selector.lookupName(vals.get(i));
//              if (searchQuerySpec.accept(dimVal)) {
//                MutableInt counter = new MutableInt(1);
//                MutableInt prev = set.put(new SearchHit(entry.getKey(), dimVal), counter);
//                if (prev != null) {
//                  counter.add(prev.intValue());
//                }
//                if (set.size() >= limit) {
//                  return set;
//                }
//              }
//            }
//          }
//        }
//
//        cursor.advance();
//      }
//    } catch (IOException e) {
//      throw new IAE(e, "");
//    }

    return null;
  }

  private Sequence<Result<SearchResultValue>> makeReturnResult(
      int limit, TreeMap<SearchHit, MutableInt> retVal)
  {
//    Iterable<SearchHit> source = Iterables.transform(
//        retVal.entrySet(), new Function<Map.Entry<SearchHit, MutableInt>, SearchHit>()
//        {
//          @Override
//          public SearchHit apply(Map.Entry<SearchHit, MutableInt> input)
//          {
//            SearchHit hit = input.getKey();
//            return new SearchHit(hit.getDimension(), hit.getValue(), input.getValue().intValue());
//          }
//        }
//    );
//    return Sequences.simple(
//        ImmutableList.of(
//            new Result<SearchResultValue>(
//                segment.getDataInterval().getStart(),
//                new SearchResultValue(
//                    Lists.newArrayList(new FunctionalIterable<SearchHit>(source).limit(limit))
//                )
//            )
//        )
//    );
    return null;
  }
}
