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

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.guava.nary.BinaryFn;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.lucene.query.search.search.SearchHit;
import io.druid.lucene.query.search.search.SearchQuery;
import io.druid.lucene.query.search.search.SearchQueryConfig;
import io.druid.query.*;
import io.druid.query.aggregation.MetricManipulationFn;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 */
public class SearchQueryQueryToolChest extends QueryToolChest<Result<SearchResultValue>, SearchQuery>
{
  private static final byte SEARCH_QUERY = 0x2;
  private static final TypeReference<Result<SearchResultValue>> TYPE_REFERENCE = new TypeReference<Result<SearchResultValue>>()
  {
  };
  private static final TypeReference<Object> OBJECT_TYPE_REFERENCE = new TypeReference<Object>()
  {
  };

  private final SearchQueryConfig config;

  private final IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator;

  @Inject
  public SearchQueryQueryToolChest(
      SearchQueryConfig config,
      IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator
  )
  {
    this.config = config;
    this.intervalChunkingQueryRunnerDecorator = intervalChunkingQueryRunnerDecorator;
  }

  @Override
  public QueryRunner<Result<SearchResultValue>> mergeResults(
      QueryRunner<Result<SearchResultValue>> runner
  )
  {
    return new ResultMergeQueryRunner<Result<SearchResultValue>>(runner)
    {
      @Override
      protected Ordering<Result<SearchResultValue>> makeOrdering(Query<Result<SearchResultValue>> query)
      {
        return ResultGranularTimestampComparator.create(
            ((SearchQuery) query).getGranularity(),
            query.isDescending()
        );
      }

      @Override
      protected BinaryFn<Result<SearchResultValue>, Result<SearchResultValue>, Result<SearchResultValue>> createMergeFn(
          Query<Result<SearchResultValue>> input
      )
      {
        SearchQuery query = (SearchQuery) input;
        return new SearchBinaryFn(query.getSort(), query.getGranularity(), query.getLimit());
      }
    };
  }

  @Override
  public ServiceMetricEvent.Builder makeMetricBuilder(SearchQuery query)
  {
    return DruidMetrics.makePartialQueryTimeMetric(query);
  }

  @Override
  public Function<Result<SearchResultValue>, Result<SearchResultValue>> makePreComputeManipulatorFn(
      SearchQuery query, MetricManipulationFn fn
  )
  {
    return Functions.identity();
  }

  @Override
  public TypeReference<Result<SearchResultValue>> getResultTypeReference()
  {
    return TYPE_REFERENCE;
  }

  @Override
  public CacheStrategy<Result<SearchResultValue>, Object, SearchQuery> getCacheStrategy(SearchQuery query)
  {
    return new CacheStrategy<Result<SearchResultValue>, Object, SearchQuery>()
    {
      @Override
      public byte[] computeCacheKey(SearchQuery query)
      {
        final String dimFilter = query.getDimensionsFilter();
        final byte[] filterBytes = dimFilter == null ? new byte[]{} : dimFilter.getBytes();
        final byte[] querySpecBytes = query.getQuery().getCacheKey();
        final byte[] granularityBytes = query.getGranularity().cacheKey();

        final Collection<DimensionSpec> dimensions = query.getDimensions() == null
                                                     ? ImmutableList.<DimensionSpec>of()
                                                     : query.getDimensions();

        final byte[][] dimensionsBytes = new byte[dimensions.size()][];
        int dimensionsBytesSize = 0;
        int index = 0;
        for (DimensionSpec dimension : dimensions) {
          dimensionsBytes[index] = dimension.getCacheKey();
          dimensionsBytesSize += dimensionsBytes[index].length;
          ++index;
        }

        final byte[] sortSpecBytes = query.getSort().getCacheKey();

        final ByteBuffer queryCacheKey = ByteBuffer
            .allocate(
                1 + 4 + granularityBytes.length + filterBytes.length +
                querySpecBytes.length + dimensionsBytesSize + sortSpecBytes.length
            )
            .put(SEARCH_QUERY)
            .put(Ints.toByteArray(query.getLimit()))
            .put(granularityBytes)
            .put(filterBytes)
            .put(querySpecBytes)
            .put(sortSpecBytes)
            ;

        for (byte[] bytes : dimensionsBytes) {
          queryCacheKey.put(bytes);
        }

        return queryCacheKey.array();
      }

      @Override
      public TypeReference<Object> getCacheObjectClazz()
      {
        return OBJECT_TYPE_REFERENCE;
      }

      @Override
      public Function<Result<SearchResultValue>, Object> prepareForCache()
      {
        return new Function<Result<SearchResultValue>, Object>()
        {
          @Override
          public Object apply(Result<SearchResultValue> input)
          {
            return Lists.newArrayList(input.getTimestamp().getMillis(), input.getValue());
          }
        };
      }

      @Override
      public Function<Object, Result<SearchResultValue>> pullFromCache()
      {
        return new Function<Object, Result<SearchResultValue>>()
        {
          @Override
          @SuppressWarnings("unchecked")
          public Result<SearchResultValue> apply(Object input)
          {
            List<Object> result = (List<Object>) input;

            return new Result<>(
                new DateTime(((Number) result.get(0)).longValue()),
                new SearchResultValue(
                    Lists.transform(
                        (List) result.get(1),
                        new Function<Object, SearchHit>()
                        {
                          @Override
                          public SearchHit apply(@Nullable Object input)
                          {
                            if (input instanceof Map) {
                              return new SearchHit(
                                  (String) ((Map) input).get("dimension"),
                                  (String) ((Map) input).get("value")
                              );
                            } else if (input instanceof SearchHit) {
                              return (SearchHit) input;
                            } else {
                              throw new IAE("Unknown format [%s]", input.getClass());
                            }
                          }
                        }
                    )
                )
            );
          }
        };
      }
    };
  }

  @Override
  public QueryRunner<Result<SearchResultValue>> preMergeQueryDecoration(final QueryRunner<Result<SearchResultValue>> runner)
  {
    return new SearchThresholdAdjustingQueryRunner(
        intervalChunkingQueryRunnerDecorator.decorate(
            new QueryRunner<Result<SearchResultValue>>()
            {
              @Override
              public Sequence<Result<SearchResultValue>> run(
                  Query<Result<SearchResultValue>> query, Map<String, Object> responseContext
              )
              {
                SearchQuery searchQuery = (SearchQuery) query;
                if (searchQuery.getDimensionsFilter() != null) {
                  searchQuery = searchQuery.withDimFilter(searchQuery.getDimensionsFilter());
                }
                return runner.run(searchQuery, responseContext);
              }
            } , this),
        config
    );
  }

  private static class SearchThresholdAdjustingQueryRunner implements QueryRunner<Result<SearchResultValue>>
  {
    private final QueryRunner<Result<SearchResultValue>> runner;
    private final SearchQueryConfig config;

    public SearchThresholdAdjustingQueryRunner(
        QueryRunner<Result<SearchResultValue>> runner,
        SearchQueryConfig config
    )
    {
      this.runner = runner;
      this.config = config;
    }

    @Override
    public Sequence<Result<SearchResultValue>> run(
        Query<Result<SearchResultValue>> input,
        Map<String, Object> responseContext
    )
    {
      if (!(input instanceof SearchQuery)) {
        throw new ISE("Can only handle [%s], got [%s]", SearchQuery.class, input.getClass());
      }

      final SearchQuery query = (SearchQuery) input;
      if (query.getLimit() < config.getMaxSearchLimit()) {
        return runner.run(query, responseContext);
      }

      final boolean isBySegment = BaseQuery.getContextBySegment(query, false);

      return Sequences.map(
          runner.run(query.withLimit(config.getMaxSearchLimit()), responseContext),
          new Function<Result<SearchResultValue>, Result<SearchResultValue>>()
          {
            @Override
            public Result<SearchResultValue> apply(Result<SearchResultValue> input)
            {
              if (isBySegment) {
                BySegmentSearchResultValue value = (BySegmentSearchResultValue) input.getValue();

                return new Result<SearchResultValue>(
                    input.getTimestamp(),
                    new BySegmentSearchResultValue(
                        Lists.transform(
                            value.getResults(),
                            new Function<Result<SearchResultValue>, Result<SearchResultValue>>()
                            {
                              @Override
                              public Result<SearchResultValue> apply(@Nullable Result<SearchResultValue> input)
                              {
                                return new Result<SearchResultValue>(
                                    input.getTimestamp(),
                                    new SearchResultValue(
                                        Lists.newArrayList(
                                            Iterables.limit(
                                                input.getValue(),
                                                query.getLimit()
                                            )
                                        )
                                    )
                                );
                              }
                            }
                        ),
                        value.getSegmentId(),
                        value.getInterval()
                    )
                );
              }

              return new Result<SearchResultValue>(
                  input.getTimestamp(),
                  new SearchResultValue(
                      Lists.<SearchHit>newArrayList(
                          Iterables.limit(input.getValue(), query.getLimit())
                      )
                  )
              );
            }
          }
      );
    }
  }
}
