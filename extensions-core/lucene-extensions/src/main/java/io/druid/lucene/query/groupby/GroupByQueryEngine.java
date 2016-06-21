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

package io.druid.lucene.query.groupby;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.inject.Inject;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.guava.*;
import com.metamx.common.parsers.CloseableIterator;
import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidPool;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.guice.annotations.Global;
import io.druid.lucene.LuceneDirectory;
import io.druid.lucene.aggregation.LuceneAggregatorFactory;
import io.druid.lucene.segment.DimensionSelector;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.DocSetCollector;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 */
public class GroupByQueryEngine
{
  private static final String CTX_KEY_MAX_INTERMEDIATE_ROWS = "maxIntermediateRows";

  private final Supplier<GroupByQueryConfig> config;
  private final StupidPool<ByteBuffer> intermediateResultsBufferPool;

  @Inject
  public GroupByQueryEngine(
      Supplier<GroupByQueryConfig> config,
      @Global StupidPool<ByteBuffer> intermediateResultsBufferPool
  )
  {
    this.config = config;
    this.intermediateResultsBufferPool = intermediateResultsBufferPool;
  }

  public Sequence<Row> process(final GroupByQuery query, final LuceneDirectory directory)
  {
    if (directory == null) {
      throw new ISE(
          "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
      );
    }

    final List<Interval> intervals = query.getQuerySegmentSpec().getIntervals();
    if (intervals.size() != 1) {
      throw new IAE("Should only have one interval, got[%s]", intervals);
    }

    final ResourceHolder<ByteBuffer> bufferHolder = intermediateResultsBufferPool.take();

    try {
      IndexReader reader = directory.getIndexReader();
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
      final Map<String, DimensionSchema.ValueType> dimTypes = directory.getFieldTypes();
      return Sequences.concat(
              Sequences.withBaggage(
                      Sequences.map(
                              leafReaders,
                              new Function<LeafReader, Sequence<Row>>()
                              {
                                @Override
                                public Sequence<Row> apply(final LeafReader leafReader)
                                {
                                  return new BaseSequence<>(
                                          new BaseSequence.IteratorMaker<Row, RowIterator>()
                                          {
                                            @Override
                                            public RowIterator make()
                                            {
                                              try {
                                                return new RowIterator(query, leafReader, dimTypes, bufferHolder.get(), config.get());
                                              } catch (IOException e) {
                                                throw new IAE(e, "");
                                              }
                                            }

                                            @Override
                                            public void cleanup(RowIterator iterFromMake)
                                            {
                                              CloseQuietly.close(iterFromMake);
                                            }
                                          }
                                  );
                                }
                              }
                      ),
                      new Closeable()
                      {
                        @Override
                        public void close() throws IOException
                        {
                          CloseQuietly.close(bufferHolder);
                        }
                      }
              )
      );
    } catch (IOException e) {
      throw new IAE(e, "");
    }

  }

  private static class RowUpdater
  {

    private final ByteBuffer metricValues;
    private final BufferAggregator[] aggregators;
    private final PositionMaintainer positionMaintainer;

    private final Map<ByteBuffer, Integer> positions = Maps.newTreeMap();
    // GroupBy queries tend to do a lot of reads from this. We co-store a hash map to make those reads go faster.
    private final Map<ByteBuffer, Integer> positionsHash = Maps.newHashMap();

    public RowUpdater(
        ByteBuffer metricValues,
        BufferAggregator[] aggregators,
        PositionMaintainer positionMaintainer
    )
    {
      this.metricValues = metricValues;
      this.aggregators = aggregators;
      this.positionMaintainer = positionMaintainer;
    }

    public int getNumRows()
    {
      return positions.size();
    }

    public Map<ByteBuffer, Integer> getPositions()
    {
      return positions;
    }

    private List<ByteBuffer> updateValues(
        ByteBuffer key,
        List<DimensionSelector> dims
    )
    {
      if (dims.size() > 0) {
        List<ByteBuffer> retVal = null;
        List<ByteBuffer> unaggregatedBuffers = null;

        final DimensionSelector dimSelector = dims.get(0);
        final List row = dimSelector.getRow();
        for (Object value: row) {
          ByteBuffer newKey = key.duplicate();
          switch (dimSelector.getType()) {
            case INT:
              newKey.putInt((int)value);
              break;
            case FLOAT:
              newKey.putFloat((float)value);
              break;
            case LONG:
              newKey.putLong((long)value);
              break;
            default:
              throw new IAE("");
          }
          unaggregatedBuffers = updateValues(newKey, dims.subList(1, dims.size()));
        }
        if (unaggregatedBuffers != null) {
          if (retVal == null) {
            retVal = Lists.newArrayList();
          }
          retVal.addAll(unaggregatedBuffers);
        }
        return retVal;
      } else {
        key.clear();
        Integer position = positionsHash.get(key);
        int[] increments = positionMaintainer.getIncrements();
        int thePosition;

        if (position == null) {
          ByteBuffer keyCopy = ByteBuffer.allocate(key.limit());
          keyCopy.put(key.asReadOnlyBuffer());
          keyCopy.clear();

          position = positionMaintainer.getNext();
          if (position == null) {
            return Lists.newArrayList(keyCopy);
          }

          positions.put(keyCopy, position);
          positionsHash.put(keyCopy, position);
          thePosition = position;
          for (int i = 0; i < aggregators.length; ++i) {
            aggregators[i].init(metricValues, thePosition);
            thePosition += increments[i];
          }
        }

        thePosition = position;
        for (int i = 0; i < aggregators.length; ++i) {
          aggregators[i].aggregate(metricValues, thePosition);
          thePosition += increments[i];
        }
        return null;
      }
    }
  }

  private static class PositionMaintainer
  {
    private final int[] increments;
    private final int increment;
    private final int max;

    private long nextVal;

    public PositionMaintainer(
        int start,
        int[] increments,
        int max
    )
    {
      this.nextVal = (long) start;
      this.increments = increments;

      int theIncrement = 0;
      for (int i = 0; i < increments.length; i++) {
        theIncrement += increments[i];
      }
      increment = theIncrement;

      this.max = max - increment; // Make sure there is enough room for one more increment
    }

    public Integer getNext()
    {
      if (nextVal > max) {
        return null;
      } else {
        int retVal = (int) nextVal;
        nextVal += increment;
        return retVal;
      }
    }

    public int getIncrement()
    {
      return increment;
    }

    public int[] getIncrements()
    {
      return increments;
    }
  }

  private static class RowIterator implements CloseableIterator<Row>
  {
    private final GroupByQuery query;
    private final IndexSearcher searcher;
    private final ByteBuffer metricsBuffer;
    private final int maxIntermediateRows;

    private final List<DimensionSpec> dimensionSpecs;
    private final DimensionSelector<Long> timestamp;
    private final List<DimensionSelector> dimensions;
    private final ArrayList<String> dimNames;
    private final BufferAggregator[] aggregators;
    private final String[] metricNames;
    private final int[] sizesRequired;

    private List<ByteBuffer> unprocessedKeys;
    private Iterator<Row> delegate;
    private LuceneCursor cursor;

    public RowIterator(GroupByQuery query, final LeafReader leafReader, Map<String, DimensionSchema.ValueType> dimTypes, ByteBuffer metricsBuffer, GroupByQueryConfig config) throws IOException {
      this.query = query;
      this.searcher = new IndexSearcher(leafReader);
      this.metricsBuffer = metricsBuffer;

      this.maxIntermediateRows = Math.min(
          query.getContextValue(
              CTX_KEY_MAX_INTERMEDIATE_ROWS,
              config.getMaxIntermediateRows()
          ), config.getMaxIntermediateRows()
      );

      unprocessedKeys = null;
      delegate = Iterators.emptyIterator();
      dimensionSpecs = query.getDimensions();
      dimensions = Lists.newArrayListWithExpectedSize(dimensionSpecs.size());
      dimNames = Lists.newArrayListWithExpectedSize(dimensionSpecs.size());

      cursor = new LuceneCursor(leafReader, dimTypes);
      for (int i = 0; i < dimensionSpecs.size(); ++i) {
        final DimensionSpec dimSpec = dimensionSpecs.get(i);
        DimensionSelector selector = cursor.makeDimensionSelector(dimSpec.getDimension());
        if (selector != null) {
          dimensions.add(selector);
          dimNames.add(dimSpec.getOutputName());
        }
      }

      timestamp = cursor.makeTimestampSelector();
      List<LuceneAggregatorFactory> aggregatorSpecs = query.getAggregatorSpecs();
      aggregators = new BufferAggregator[aggregatorSpecs.size()];
      metricNames = new String[aggregatorSpecs.size()];
      sizesRequired = new int[aggregatorSpecs.size()];
      for (int i = 0; i < aggregatorSpecs.size(); ++i) {
        LuceneAggregatorFactory aggregatorSpec = aggregatorSpecs.get(i);
        aggregators[i] = aggregatorSpec.factorizeBuffered(cursor);
        metricNames[i] = aggregatorSpec.getName();
        sizesRequired[i] = aggregatorSpec.getMaxIntermediateSize();
      }

      String queryStr = query.getQuery();
      Analyzer analyzer = new StandardAnalyzer();
      QueryParser parser = new QueryParser("", analyzer);
      try {
        Query q = parser.parse(queryStr);
        DocSetCollector collector = new DocSetCollector(100, 10000);
        searcher.search(q, collector);
        DocSet docSet = collector.getDocSet();
        cursor.reset(docSet.iterator());
      } catch (ParseException | IOException e) {
        throw new IAE("", e);
      }
    }

    @Override
    public boolean hasNext()
    {
      return delegate.hasNext() || !cursor.isDone();
    }

    @Override
    public Row next()
    {
      if (delegate.hasNext()) {
        return delegate.next();
      }

//      if (unprocessedKeys == null && cursor.isDone()) {
//        throw new NoSuchElementException();
//      }
//
      final PositionMaintainer positionMaintainer = new PositionMaintainer(0, sizesRequired, metricsBuffer.remaining());
      final RowUpdater rowUpdater = new RowUpdater(metricsBuffer, aggregators, positionMaintainer);
//      if (unprocessedKeys != null) {
//        for (ByteBuffer key : unprocessedKeys) {
//          final List<ByteBuffer> unprocUnproc = rowUpdater.updateValues(key, ImmutableList.<DimensionSelector>of());
//          if (unprocUnproc != null) {
//            throw new ISE("Not enough memory to process the request.");
//          }
//        }
//        cursor.advance();
//      }

      while (!cursor.isDone() && rowUpdater.getNumRows() < maxIntermediateRows) {
        ByteBuffer key = ByteBuffer.allocate(dimensions.size() * Ints.BYTES + Longs.BYTES);
        long time = query.getGranularity().next(timestamp.getRow().get(0));
        key.putLong(time);
        unprocessedKeys = rowUpdater.updateValues(key, dimensions);
        if (unprocessedKeys != null) {
          break;
        }

        cursor.advance();
      }

      if (rowUpdater.getPositions().isEmpty() && unprocessedKeys != null) {
        throw new ISE(
            "Not enough memory to process even a single item.  Required [%,d] memory, but only have[%,d]",
            positionMaintainer.getIncrement(), metricsBuffer.remaining()
        );
      }

      delegate = FunctionalIterator
          .create(rowUpdater.getPositions().entrySet().iterator())
          .transform(
              new Function<Map.Entry<ByteBuffer, Integer>, Row>()
              {
                private final int[] increments = positionMaintainer.getIncrements();

                @Override
                public Row apply(@Nullable Map.Entry<ByteBuffer, Integer> input)
                {
                  Map<String, Object> theEvent = Maps.newLinkedHashMap();
                  ByteBuffer keyBuffer = input.getKey().duplicate();
                  long time = keyBuffer.getLong();
                  for (int i = 0; i < dimensions.size(); ++i) {
                    final DimensionSelector dimSelector = dimensions.get(i);
                    final String dimVal;
                    switch (dimSelector.getType()) {
                      case INT:
                        dimVal = dimSelector.lookupName(keyBuffer.getInt());
                        break;
                      case LONG:
                        dimVal = dimSelector.lookupName(keyBuffer.getLong());
                        break;
                      case FLOAT:
                        dimVal = dimSelector.lookupName(keyBuffer.getFloat());
                        break;
                      default:
                        throw new IAE("");
                    }

                    if (null != dimVal) {
                      theEvent.put(dimNames.get(i), dimVal);
                    }
                  }

                  int position = input.getValue();
                  for (int i = 0; i < aggregators.length; ++i) {
                    theEvent.put(metricNames[i], aggregators[i].get(metricsBuffer, position));
                    position += increments[i];
                  }

                  for (PostAggregator postAggregator : query.getPostAggregatorSpecs()) {
                    theEvent.put(postAggregator.getName(), postAggregator.compute(theEvent));
                  }

                  return new MapBasedRow(time, theEvent);
                }
              }
          );

      return delegate.next();
    }

    @Override
    public void remove()
    {
      throw new UnsupportedOperationException();
    }

    public void close()
    {
      // cleanup
      for (BufferAggregator agg : aggregators) {
        agg.close();
      }
    }
  }
}
