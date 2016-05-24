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
package io.druid.lucene.segment.realtime;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.emitter.EmittingLogger;

import io.druid.data.input.Committer;
import io.druid.data.input.InputRow;
import io.druid.query.BySegmentQueryRunner;
import io.druid.query.NoopQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.query.QueryToolChest;
import io.druid.query.ReportTimelineMissingSegmentQueryRunner;
import io.druid.query.SegmentDescriptor;
import io.druid.query.spec.SpecificSegmentQueryRunner;
import io.druid.query.spec.SpecificSegmentSpec;
import io.druid.segment.incremental.IndexSizeExceededException;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.realtime.appenderator.Appenderator;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;
import io.druid.segment.realtime.appenderator.SegmentNotWritableException;
import io.druid.segment.realtime.appenderator.SegmentsAndMetadata;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.PartitionChunk;
import io.druid.timeline.partition.PartitionHolder;

import org.joda.time.Interval;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class LuceneAppenderator implements Appenderator, Runnable
{
  private static final EmittingLogger log = new EmittingLogger(LuceneAppenderator.class);
  
  private static final long DEFAULT_INDEX_REFRESH_INTERVAL_SECONDS = 5;

  private final DataSchema schema;
  private final LuceneDocumentBuilder docBuilder;
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final RealtimeTuningConfig realtimeTuningConfig;
  private final ExecutorService queryExecutorService;
  private final Thread indexRefresher;
  private volatile boolean isClosed = false;
  private final Map<SegmentIdentifier, RealtimeDirectory> directories = Maps.newHashMap();
  private final VersionedIntervalTimeline<String, RealtimeDirectory> timeline = new VersionedIntervalTimeline<>(
      Ordering.natural()
  );
  
  public LuceneAppenderator(
      DataSchema schema,
      RealtimeTuningConfig realtimeTuningConfig,
      QueryRunnerFactoryConglomerate conglomerate,
      ExecutorService queryExecutorService
  )
  {
    this.schema = schema;
    this.docBuilder = new LuceneDocumentBuilder(schema.getParser().getParseSpec().getDimensionsSpec());
    this.realtimeTuningConfig = realtimeTuningConfig;
    this.queryExecutorService = queryExecutorService;
    this.conglomerate = conglomerate;
    this.indexRefresher = new Thread(this, "lucene index refresher");
    this.indexRefresher.setDaemon(true);
  }

  @Override
  public String getDataSource()
  {
    return schema.getDataSource();
  }
  
  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query,
      Iterable<Interval> intervals) {
    final List<SegmentDescriptor> specs = Lists.newArrayList();

    Iterables.addAll(
        specs,
        FunctionalIterable
            .create(intervals)
            .transformCat(
                new Function<Interval, Iterable<TimelineObjectHolder<String, RealtimeDirectory>>>()
                {
                  @Override
                  public Iterable<TimelineObjectHolder<String, RealtimeDirectory>> apply(final Interval interval)
                  {
                    return timeline.lookup(interval);
                  }
                }
            )
            .transformCat(
                new Function<TimelineObjectHolder<String, RealtimeDirectory>, Iterable<SegmentDescriptor>>()
                {
                  @Override
                  public Iterable<SegmentDescriptor> apply(final TimelineObjectHolder<String, RealtimeDirectory> holder)
                  {
                    return FunctionalIterable
                        .create(holder.getObject())
                        .transform(
                            new Function<PartitionChunk<RealtimeDirectory>, SegmentDescriptor>()
                            {
                              @Override
                              public SegmentDescriptor apply(final PartitionChunk<RealtimeDirectory> chunk)
                              {
                                return new SegmentDescriptor(
                                    holder.getInterval(),
                                    holder.getVersion(),
                                    chunk.getChunkNumber()
                                );
                              }
                            }
                        );
                  }
                }
            )
    );

    return getQueryRunnerForSegments(query, specs);
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query,
      Iterable<SegmentDescriptor> specs) {
 // We only handle one dataSource. Make sure it's in the list of names, then ignore from here on out.
    if (!query.getDataSource().getNames().contains(getDataSource())) {
      log.makeAlert("Received query for unknown dataSource")
         .addData("dataSource", query.getDataSource())
         .emit();
      return new NoopQueryRunner<>();
    }

    final QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);
    if (factory == null) {
      log.makeAlert("Unknown query type, [%s]", query.getClass())
         .addData("dataSource", query.getDataSource())
         .emit();
      return new NoopQueryRunner<>();
    }

    final QueryToolChest<T, Query<T>> toolchest = factory.getToolchest();

    return toolchest.mergeResults(
        factory.mergeRunners(
            queryExecutorService,
            FunctionalIterable
                .create(specs)
                .transform(
                    new Function<SegmentDescriptor, QueryRunner<T>>()
                    {
                      @Override
                      public QueryRunner<T> apply(final SegmentDescriptor descriptor)
                      {
                        final PartitionHolder<RealtimeDirectory> holder = timeline.findEntry(
                            descriptor.getInterval(),
                            descriptor.getVersion()
                        );
                        if (holder == null) {
                          return new ReportTimelineMissingSegmentQueryRunner<>(descriptor);
                        }

                        final PartitionChunk<RealtimeDirectory> chunk = holder.getChunk(descriptor.getPartitionNumber());
                        if (chunk == null) {
                          return new ReportTimelineMissingSegmentQueryRunner<>(descriptor);
                        }

                        final RealtimeDirectory directory = chunk.getObject();

                        return new SpecificSegmentQueryRunner<>(
                            new BySegmentQueryRunner<>(
                                directory.getIdentifier().getIdentifierAsString(),
                                descriptor.getInterval().getStart(),
                                factory.createRunner(new LuceneIncrementalSegment(directory))
                            ),
                            new SpecificSegmentSpec(descriptor)
                        );
                      }
                    }
                )
        )
    );
  }

  @Override
  public Object startJob() {
    indexRefresher.start();
    return null;
  }

  @Override
  public int add(SegmentIdentifier identifier, InputRow row,
      Supplier<Committer> committerSupplier) throws IndexSizeExceededException,
      SegmentNotWritableException {
    RealtimeDirectory directory = directories.get(identifier);

    try {
      if (directory == null) {
        directory = new RealtimeDirectory(identifier, realtimeTuningConfig.getBasePersistDirectory(),
                docBuilder, realtimeTuningConfig.getMaxRowsInMemory());
        directories.put(identifier, directory);
        timeline.add(
                identifier.getInterval(),
                identifier.getVersion(),
                identifier.getShardSpec().createChunk(directory)
        );
      }
      directory.add(row);
      return directory.numRows();
    } catch (IOException ioe) {
      ioe.printStackTrace();
      throw new SegmentNotWritableException(ioe.getMessage(), ioe);
    }
  }

  @Override
  public List<SegmentIdentifier> getSegments() {
    return ImmutableList.copyOf(directories.keySet());
  }

  @Override
  public int getRowCount(SegmentIdentifier identifier) {
    RealtimeDirectory directory = directories.get(identifier);
    return directory == null ? 0 : directory.numRows();
  }

  @Override
  public void clear() throws InterruptedException {
    for (Map.Entry<SegmentIdentifier, RealtimeDirectory> entry : directories.entrySet()) {
      timeline.remove(
          entry.getKey().getInterval(),
          entry.getKey().getVersion(),
          entry.getKey().getShardSpec().createChunk(entry.getValue())
      );
      try {
        entry.getValue().close();
      } catch (IOException e) {
        log.error(e.getMessage(), e);
      }
    }
    directories.clear();
  }

  @Override
  public ListenableFuture<?> drop(SegmentIdentifier identifier)
  {
    final RealtimeDirectory directory = directories.get(identifier);
    if (directory != null) {
      timeline.remove(
          identifier.getInterval(),
          identifier.getVersion(),
          identifier.getShardSpec().createChunk(directory)
      );
      directories.remove(identifier);
      try {
        directory.close();
      } catch (IOException e) {
        log.error(e.getMessage(), e);
      }
    }
    return Futures.immediateFuture(null);
  }

  @Override
  public ListenableFuture<Object> persistAll(Committer committer)
  {
    for (RealtimeDirectory directory : directories.values()) {
      try {
        directory.persist();
      } catch (IOException e) {
        log.error(e.getMessage(), e);
      }
    }
    committer.run();
    return Futures.immediateFuture(null);
  }

  @Override
  public ListenableFuture<SegmentsAndMetadata> push(
      final List<SegmentIdentifier> identifiers,
      final Committer committer
  )
  {
    // TODO - should persist to disk and push data to deep storage in a background thread
    return Futures.immediateFuture(
        new SegmentsAndMetadata(
            ImmutableList.<DataSegment>of(),
            committer.getMetadata()
        )
    );
  }

  @Override
  public void close() {
    indexRefresher.interrupt();
    try {
      indexRefresher.join();
    } catch (InterruptedException e) {
      log.error(e.getMessage(), e);
    }
  }

  @Override
  public void run() {
    while(!isClosed) {
      log.info("refresh index segments");
      for (RealtimeDirectory directory : directories.values()) {
        try {
          directory.refreshRealtimeReader();
        } catch (IOException e) {
          log.error(e.getMessage(), e);
        }
      }
      
      try {
        Thread.sleep(DEFAULT_INDEX_REFRESH_INTERVAL_SECONDS * 1000);   // refresh eery
      } catch (InterruptedException ie) {
        continue;
      }
    }
  }
}
