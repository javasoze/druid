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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.ISE;
import com.metamx.common.Pair;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.emitter.EmittingLogger;

import io.druid.common.guava.ThreadRenamingCallable;
import io.druid.concurrent.Execs;
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
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.incremental.IndexSizeExceededException;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.realtime.FireHydrant;
import io.druid.segment.realtime.appenderator.*;
import io.druid.segment.realtime.plumber.Sink;
import io.druid.server.coordination.DataSegmentAnnouncer;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.PartitionChunk;
import io.druid.timeline.partition.PartitionHolder;

import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class LuceneAppenderator implements Appenderator, Runnable
{
  private static final EmittingLogger log = new EmittingLogger(LuceneAppenderator.class);
  
  private static final long DEFAULT_INDEX_REFRESH_INTERVAL_SECONDS = 5;

  private final DataSchema schema;
  private final DataSegmentPusher dataSegmentPusher;
  private final ObjectMapper objectMapper;
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final DataSegmentAnnouncer segmentAnnouncer;
  private final RealtimeTuningConfig realtimeTuningConfig;
  private final ExecutorService queryExecutorService;
  private final Thread indexRefresher;
  private volatile boolean isClosed = false;
  private final Map<SegmentIdentifier, RealtimeDirectory> directories = Maps.newConcurrentMap();
  private final Set<SegmentIdentifier> droppingDirectories = Sets.newConcurrentHashSet();
  private final VersionedIntervalTimeline<String, RealtimeDirectory> timeline = new VersionedIntervalTimeline<>(
      Ordering.natural()
  );

  private volatile long nextFlush;
  private volatile ListeningExecutorService persistExecutor = null;
  private volatile ListeningExecutorService mergeExecutor = null;

  public LuceneAppenderator(
      DataSchema schema,
      RealtimeTuningConfig realtimeTuningConfig,
      DataSegmentPusher dataSegmentPusher,
      ObjectMapper objectMapper,
      QueryRunnerFactoryConglomerate conglomerate,
      DataSegmentAnnouncer segmentAnnouncer,
      ExecutorService queryExecutorService
  )
  {
    this.schema = schema;
    this.realtimeTuningConfig = realtimeTuningConfig;
    this.queryExecutorService = queryExecutorService;
    this.dataSegmentPusher = dataSegmentPusher;
    this.objectMapper = objectMapper;
    this.conglomerate = conglomerate;
    this.segmentAnnouncer = segmentAnnouncer;
    this.indexRefresher = new Thread(this, "lucene index refresher");
    this.indexRefresher.setDaemon(true);
  }

  @Override
  public String getDataSource()
  {
    return schema.getDataSource();
  }


  @Override
  public Object startJob() {
    realtimeTuningConfig.getBasePersistDirectory().mkdirs();
    indexRefresher.start();
    initializeExecutors();
    resetNextFlush();
    return null;
  }

  @Override
  public int add(SegmentIdentifier identifier, InputRow row,
                 Supplier<Committer> committerSupplier) throws IndexSizeExceededException,
          SegmentNotWritableException {

    try {
      RealtimeDirectory directory = getOrCreateDir(identifier);
      directory.add(row);

      if (!directory.canAppendRow() || System.currentTimeMillis() > nextFlush) {
        log.info("Intermediate persist.");
        persistAll(committerSupplier.get());
      }
      return directory.numRows();
    } catch (IOException ioe) {
      ioe.printStackTrace();
      throw new SegmentNotWritableException(ioe.getMessage(), ioe);
    }
  }

  private RealtimeDirectory getOrCreateDir(final SegmentIdentifier identifier) throws IOException {
    RealtimeDirectory retVal = directories.get(identifier);

    if (retVal == null) {
      retVal = new RealtimeDirectory(identifier, schema, realtimeTuningConfig);

      try {
        segmentAnnouncer.announceSegment(retVal.getSegment());
      }
      catch (IOException e) {
        log.makeAlert(e, "Failed to announce new segment[%s]", schema.getDataSource())
                .addData("interval", identifier.getInterval())
                .emit();
      }

      directories.put(identifier, retVal);
      timeline.add(
              identifier.getInterval(),
              identifier.getVersion(),
              identifier.getShardSpec().createChunk(retVal)
      );
    }

    return retVal;
  }

  @Override
  public List<SegmentIdentifier> getSegments() {
    return ImmutableList.copyOf(directories.keySet());
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
  public int getRowCount(SegmentIdentifier identifier) {
    RealtimeDirectory directory = directories.get(identifier);
    return directory == null ? 0 : directory.numRows();
  }

  @Override
  public void clear() throws InterruptedException {
    // Drop commit metadata, then abandon all segments.

    try {
      final ListenableFuture<?> uncommitFuture = persistExecutor.submit(
              new Callable<Object>()
              {
                @Override
                public Object call() throws Exception
                {
                  objectMapper.writeValue(computeCommitFile(), Committed.nil());
                  return null;
                }
              }
      );

      // Await uncommit.
      uncommitFuture.get();

      // Drop everything.
      final List<ListenableFuture<?>> futures = Lists.newArrayList();
      for (Map.Entry<SegmentIdentifier, RealtimeDirectory> entry : directories.entrySet()) {
        futures.add(abandonSegment(entry.getKey(), entry.getValue(), true));
      }

      // Await dropping.
      Futures.allAsList(futures).get();
    }
    catch (ExecutionException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public ListenableFuture<?> drop(SegmentIdentifier identifier)
  {
    final RealtimeDirectory directory = directories.get(identifier);
    if (directory != null) {
      return abandonSegment(identifier, directory, true);
    } else {
      return Futures.immediateFuture(null);
    }
  }

  @Override
  public ListenableFuture<Object> persistAll(final Committer committer)
  {
    final Map<SegmentIdentifier, Integer> commitHydrants = Maps.newHashMap();
    final List<Pair<FireHydrant, SegmentIdentifier>> indexesToPersist = Lists.newArrayList();
    final String threadName = String.format("%s-incremental-persist", schema.getDataSource());
    final Object commitMetadata = committer.getMetadata();
    final ListenableFuture<Object> future = persistExecutor.submit(
            new ThreadRenamingCallable<Object>(threadName)
            {
              @Override
              public Object doCall()
              {
                try {
                  for (RealtimeDirectory directory : directories.values()) {
                    directory.persist();
                  }

                  committer.run();
                  objectMapper.writeValue(computeCommitFile(), Committed.create(commitHydrants, commitMetadata));

                  return commitMetadata;
                }
                catch (Exception e) {
                  throw Throwables.propagate(e);
                }
              }
            }
    );

    resetNextFlush();
    return future;
  }

  @Override
  public ListenableFuture<SegmentsAndMetadata> push(
      final List<SegmentIdentifier> identifiers,
      final Committer committer
  )
  {
    final Map<SegmentIdentifier, RealtimeDirectory> dirs = Maps.newHashMap();
    for (final SegmentIdentifier identifier : identifiers) {
      final RealtimeDirectory directory = directories.get(identifier);
      if (directory == null) {
        throw new NullPointerException("No sink for identifier: " + identifier);
      }
      dirs.put(identifier, directory);
    }

    return Futures.transform(
            persistAll(committer),
            new Function<Object, SegmentsAndMetadata>()
            {
              @Override
              public SegmentsAndMetadata apply(Object commitMetadata)
              {
                final List<DataSegment> dataSegments = Lists.newArrayList();

                for (Map.Entry<SegmentIdentifier, RealtimeDirectory> entry : dirs.entrySet()) {
                  if (droppingDirectories.contains(entry.getKey())) {
                    log.info("Skipping push of currently-dropping sink[%s]", entry.getKey());
                    continue;
                  }

                  final DataSegment dataSegment = mergeAndPush(entry.getValue());
                  if (dataSegment != null) {
                    dataSegments.add(dataSegment);
                  } else {
                    log.warn("mergeAndPush[%s] returned null, skipping.", entry.getKey());
                  }
                }

                return new SegmentsAndMetadata(dataSegments, commitMetadata);
              }
            },
            mergeExecutor
    );
  }

  /**
   * Insert a barrier into the merge-and-push queue. When this future resolves, all pending pushes will have finished.
   * This is useful if we're going to do something that would otherwise potentially break currently in-progress
   * pushes.
   */
  private ListenableFuture<?> mergeBarrier()
  {
    return mergeExecutor.submit(
            new Runnable()
            {
              @Override
              public void run()
              {
                // Do nothing
              }
            }
    );
  }

  private DataSegment mergeAndPush(final RealtimeDirectory directory) {
    try {
      File mergedFile = directory.merge();
      DataSegment segment = dataSegmentPusher.push(
              mergedFile,
              directory.getSegment()
      );
      return segment;
    } catch (Exception e) {
      log.warn(e, "Failed to push merged index for segment[%s].", directory.getIdentifier());
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void close() {
    log.info("Shutting down...");

    final List<ListenableFuture<?>> futures = Lists.newArrayList();
    for (Map.Entry<SegmentIdentifier, RealtimeDirectory> entry : directories.entrySet()) {
      futures.add(abandonSegment(entry.getKey(), entry.getValue(), false));
    }

    try {
      Futures.allAsList(futures).get();
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.warn(e, "Interrupted during close()");
    }
    catch (ExecutionException e) {
      log.warn(e, "Unable to abandon existing segments during close()");
    }

    indexRefresher.interrupt();
    try {
      indexRefresher.join();
    } catch (InterruptedException e) {
      log.error(e.getMessage(), e);
    }

    try {
      shutdownExecutors();
      Preconditions.checkState(persistExecutor.awaitTermination(365, TimeUnit.DAYS), "persistExecutor not terminated");
      Preconditions.checkState(mergeExecutor.awaitTermination(365, TimeUnit.DAYS), "mergeExecutor not terminated");
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ISE("Failed to shutdown executors during close()");
    }
  }

  private void initializeExecutors()
  {
    final int maxPendingPersists = realtimeTuningConfig.getMaxPendingPersists();

    if (persistExecutor == null) {
      // use a blocking single threaded executor to throttle the firehose when write to disk is slow
      persistExecutor = MoreExecutors.listeningDecorator(
              Execs.newBlockingSingleThreaded(
                      "appenderator_persist_%d", maxPendingPersists
              )
      );
    }
    if (mergeExecutor == null) {
      // use a blocking single threaded executor to throttle the firehose when write to disk is slow
      mergeExecutor = MoreExecutors.listeningDecorator(
              Execs.newBlockingSingleThreaded(
                      "appenderator_merge_%d", 1
              )
      );
    }
  }

  private void shutdownExecutors()
  {
    persistExecutor.shutdownNow();
    mergeExecutor.shutdownNow();
  }

  private void resetNextFlush()
  {
    nextFlush = new DateTime().plus(realtimeTuningConfig.getIntermediatePersistPeriod()).getMillis();
  }


  private ListenableFuture<?> abandonSegment(
          final SegmentIdentifier identifier,
          final RealtimeDirectory directory,
          final boolean removeOnDiskData
  )
  {
    // Mark this identifier as dropping, so no future merge tasks will pick it up.
    droppingDirectories.add(identifier);

    // Wait for any outstanding merges to finish, then abandon the segment inside the persist thread.
    return Futures.transform(
            mergeBarrier(),
            new Function<Object, Object>()
            {
              @Nullable
              @Override
              public Object apply(@Nullable Object input)
              {
                if (directories.get(identifier) != directory) {
                  // Only abandon sink if it is the same one originally requested to be abandoned.
                  log.warn("Sink for segment[%s] no longer valid, not abandoning.");
                  return null;
                }

                if (removeOnDiskData) {
                  // Remove this segment from the committed list. This must be done from the persist thread.
                  log.info("Removing commit metadata for segment[%s].", identifier);
                  try {
                    final File commitFile = computeCommitFile();
                    if (commitFile.exists()) {
                      final Committed oldCommitted = objectMapper.readValue(commitFile, Committed.class);
                      objectMapper.writeValue(commitFile, oldCommitted.without(identifier.getIdentifierAsString()));
                    }
                  }
                  catch (Exception e) {
                    log.makeAlert(e, "Failed to update committed segments[%s]", schema.getDataSource())
                            .addData("identifier", identifier.getIdentifierAsString())
                            .emit();
                    throw Throwables.propagate(e);
                  }
                }

                // Unannounce the segment.
                try {
                  segmentAnnouncer.unannounceSegment(directory.getSegment());
                }
                catch (Exception e) {
                  log.makeAlert(e, "Failed to unannounce segment[%s]", schema.getDataSource())
                          .addData("identifier", identifier.getIdentifierAsString())
                          .emit();
                }

                log.info("Removing sink for segment[%s].", identifier);
                directories.remove(identifier);
                droppingDirectories.remove(identifier);
                timeline.remove(
                        directory.getIdentifier().getInterval(),
                        directory.getIdentifier().getVersion(),
                        identifier.getShardSpec().createChunk(directory)
                );

                try {
                  directory.close();
                  if (removeOnDiskData) {
                    removeDirectory(directory.getPersistDir());
                  }
                }
                catch (Exception e) {
                  log.makeAlert(e, "Failed to unannounce segment[%s]", schema.getDataSource())
                          .addData("identifier", identifier.getIdentifierAsString())
                          .emit();
                }


                return null;
              }
            },
            persistExecutor
    );
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

  private File computeCommitFile()
  {
    return new File(realtimeTuningConfig.getBasePersistDirectory(), "commit.json");
  }

  private void removeDirectory(final File target)
  {
    if (target.exists()) {
      try {
        log.info("Deleting Index File[%s]", target);
        FileUtils.deleteDirectory(target);
      }
      catch (Exception e) {
        log.makeAlert(e, "Failed to remove directory[%s]", schema.getDataSource())
                .addData("file", target)
                .emit();
      }
    }
  }
}
