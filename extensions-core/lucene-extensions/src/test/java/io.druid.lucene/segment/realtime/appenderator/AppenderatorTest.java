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

package io.druid.lucene.segment.realtime.appenderator;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.common.guava.Sequences;
import io.druid.data.input.Committer;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.SpatialDimensionSchema;
import io.druid.granularity.QueryGranularities;
import io.druid.lucene.query.LuceneDruidQuery;
import io.druid.lucene.query.LuceneQueryResultValue;
import io.druid.query.Druids;
import io.druid.query.Result;
import io.druid.query.SegmentDescriptor;
import io.druid.query.TableDataSource;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.spec.LegacySegmentSpec;
import io.druid.query.spec.MultipleSpecificSegmentSpec;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesResultValue;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.realtime.appenderator.Appenderator;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;
import io.druid.segment.realtime.appenderator.SegmentsAndMetadata;
import io.druid.segment.realtime.plumber.Committers;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.LinearShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class AppenderatorTest {
    private static final List<SegmentIdentifier> IDENTIFIERS = ImmutableList.of(
            SI("2000/2001", "A", 0),
            SI("2000/2001", "A", 1),
            SI("2001/2002", "A", 0)
    );

    @Test
    public void testSimpleIngestion() throws Exception
    {
        try (final AppenderatorTester tester = new AppenderatorTester(2)) {
            final Appenderator appenderator = tester.getAppenderator();

            final ConcurrentMap<String, String> commitMetadata = new ConcurrentHashMap<>();
            final Supplier<Committer> committerSupplier = committerSupplierFromConcurrentMap(commitMetadata);

            // startJob
            Assert.assertEquals(null, appenderator.startJob());

            // getDataSource
            Assert.assertEquals(AppenderatorTester.DATASOURCE, appenderator.getDataSource());

            // add
            appenderator.add(IDENTIFIERS.get(0), IR("2000", "foo", 1), committerSupplier);

            // make sure refresh finished
            Thread.sleep(5000L);

            Assert.assertEquals(1, appenderator.getRowCount(IDENTIFIERS.get(0)));

            appenderator.add(IDENTIFIERS.get(1), IR("2000", "qux", 4), committerSupplier);

            appenderator.persistAll(committerSupplier.get());

            // make sure persist finished
            Thread.sleep(1000L);

            Assert.assertEquals(1, appenderator.getRowCount(IDENTIFIERS.get(1)));


            final SegmentsAndMetadata segmentsAndMetadata = appenderator.push(
                    appenderator.getSegments(),
                    committerSupplier.get()
            ).get();

            // clear
            appenderator.clear();
            Assert.assertTrue(appenderator.getSegments().isEmpty());
        }
    }


    @Test
    public void testQuery() throws Exception
    {
        try (final AppenderatorTester tester = new AppenderatorTester(2)) {
            final Appenderator appenderator = tester.getAppenderator();

            appenderator.startJob();
            appenderator.add(IDENTIFIERS.get(0), IR("2000", "foo", 1), Suppliers.ofInstance(Committers.nil()));
            appenderator.add(IDENTIFIERS.get(0), IR("2000", "bar", 2), Suppliers.ofInstance(Committers.nil()));
            appenderator.add(IDENTIFIERS.get(1), IR("2000", "bar", 4), Suppliers.ofInstance(Committers.nil()));
            appenderator.add(IDENTIFIERS.get(2), IR("2001", "foo", 8), Suppliers.ofInstance(Committers.nil()));
            appenderator.add(IDENTIFIERS.get(2), IR("2001T01", "foo", 16), Suppliers.ofInstance(Committers.nil()));
            appenderator.add(IDENTIFIERS.get(2), IR("2001T02", "foo", 32), Suppliers.ofInstance(Committers.nil()));

            // Query1: foo/bar
            final LuceneDruidQuery query1 = new LuceneDruidQuery(
                    new TableDataSource(AppenderatorTester.DATASOURCE),
                    new LegacySegmentSpec(ImmutableList.of(new Interval("2000/2002"))),
                    null,
                    "dim",
                    "bar",
                    1
            );

            final List<Result<LuceneQueryResultValue>> results1 = Lists.newArrayList();
            Sequences.toList(query1.run(appenderator, ImmutableMap.<String, Object>of()), results1);
            Assert.assertEquals(
                    "query1",
                    ImmutableList.of(
                            new Result<>(
                                    new DateTime("2000"),
                                    new LuceneQueryResultValue(2, 6)
                            )

                    ),
                    results1
            );

        }
    }

    private static SegmentIdentifier SI(String interval, String version, int partitionNum)
    {
        return new SegmentIdentifier(
                AppenderatorTester.DATASOURCE,
                new Interval(interval),
                version,
                new LinearShardSpec(partitionNum)
        );
    }

    static InputRow IR(String ts, String dim, long met)
    {
        return new MapBasedInputRow(
                new DateTime(ts).getMillis(),
                ImmutableList.of("dim", "met"),
                ImmutableMap.<String, Object>of(
                        "dim",
                        dim,
                        "met",
                        met
                )
        );
    }

    private static Supplier<Committer> committerSupplierFromConcurrentMap(final ConcurrentMap<String, String> map)
    {
        return new Supplier<Committer>()
        {
            @Override
            public Committer get()
            {
                final Map<String, String> mapCopy = ImmutableMap.copyOf(map);

                return new Committer()
                {
                    @Override
                    public Object getMetadata()
                    {
                        return mapCopy;
                    }

                    @Override
                    public void run()
                    {
                        // Do nothing
                    }
                };
            }
        };
    }

    private static <T> List<T> sorted(final List<T> xs)
    {
        final List<T> xsSorted = Lists.newArrayList(xs);
        Collections.sort(
                xsSorted, new Comparator<T>()
                {
                    @Override
                    public int compare(T a, T b)
                    {
                        if (a instanceof SegmentIdentifier && b instanceof SegmentIdentifier) {
                            return ((SegmentIdentifier) a).getIdentifierAsString()
                                    .compareTo(((SegmentIdentifier) b).getIdentifierAsString());
                        } else if (a instanceof DataSegment && b instanceof DataSegment) {
                            return ((DataSegment) a).getIdentifier()
                                    .compareTo(((DataSegment) b).getIdentifier());
                        } else {
                            throw new IllegalStateException("WTF??");
                        }
                    }
                }
        );
        return xsSorted;
    }


}
