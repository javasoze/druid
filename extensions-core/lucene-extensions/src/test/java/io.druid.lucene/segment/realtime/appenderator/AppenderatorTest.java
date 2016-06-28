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

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.common.guava.Sequences;
import io.druid.data.input.Committer;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.granularity.QueryGranularities;
import io.druid.granularity.QueryGranularity;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.lucene.LuceneDruidModule;
import io.druid.lucene.aggregation.LongMaxAggregatorFactory;
import io.druid.lucene.aggregation.LuceneAggregatorFactory;
import io.druid.lucene.query.groupby.GroupByQuery;
import io.druid.lucene.query.search.search.SearchQuery;
import io.druid.lucene.query.search.search.StrlenSearchSortSpec;
import io.druid.lucene.query.select.PagingSpec;
import io.druid.lucene.query.select.SelectQuery;
import io.druid.lucene.segment.realtime.LuceneAppenderatorFactory;
import io.druid.query.Query;
import io.druid.query.TableDataSource;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
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

/**
 */
public class AppenderatorTest {
    private static final List<SegmentIdentifier> IDENTIFIERS = ImmutableList.of(
            SI("2000/2001", "A", 0),
            SI("2000/2001", "A", 1),
            SI("2001/2002", "A", 0)
    );

    @Test
    public void test () {
        ObjectMapper objectMapper = new DefaultObjectMapper();
        Module m = new SimpleModule(LuceneDruidModule.class.getSimpleName())
                .registerSubtypes(
                        new NamedType(LuceneAppenderatorFactory.class, "lucene"),
                        new NamedType(GroupByQuery.class, "lucene_groupby")
                );
        objectMapper.registerModule(m);
    }

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

            appenderator.add(IDENTIFIERS.get(1), IR("2000", "qux", 4), committerSupplier);

            appenderator.persistAll(committerSupplier.get());

            Assert.assertEquals(1, appenderator.getRowCount(IDENTIFIERS.get(0)));
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
    public void testGroupByQuery() throws Exception
    {
        try (final AppenderatorTester tester = new AppenderatorTester(10000)) {
            final Appenderator appenderator = tester.getAppenderator();

            appenderator.startJob();
            appenderator.add(IDENTIFIERS.get(0), IR("2000", "foo", 21), Suppliers.ofInstance(Committers.nil()));
            appenderator.add(IDENTIFIERS.get(0), IR("2000", "bar", 2), Suppliers.ofInstance(Committers.nil()));
            appenderator.add(IDENTIFIERS.get(1), IR("2000", "bar", 4), Suppliers.ofInstance(Committers.nil()));
            appenderator.add(IDENTIFIERS.get(2), IR("2001", "foo", 8), Suppliers.ofInstance(Committers.nil()));
            appenderator.add(IDENTIFIERS.get(2), IR("2001T01", "foo", 16), Suppliers.ofInstance(Committers.nil()));
            appenderator.add(IDENTIFIERS.get(2), IR("2001T02", "foo", 32), Suppliers.ofInstance(Committers.nil()));

            Thread.sleep(5000);

            QuerySegmentSpec firstToThird = new MultipleIntervalSegmentSpec(
                    Arrays.asList(new Interval("1999-04-01T00:00:00.000Z/2011-04-03T00:00:00.000Z")));
            QueryGranularity dayGran = QueryGranularities.DAY;
            Query query = GroupByQuery
                    .builder()
                    .setDataSource(AppenderatorTester.DATASOURCE)
                    .setQuerySegmentSpec(firstToThird)
                    .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("dim", "dim")))
                    .setAggregatorSpecs(
                            Arrays.<LuceneAggregatorFactory>asList(
                                    new LongMaxAggregatorFactory("idx", "val")
                            )
                    )
                    .setQuery("dim:foo")
                    .setGranularity(dayGran)
                    .build();
            final List results1 = Lists.newArrayList();
            Sequences.toList(query.run(appenderator, ImmutableMap.<String, Object>of()), results1);
            System.out.println(results1);
        }
    }

    @Test
    public void testSearchQuery() throws Exception {
        try (final AppenderatorTester tester = new AppenderatorTester(10000)) {
            final Appenderator appenderator = tester.getAppenderator();

            appenderator.startJob();
            appenderator.add(IDENTIFIERS.get(0), IR("2000", "foo", 21), Suppliers.ofInstance(Committers.nil()));
            appenderator.add(IDENTIFIERS.get(0), IR("2000", "bar", 2), Suppliers.ofInstance(Committers.nil()));
            appenderator.add(IDENTIFIERS.get(1), IR("2000", "bar", 4), Suppliers.ofInstance(Committers.nil()));
            appenderator.add(IDENTIFIERS.get(2), IR("2001", "foo", 8), Suppliers.ofInstance(Committers.nil()));
            appenderator.add(IDENTIFIERS.get(2), IR("2001T01", "foo", 16), Suppliers.ofInstance(Committers.nil()));
            appenderator.add(IDENTIFIERS.get(2), IR("2001T02", "foo", 32), Suppliers.ofInstance(Committers.nil()));

            Thread.sleep(5000);

            QuerySegmentSpec firstToThird = new MultipleIntervalSegmentSpec(
                    Arrays.asList(new Interval("1999-04-01T00:00:00.000Z/2011-04-03T00:00:00.000Z")));
            QueryGranularity dayGran = QueryGranularities.DAY;
            Query query = new SearchQuery(
                new TableDataSource(AppenderatorTester.DATASOURCE),
                    "dim:foo",
                    dayGran,
                    10,
                firstToThird,
                    Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("dim", "dim")),
                new StrlenSearchSortSpec(),
                new HashMap<String, Object>()
            );

            final List results1 = Lists.newArrayList();
            Sequences.toList(query.run(appenderator, ImmutableMap.<String, Object>of()), results1);
            System.out.println(results1);
        }
    }

    @Test
    public void testSelectQuery() throws Exception {
        try (final AppenderatorTester tester = new AppenderatorTester(10000)) {
            final Appenderator appenderator = tester.getAppenderator();

            appenderator.startJob();
            appenderator.add(IDENTIFIERS.get(0), IR("2000", "foo", 21), Suppliers.ofInstance(Committers.nil()));
            appenderator.add(IDENTIFIERS.get(0), IR("2000", "bar", 2), Suppliers.ofInstance(Committers.nil()));
            appenderator.add(IDENTIFIERS.get(1), IR("2000", "bar", 4), Suppliers.ofInstance(Committers.nil()));
            appenderator.add(IDENTIFIERS.get(2), IR("2001", "foo", 8), Suppliers.ofInstance(Committers.nil()));
            appenderator.add(IDENTIFIERS.get(2), IR("2001T01", "foo", 16), Suppliers.ofInstance(Committers.nil()));
            appenderator.add(IDENTIFIERS.get(2), IR("2001T02", "foo", 32), Suppliers.ofInstance(Committers.nil()));

            Thread.sleep(5000);

            QuerySegmentSpec firstToThird = new MultipleIntervalSegmentSpec(
                    Arrays.asList(new Interval("1999-04-01T00:00:00.000Z/2011-04-03T00:00:00.000Z")));
            QueryGranularity dayGran = QueryGranularities.DAY;
            Query query = new SelectQuery(
                    new TableDataSource(AppenderatorTester.DATASOURCE),
                    firstToThird,
                    false,
                    "dim:foo",
                    dayGran,
                    Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("dim", "dim")),
                    PagingSpec.newSpec(100),
                    new HashMap<String, Object>()
            );
            final List results1 = Lists.newArrayList();
            Sequences.toList(query.run(appenderator, ImmutableMap.<String, Object>of()), results1);
            System.out.println(results1);
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
                ImmutableList.of("dim", "val"),
                ImmutableMap.<String, Object>of(
                        "dim",
                        dim,
                        "val",
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
