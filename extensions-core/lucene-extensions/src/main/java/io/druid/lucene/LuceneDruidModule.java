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

package io.druid.lucene;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.multibindings.MapBinder;
import io.druid.guice.DruidBinders;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.PolyBind;
import io.druid.initialization.DruidModule;
import io.druid.lucene.aggregation.CountAggregatorFactory;
import io.druid.lucene.aggregation.LongMaxAggregatorFactory;
import io.druid.lucene.aggregation.LongMinAggregatorFactory;
import io.druid.lucene.aggregation.LongSumAggregatorFactory;
import io.druid.lucene.query.groupby.*;
import io.druid.lucene.query.search.SearchQueryQueryToolChest;
import io.druid.lucene.query.search.SearchQueryRunnerFactory;
import io.druid.lucene.query.search.search.SearchQuery;
import io.druid.lucene.query.select.SelectQuery;
import io.druid.lucene.query.select.SelectQueryQueryToolChest;
import io.druid.lucene.query.select.SelectQueryRunnerFactory;
import io.druid.lucene.segment.loading.LuceneSegmentFactory;
import io.druid.lucene.segment.realtime.LuceneAppenderatorFactory;
import io.druid.lucene.task.LuceneIndexRealtimeTask;
import io.druid.query.Query;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryToolChest;
import io.druid.segment.loading.SegmentFactory;

import java.util.List;
import java.util.Map;

public class LuceneDruidModule implements DruidModule
{

  private static final Map<Class<? extends Query>, Class<? extends QueryRunnerFactory>> queryRunnerMappings =
          ImmutableMap.<Class<? extends Query>, Class<? extends QueryRunnerFactory>>builder()
                  .put(GroupByQuery.class, GroupByQueryRunnerFactory.class)
                  .put(SelectQuery.class, SelectQueryRunnerFactory.class)
                  .put(SearchQuery.class, SearchQueryRunnerFactory.class)
                  .build();

  private static final Map<Class<? extends Query>, Class<? extends QueryToolChest>> toolChestMappings =
          ImmutableMap.<Class<? extends Query>, Class<? extends QueryToolChest>>builder()
                  .put(GroupByQuery.class, GroupByQueryQueryToolChest.class)
                  .put(SelectQuery.class, SelectQueryQueryToolChest.class)
                  .put(SearchQuery.class, SearchQueryQueryToolChest.class)
                  .build();

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new SimpleModule(LuceneDruidModule.class.getSimpleName())
            .registerSubtypes(
                new NamedType(LuceneIndexRealtimeTask.class, "lucene_index_realtime"),
                new NamedType(LuceneAppenderatorFactory.class, "lucene"),
                new NamedType(GroupByQuery.class, "lucene_groupby"),
                new NamedType(SelectQuery.class, "lucene_select"),
                new NamedType(SearchQuery.class, "lucene_search"),
                new NamedType(CountAggregatorFactory.class, "lucene_count"),
                new NamedType(LongMinAggregatorFactory.class, "lucene_longMin"),
                new NamedType(LongMaxAggregatorFactory.class, "lucene_longMax"),
                new NamedType(LongSumAggregatorFactory.class, "lucene_longSum")
            )
    );
  }

  @Override
  public void configure(Binder binder)
  {

    final MapBinder<Class<? extends Query>, QueryRunnerFactory> queryFactoryBinder = DruidBinders.queryRunnerFactoryBinder(
            binder
    );

    for (Map.Entry<Class<? extends Query>, Class<? extends QueryRunnerFactory>> entry : queryRunnerMappings.entrySet()) {
      queryFactoryBinder.addBinding(entry.getKey()).to(entry.getValue());
      binder.bind(entry.getValue()).in(LazySingleton.class);
    }

    MapBinder<Class<? extends Query>, QueryToolChest> toolChests = DruidBinders.queryToolChestBinder(binder);

    for (Map.Entry<Class<? extends Query>, Class<? extends QueryToolChest>> entry : toolChestMappings.entrySet()) {
      toolChests.addBinding(entry.getKey()).to(entry.getValue());
      binder.bind(entry.getValue()).in(LazySingleton.class);
    }

    binder.bind(GroupByQueryEngine.class).in(LazySingleton.class);

    JsonConfigProvider.bind(binder, "druid.lucene.query.groupBy", GroupByQueryConfig.class);
    PolyBind.optionBinder(binder, Key.get(SegmentFactory.class))
            .addBinding("lucene")
            .to(LuceneSegmentFactory.class)
            .in(LazySingleton.class);

  }
}
