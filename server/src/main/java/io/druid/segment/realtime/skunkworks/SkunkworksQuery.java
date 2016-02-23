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

package io.druid.segment.realtime.skunkworks;

import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.Result;
import io.druid.query.spec.QuerySegmentSpec;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SkunkworksQuery extends BaseQuery<Result<SkunkworksQueryResultValue>>
{
  private static final String TYPE = "skunkworks";
  private final String queryString;
  private final int count;

  @JsonCreator
  public SkunkworksQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("context") Map<String, Object> context,
      @JsonProperty("query") String queryString,
      @JsonProperty("count") int count
  )
  {
    super(
        dataSource,
        querySegmentSpec,
        false,
        context
    );
    this.queryString = queryString;
    this.count = count;
  }
  
  public int getCount() {
    return count;
  }
  
  public String getQueryString() {
    return queryString;
  }

  @Override
  public boolean hasFilters()
  {
    return false;
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @Override
  public Query<Result<SkunkworksQueryResultValue>> withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new SkunkworksQuery(getDataSource(), getQuerySegmentSpec(), 
        computeOverridenContext(contextOverride), queryString, count);
  }

  @Override
  public Query<Result<SkunkworksQueryResultValue>> withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new SkunkworksQuery(getDataSource(), spec, getContext(), queryString, count);
  }

  @Override
  public Query<Result<SkunkworksQueryResultValue>> withDataSource(DataSource dataSource)
  {
    return new SkunkworksQuery(dataSource, getQuerySegmentSpec(), getContext(), queryString, count);
  }
}
