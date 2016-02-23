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

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SkunkworksQueryResultValue
{
  private final long numHits;
  private final long totalCount;
  
  @JsonCreator
  public SkunkworksQueryResultValue(
      @JsonProperty("numHits") long numHits,
      @JsonProperty("totalCount") long totalCount
  )
  {
    this.numHits = numHits;
    this.totalCount = totalCount;
  }

  @JsonProperty
  public long getNumHits()
  {
    return numHits;
  }
  
  @JsonProperty
  public long getTotalCount()
  {
    return totalCount;
  }

  @Override
  public String toString()
  {
    return "SkunkworksQueryResultValue{" +
           "numHits=" + numHits +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SkunkworksQueryResultValue that = (SkunkworksQueryResultValue) o;
    return numHits == that.numHits && totalCount == that.totalCount;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(numHits, totalCount);
  }
}
