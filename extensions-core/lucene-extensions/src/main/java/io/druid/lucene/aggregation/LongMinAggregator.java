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

package io.druid.lucene.aggregation;

import io.druid.lucene.segment.DimensionSelector;
import io.druid.query.aggregation.Aggregator;

import java.util.Comparator;

/**
 */
public class LongMinAggregator extends BaseLongAggregator
{
  static final Comparator COMPARATOR = LongSumAggregator.COMPARATOR;

  private final DimensionSelector selector;
  private final String name;

  private long min;

  public LongMinAggregator(String name, DimensionSelector selector)
  {
    super(selector);
    this.name = name;
    this.selector = selector;

    reset();
  }

  @Override
  public void aggregate()
  {
    long[] vals = getValue(0L);
    for (long val: vals) {
      min = Math.min(min, val);
    }
  }

  @Override
  public void reset()
  {
    min = Long.MAX_VALUE;
  }

  @Override
  public Object get()
  {
    return min;
  }

  @Override
  public float getFloat()
  {
    return (float) min;
  }

  @Override
  public long getLong()
  {
    return min;
  }

  @Override
  public String getName()
  {
    return this.name;
  }

  @Override
  public Aggregator clone()
  {
    return new LongMinAggregator(name, selector);
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
