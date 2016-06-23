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

import com.google.common.primitives.Longs;
import io.druid.lucene.segment.DimensionSelector;
import io.druid.query.aggregation.Aggregator;
import io.druid.segment.LongColumnSelector;

import java.util.Comparator;

/**
 */
public class LongSumAggregator extends BaseLongAggregator
{
  static final Comparator COMPARATOR = new Comparator()
  {
    @Override
    public int compare(Object o, Object o1)
    {
      return Longs.compare(((Number) o).longValue(), ((Number) o1).longValue());
    }
  };

  private final DimensionSelector selector;
  private final String name;

  private long sum;

  public LongSumAggregator(String name, DimensionSelector selector)
  {
    super(selector);
    this.name = name;
    this.selector = selector;
    this.sum = 0;
  }

  @Override
  public void aggregate()
  {
    long[] vals = getValue(0L);
    long rowSum = 0L;
    for (long val: vals) {
      rowSum += val;
    }
    sum += rowSum;
  }

  @Override
  public void reset()
  {
    sum = 0;
  }

  @Override
  public Object get()
  {
    return sum;
  }

  @Override
  public float getFloat()
  {
    return (float) sum;
  }

  @Override
  public long getLong()
  {
    return sum;
  }

  @Override
  public String getName()
  {
    return name;
  }

  @Override
  public Aggregator clone()
  {
    return new LongSumAggregator(name, selector);
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
