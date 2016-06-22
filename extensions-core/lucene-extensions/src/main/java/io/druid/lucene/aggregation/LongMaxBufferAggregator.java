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
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.LongColumnSelector;

import java.nio.ByteBuffer;

/**
 */
public class LongMaxBufferAggregator extends BaseLongBufferAggregator
{
  public LongMaxBufferAggregator(DimensionSelector selector)
  {
    super(selector);
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.putLong(position, Long.MIN_VALUE);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    long[] vals = getValue(0L);
    long max = buf.getLong(position);
    for (long val: vals) {
      max = Math.max(max, val);
    }
    buf.putLong(position, max);
  }
}
