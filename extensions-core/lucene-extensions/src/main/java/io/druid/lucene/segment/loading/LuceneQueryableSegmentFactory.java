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
package io.druid.lucene.segment.loading;

import io.druid.segment.Segment;
import io.druid.segment.loading.QueryableSegmentFactory;
import io.druid.segment.loading.SegmentLoadingException;
import org.joda.time.Interval;

import java.io.File;
import java.io.IOException;

/**
 */
public class LuceneQueryableSegmentFactory implements QueryableSegmentFactory {
    @Override
    public Segment factorize(String segmentIdentifier, Interval interval, File parentDir) throws SegmentLoadingException {
        try {
            return new LuceneQueryableSegment(segmentIdentifier, interval, new ReadOnlyDirectory(parentDir));
        } catch (IOException e) {
            throw new SegmentLoadingException(e, "%s", e.getMessage());
        }
    }
}
