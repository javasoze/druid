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

import io.druid.lucene.LuceneDirectory;
import io.druid.segment.AbstractSegment;
import io.druid.segment.QueryableIndex;
import io.druid.segment.StorageAdapter;
import org.joda.time.Interval;

import java.io.IOException;

/**
 */
public class LuceneIncrementalSegment extends AbstractSegment {
    private final RealtimeDirectory realtimeDirectory;

    public LuceneIncrementalSegment(RealtimeDirectory realtimeDirectory) {
        this.realtimeDirectory = realtimeDirectory;
    }

    @Override
    public String getIdentifier() {
        return realtimeDirectory.getIdentifier().getIdentifierAsString();
    }

    @Override
    public Interval getDataInterval() {
        return realtimeDirectory.getIdentifier().getInterval();
    }

    @Override
    public <T> T as(Class<T> clazz) {
        if (clazz.equals(LuceneDirectory.class)) {
            return (T) realtimeDirectory;
        }
        return null;
    }

    @Override
    public QueryableIndex asQueryableIndex() {
        throw new UnsupportedOperationException();
    }

    @Override
    public StorageAdapter asStorageAdapter() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        realtimeDirectory.close();
    }
}
