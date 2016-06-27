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

import io.druid.data.input.impl.DimensionSchema;
import io.druid.lucene.LuceneDirectory;
import io.druid.lucene.segment.mapping.FieldMappings;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.FSDirectory;
import org.joda.time.Interval;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 */
public class ReadOnlyDirectory implements LuceneDirectory {

    private final SegmentIdentifier segmentIdentifier;
    private final IndexReader indexReader;
    private final Map<String, DimensionSchema.ValueType> dimensions;

    public ReadOnlyDirectory(File parentDir, SegmentIdentifier segmentIdentifier) throws IOException {
        this.segmentIdentifier = segmentIdentifier;
        this.dimensions = FieldMappings.builder().buildFieldTypesFrom(parentDir).build().getFieldTypes();
        FSDirectory luceneDir = FSDirectory.open(parentDir.toPath());
        this.indexReader = DirectoryReader.open(luceneDir);
    }

    public SegmentIdentifier getSegmentIdentifier() {
        return segmentIdentifier;
    }

    @Override
    public int numRows() {
        return indexReader.numDocs();
    }

    @Override
    public IndexReader getIndexReader() {
        return indexReader;
    }

    @Override
    public Map<String, DimensionSchema.ValueType> getFieldTypes() {
        return this.dimensions;
    }

    @Override
    public void close() throws IOException {
        if (indexReader != null) {
            indexReader.close();
        }
    }

}
