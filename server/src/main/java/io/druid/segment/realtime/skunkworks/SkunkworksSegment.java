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

import io.druid.data.input.InputRow;
import io.druid.segment.QueryableIndex;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.codecs.lucene50.Lucene50StoredFieldsFormat.Mode;
import org.apache.lucene.codecs.lucene54.Lucene54Codec;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.NoDeletionPolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.NoMergeScheduler;
import org.apache.lucene.store.RAMDirectory;
import org.joda.time.Interval;

public class SkunkworksSegment implements Segment
{
  private final SegmentIdentifier identifier;
  private RAMDirectory dir;
  private final int maxRowsInMemory;
  private int numRows = 0;
  
  static IndexWriter buildRamWriter(RAMDirectory dir, Analyzer analyzer, int maxDocsPerSegment) throws IOException {
    IndexWriterConfig writerConfig = new IndexWriterConfig(analyzer);
    writerConfig.setOpenMode(OpenMode.CREATE_OR_APPEND);
    writerConfig.setCodec(new Lucene54Codec(Mode.BEST_COMPRESSION));
    // some arbitrary large numbers
    writerConfig.setMaxBufferedDocs(maxDocsPerSegment * 2);        
    writerConfig.setRAMBufferSizeMB(5000);
    writerConfig.setUseCompoundFile(false);
    writerConfig.setCommitOnClose(true);
    writerConfig.setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE);
    writerConfig.setMergePolicy(NoMergePolicy.INSTANCE);
    writerConfig.setMergeScheduler(NoMergeScheduler.INSTANCE);
    return new IndexWriter(dir, writerConfig);
  }

  public SkunkworksSegment(
      SegmentIdentifier identifier,
      int maxRowsInMemory
  )
  {
    this.identifier = identifier;
    this.dir = new RAMDirectory();
    this.maxRowsInMemory = maxRowsInMemory;
  }

  @Override
  public String getIdentifier()
  {
    return identifier.getIdentifierAsString();
  }

  @Override
  public Interval getDataInterval()
  {
    return identifier.getInterval();
  }

  @Override
  public QueryableIndex asQueryableIndex()
  {
    return null;
  }

  @Override
  public StorageAdapter asStorageAdapter()
  {
    return null;
  }
  
  public void add(InputRow row) {	  
	numRows ++;
  }
  
  public int getNumRows() {
	return numRows;
  }

  /**
   * Hack alert! Should really be a new "asXXX" method in "Segment".
   */
  public IndexReader getIndexReader()
  {
	  return null;
  }

  @Override
  public void close() throws IOException
  {
  }
}
