/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.lucene.segment.realtime;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.druid.data.input.InputRow;
import io.druid.lucene.LuceneDirectory;
import io.druid.lucene.segment.realtime.LuceneDocumentBuilder;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;
import io.druid.timeline.DataSegment;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 */
public class RealtimeDirectory implements LuceneDirectory {
    private final SegmentIdentifier segmentIdentifier;
    private final LuceneDocumentBuilder docBuilder;
    private final int maxRowsPerSegment;
    private final File persistDir;
    private int numRowsAdded;
    private RAMDirectory ramDir;
    private IndexWriter ramWriter;
    private int numRowsPersisted;
    private volatile DirectoryReader realtimeReader;
    private volatile boolean isOpen;
    private final Object refreshLock = new Object();
    private ConcurrentLinkedQueue<DirectoryReader> persistedReaders = new ConcurrentLinkedQueue<DirectoryReader>();
    private volatile boolean writable = true;

    private static IndexWriter buildRamWriter(
            RAMDirectory dir, Analyzer analyzer, int maxDocsPerSegment) throws IOException {
        IndexWriterConfig writerConfig = new IndexWriterConfig(analyzer);
        writerConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        // some arbitrary large numbers
        writerConfig.setMaxBufferedDocs(maxDocsPerSegment * 2);
        writerConfig.setRAMBufferSizeMB(5000);
        writerConfig.setUseCompoundFile(true);
        writerConfig.setCommitOnClose(true);
        writerConfig.setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE);
        writerConfig.setMergePolicy(NoMergePolicy.INSTANCE);
        writerConfig.setMergeScheduler(NoMergeScheduler.INSTANCE);
        return new IndexWriter(dir, writerConfig);
    }

    private static IndexWriter buildPersistWriter(Directory dir) throws IOException {
        IndexWriterConfig writerConfig = new IndexWriterConfig(null);
        writerConfig.setUseCompoundFile(true);
        writerConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        writerConfig.setMergePolicy(NoMergePolicy.INSTANCE);
        writerConfig.setMergeScheduler(NoMergeScheduler.INSTANCE);
        return new IndexWriter(dir, writerConfig);
    }

    private static File indexDir(File basePersistDir) {
        return new File(basePersistDir, UUID.randomUUID().toString());
    }

    private File computePersistDir(File basePersistDir, SegmentIdentifier identifier)
    {
        return new File(basePersistDir, identifier.getIdentifierAsString());
    }

    public RealtimeDirectory(SegmentIdentifier segmentIdentifier, File basePersistDir,
                              LuceneDocumentBuilder docBuilder, int maxDocsPerSegment) throws IOException {
        this.segmentIdentifier = segmentIdentifier;
        this.docBuilder = docBuilder;
        this.maxRowsPerSegment = maxDocsPerSegment;
        this.numRowsPersisted = 0;
        this.isOpen = true;
        persistDir = computePersistDir(basePersistDir, segmentIdentifier);
        persistDir.mkdirs();
        reset();
    }

    private void ensureOpen() {
        if (!isOpen) {
            throw new AlreadyClosedException("directory already closed");
        }
    }

    private void reset() throws IOException {
        if (!isOpen) {
            return;
        }

        numRowsAdded = 0;
        ramDir = new RAMDirectory();
        ramWriter = buildRamWriter(ramDir, new StandardAnalyzer(), maxRowsPerSegment);
        DirectoryReader tmpReader = realtimeReader;
        realtimeReader = null;
        if (tmpReader != null) {
            tmpReader.close();
        }
    }

    public void add(InputRow row) throws IOException {
        ensureOpen();
        Document doc = docBuilder.buildLuceneDocument(row);
        ramWriter.addDocument(doc);
        numRowsAdded++;
    }

    public boolean canAppendRow()
    {
        return writable && numRowsAdded < maxRowsPerSegment;
    }

    /**
     * Refreshes the internal index reader on a scheduled thread.
     * This is a controlled thread so it is ok to block this thread to make sure things are in a good state.
     * @throws IOException
     */
    public void refreshRealtimeReader() throws IOException {
        synchronized (refreshLock) {
            if (!isOpen) {
                return;
            }
            if (ramWriter != null) {
                if (realtimeReader == null) {
                    realtimeReader = DirectoryReader.open(ramWriter, true);
                } else {
                    DirectoryReader newReader = DirectoryReader.openIfChanged(realtimeReader, ramWriter, true);
                    if (newReader != null) {
                        DirectoryReader tmpReader = realtimeReader;
                        realtimeReader = newReader;
                        tmpReader.close();
                    }
                }
            }
        }
    }

    /**
     * Persists the segment to disk. This will be called by the same thread that calls {@link #add(InputRow)} and
     * {@link #close()}
     * @throws IOException
     */
    public void persist() throws IOException {
        synchronized(refreshLock) {
            FSDirectory luceneDir = FSDirectory.open(indexDir(persistDir).toPath());
            try(IndexWriter persistWriter = buildPersistWriter(luceneDir)) {
                ramWriter.commit();
                ramWriter.close();
                persistWriter.addIndexes(ramDir);
                persistWriter.commit();
                persistWriter.close();
                DirectoryReader reader = DirectoryReader.open(luceneDir);
                numRowsPersisted += reader.numDocs();
                persistedReaders.add(reader);
                reset();
            }
        }
    }

    public File merge() throws IOException {
        synchronized(refreshLock) {
            final File mergedTarget = persistDir;
            FSDirectory luceneDir = FSDirectory.open(mergedTarget.toPath());
            try(IndexWriter persistWriter = buildPersistWriter(luceneDir)) {
                for (DirectoryReader persistedReader : persistedReaders) {
                    persistWriter.addIndexes(persistedReader.directory());
                }
                persistWriter.forceMerge(1);
                persistWriter.commit();
                persistWriter.close();
                for (DirectoryReader persistedReader : persistedReaders) {
                    persistedReader.close();
                    removeDirectory(((FSDirectory)persistedReader.directory()).getDirectory().toFile());
                }
                persistedReaders.clear();
                DirectoryReader reader = DirectoryReader.open(luceneDir);
                persistedReaders.add(reader);
                return mergedTarget;
            }
        }
    }

    private void removeDirectory(final File target)
    {
        if (target.exists()) {
            try {
                FileUtils.deleteDirectory(target);
            }
            catch (Exception e) {
            }
        }
    }

    public DataSegment getSegment()
    {
        return new DataSegment(
                segmentIdentifier.getDataSource(),
                segmentIdentifier.getInterval(),
                segmentIdentifier.getVersion(),
                ImmutableMap.<String, Object>of(),
                Lists.<String>newArrayList(),
                Lists.<String>newArrayList(),
                segmentIdentifier.getShardSpec(),
                null,
                numRows()
        );
    }

    public SegmentIdentifier getIdentifier() {
        return segmentIdentifier;
    }

    public File getPersistDir() {
        return persistDir;
    }

    /**
     * Gets an index reader for search. This can be accessed by multiple threads and cannot be blocking.
     * @return an index reader containing in memory realtime index as well as persisted indexes. Null
     * if the index is either closed or has no documents yet indexed.
     * @throws IOException
     */
    @Override
    public IndexReader getIndexReader() throws IOException {
        // let's not build a reader if
        if (!isOpen) {
            return null;
        }
        List<DirectoryReader> readers = Lists.newArrayListWithCapacity(persistedReaders.size() + 1);
        readers.addAll(persistedReaders);
        DirectoryReader localReaderRef = realtimeReader;
        if (localReaderRef != null) {
            readers.add(localReaderRef);
        }
        return readers.isEmpty() ? null : new MultiReader(readers.toArray(new IndexReader[readers.size()]), false);
    }

    @Override
    public int numRows() {
        return (realtimeReader == null ? 0 : realtimeReader.numDocs()) + numRowsPersisted;
    }

    /**
     * Closes this segment and a {@link #persist()} will be called if there are pending writes.
     */
    @Override
    public void close() throws IOException {
        synchronized(refreshLock) {
            ensureOpen();
            isOpen =false;
            try {
                if (ramWriter.numDocs() > 0) {
                    persist();
                }
                ramWriter.close();
                ramWriter = null;
                if (realtimeReader != null) {
                    realtimeReader.close();
                    realtimeReader = null;
                }
            } finally {
                for (DirectoryReader reader : persistedReaders) {
                    try {
                        reader.close();
                    } catch (IOException ioe) {
                    }
                }
                persistedReaders.clear();
            }
        }
    }
}