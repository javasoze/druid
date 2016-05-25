package io.druid.lucene.segment.realtime;

import com.google.common.collect.Lists;
import io.druid.data.input.InputRow;
import io.druid.lucene.LuceneDirectory;
import io.druid.lucene.segment.realtime.LuceneDocumentBuilder;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;
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

    private static IndexWriter buildRamWriter(
            RAMDirectory dir, Analyzer analyzer, int maxDocsPerSegment) throws IOException {
        IndexWriterConfig writerConfig = new IndexWriterConfig(analyzer);
        writerConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
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

    private static IndexWriter buildPersistWriter(Directory dir) throws IOException {
        IndexWriterConfig writerConfig = new IndexWriterConfig(null);
        writerConfig.setUseCompoundFile(false);
        writerConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        writerConfig.setMergePolicy(NoMergePolicy.INSTANCE);
        writerConfig.setMergeScheduler(NoMergeScheduler.INSTANCE);
        return new IndexWriter(dir, writerConfig);
    }

    private static File indexDir(File basePersistDir) {
        return new File(basePersistDir, UUID.randomUUID().toString());
    }

    public RealtimeDirectory(SegmentIdentifier segmentIdentifier, File basePersistDir,
                              LuceneDocumentBuilder docBuilder, int maxDocsPerSegment) throws IOException {
        this.segmentIdentifier = segmentIdentifier;
        this.persistDir = new File(basePersistDir, segmentIdentifier.getIdentifierAsString());
        this.docBuilder = docBuilder;
        this.maxRowsPerSegment = maxDocsPerSegment;
        this.numRowsPersisted = 0;
        this.isOpen = true;
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
        ramDir = new RAMDirectory();
        ramWriter = buildRamWriter(ramDir, new StandardAnalyzer(), maxRowsPerSegment);
        numRowsAdded = 0;
        realtimeReader = null;
    }

    public void add(InputRow row) throws IOException {
        ensureOpen();
        Document doc = docBuilder.buildLuceneDocument(row);
        ramWriter.addDocument(doc);
        numRowsAdded++;
        if (numRowsAdded >= maxRowsPerSegment) {
            reset();
        }
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
                ramWriter = null;
                persistWriter.addIndexes(ramDir);
                persistWriter.commit();
                persistWriter.close();
                DirectoryReader reader = DirectoryReader.open(luceneDir);
                numRowsPersisted += reader.numDocs();
                persistedReaders.add(reader);
                DirectoryReader tmpReader = realtimeReader;
                realtimeReader = null;
                if (tmpReader != null) {
                    tmpReader.close();
                }
            }
        }
    }

    public SegmentIdentifier getIdentifier() {
        return segmentIdentifier;
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