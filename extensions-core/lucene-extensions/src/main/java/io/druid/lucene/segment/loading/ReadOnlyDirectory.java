package io.druid.lucene.segment.loading;

import io.druid.lucene.LuceneDirectory;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.IOException;

/**
 */
public class ReadOnlyDirectory implements LuceneDirectory {
    private final IndexReader indexReader;

    public ReadOnlyDirectory(File parentDir) throws IOException {
        FSDirectory luceneDir = FSDirectory.open(parentDir.toPath());
        indexReader = DirectoryReader.open(luceneDir);
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
    public void close() throws IOException {
        indexReader.close();
    }
}
