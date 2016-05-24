package io.druid.lucene;

import io.druid.data.input.InputRow;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.util.List;

/**
 */
public interface LuceneDirectory {
    public int numRows();

    public IndexReader getIndexReader() throws IOException;

    public void close() throws IOException;
}
