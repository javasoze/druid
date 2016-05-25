package io.druid.lucene.segment.loading;

import io.druid.lucene.LuceneDirectory;
import io.druid.segment.AbstractSegment;
import io.druid.segment.QueryableIndex;
import io.druid.segment.StorageAdapter;
import org.joda.time.Interval;

import java.io.File;
import java.io.IOException;

/**
 */
public class LuceneQueryableSegment extends AbstractSegment {
    private final String identifier;
    private final Interval interval;
    private final ReadOnlyDirectory readOnlyDirectory;

    public LuceneQueryableSegment(String segmentIdentifier, Interval interval, ReadOnlyDirectory readOnlyDirectory) {
        this.interval = interval;
        this.readOnlyDirectory = readOnlyDirectory;
        identifier = segmentIdentifier;

    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public Interval getDataInterval() {
        return interval;
    }

    @Override
    public <T> T as(Class<T> clazz) {
        if (clazz.equals(LuceneDirectory.class)) {
            return (T) readOnlyDirectory;
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

    }
}
