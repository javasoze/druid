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
