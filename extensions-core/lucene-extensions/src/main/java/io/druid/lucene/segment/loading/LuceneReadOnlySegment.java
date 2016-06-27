package io.druid.lucene.segment.loading;

import io.druid.lucene.LuceneDirectory;
import io.druid.segment.AbstractSegment;
import io.druid.segment.QueryableIndex;
import io.druid.segment.StorageAdapter;
import org.joda.time.Interval;

import java.io.IOException;

/**
 *
 */
public class LuceneReadOnlySegment extends AbstractSegment
{
    private final ReadOnlyDirectory directory;

    public LuceneReadOnlySegment(ReadOnlyDirectory directory)
    {
        this.directory = directory;
    }

    @Override
    public String getIdentifier()
    {
        return directory.getSegmentIdentifier().getIdentifierAsString();
    }

    @Override
    public Interval getDataInterval()
    {
        return directory.getSegmentIdentifier().getInterval();
    }

    @Override
    public <T> T as(Class<T> clazz)
    {
        if (clazz.equals(LuceneDirectory.class)) {
            return (T) directory;
        }
        return null;
    }

    @Override
    public QueryableIndex asQueryableIndex()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public StorageAdapter asStorageAdapter()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException
    {
        if (directory != null) {
            directory.close();
        }
    }
}
