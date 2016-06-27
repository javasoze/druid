package io.druid.segment.loading;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import io.druid.segment.IndexIO;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.Segment;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;

import java.io.File;
import java.io.IOException;

/**
 *
 */
public class QueryableIndexSegmentFactory implements SegmentFactory
{

    private final IndexIO indexIO;

    @Inject
    public QueryableIndexSegmentFactory(IndexIO indexIO)
    {
        this.indexIO = Preconditions.checkNotNull(indexIO, "Null IndexIO");
    }

    @Override
    public Segment factorize(File parentDir, SegmentIdentifier identifier) throws SegmentLoadingException {
        try {
            QueryableIndex index = indexIO.loadIndex(parentDir);
            return new QueryableIndexSegment(identifier.getIdentifierAsString(), index);
        }
        catch (IOException e) {
            throw new SegmentLoadingException(e, "%s", e.getMessage());
        }
    }
}
