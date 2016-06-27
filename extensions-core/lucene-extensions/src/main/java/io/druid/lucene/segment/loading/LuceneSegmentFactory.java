package io.druid.lucene.segment.loading;

import com.metamx.common.logger.Logger;
import io.druid.segment.QueryableIndex;
import io.druid.segment.Segment;
import io.druid.segment.loading.QueryableIndexFactory;
import io.druid.segment.loading.SegmentFactory;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;

import java.io.File;
import java.io.IOException;

/**
 *
 */
public class LuceneSegmentFactory implements SegmentFactory
{
    private static final Logger log = new Logger(LuceneSegmentFactory.class);

    @Override
    public Segment factorize(File parentDir, SegmentIdentifier identifier) throws SegmentLoadingException
    {
        try {
            ReadOnlyDirectory directory = new ReadOnlyDirectory(parentDir, identifier);
            return new LuceneReadOnlySegment(directory);
        } catch (IOException e) {
            throw new SegmentLoadingException(e, "%s", e.getMessage());
        }
    }
}
