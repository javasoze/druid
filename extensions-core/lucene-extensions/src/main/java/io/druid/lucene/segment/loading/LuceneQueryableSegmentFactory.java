package io.druid.lucene.segment.loading;

import io.druid.segment.Segment;
import io.druid.segment.loading.QueryableSegmentFactory;
import io.druid.segment.loading.SegmentLoadingException;
import org.joda.time.Interval;

import java.io.File;
import java.io.IOException;

/**
 */
public class LuceneQueryableSegmentFactory implements QueryableSegmentFactory {
    @Override
    public Segment factorize(String segmentIdentifier, Interval interval, File parentDir) throws SegmentLoadingException {
        try {
            return new LuceneQueryableSegment(segmentIdentifier, interval, new ReadOnlyDirectory(parentDir));
        } catch (IOException e) {
            throw new SegmentLoadingException(e, "%s", e.getMessage());
        }
    }
}
