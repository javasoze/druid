package io.druid.segment.loading;

import io.druid.segment.Segment;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;

import java.io.File;

/**
 *
 */
public interface SegmentFactory
{
    public Segment factorize(File parentDir, SegmentIdentifier identifier) throws SegmentLoadingException;
}
