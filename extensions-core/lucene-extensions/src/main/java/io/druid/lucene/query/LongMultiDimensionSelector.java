package io.druid.lucene.query;

import io.druid.segment.data.IndexedInts;
import org.apache.lucene.index.NumericDocValues;

import java.util.Iterator;
import java.util.List;

/**
 */
public class LongMultiDimensionSelector extends LongDimensionSelector {
    private NumericDocValues docValues;

    @Override
    public List<Long> getRow(int doc) {
        return null;
    }
}
