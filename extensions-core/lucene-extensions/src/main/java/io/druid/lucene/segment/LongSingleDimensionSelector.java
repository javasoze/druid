package io.druid.lucene.segment;

import org.apache.lucene.index.NumericDocValues;

import java.util.Arrays;
import java.util.List;

/**
 */
public class LongSingleDimensionSelector extends LongDimensionSelector {
    private NumericDocValues docValues;

    public LongSingleDimensionSelector(NumericDocValues docValues) {
        this.docValues = docValues;
    }

    @Override
    public List<Long> getRow(int doc) {
        long value = docValues.get(doc);
        return Arrays.asList(value);
    }
}
