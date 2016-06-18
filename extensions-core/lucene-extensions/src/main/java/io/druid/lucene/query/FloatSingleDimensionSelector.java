package io.druid.lucene.query;

import org.apache.lucene.index.NumericDocValues;

import java.util.Arrays;
import java.util.List;

/**
 */
public class FloatSingleDimensionSelector extends FloatDimensionSelector {

    private NumericDocValues docValues;

    public FloatSingleDimensionSelector(NumericDocValues docValues) {
        this.docValues = docValues;
    }

    @Override
    public List<Float> getRow(int doc) {
        long longVal = docValues.get(doc);
        final float value = Float.intBitsToFloat((int)longVal);
        return Arrays.asList(value);
    }
}
