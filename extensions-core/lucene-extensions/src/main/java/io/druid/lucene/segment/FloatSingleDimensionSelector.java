package io.druid.lucene.segment;

import io.druid.lucene.query.groupby.LuceneCursor;
import org.apache.lucene.index.NumericDocValues;

import java.util.Arrays;
import java.util.List;

/**
 */
public class FloatSingleDimensionSelector extends FloatDimensionSelector {
    private NumericDocValues docValues;
    private LuceneCursor cursor;

    public FloatSingleDimensionSelector(LuceneCursor cursor, NumericDocValues docValues) {
        this.docValues = docValues;
        this.cursor = cursor;
    }

    @Override
    public List<Float> getRow() {
        long longVal = docValues.get(cursor.getCurrentDoc());
        final float value = Float.intBitsToFloat((int)longVal);
        return Arrays.asList(value);
    }
}
