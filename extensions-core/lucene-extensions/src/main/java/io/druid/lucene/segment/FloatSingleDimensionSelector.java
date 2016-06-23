package io.druid.lucene.segment;

import io.druid.lucene.query.groupby.LuceneCursor;
import org.apache.lucene.index.NumericDocValues;

import java.util.Arrays;
import java.util.Collections;
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
    public List<Float> getIds() {
        if (null != docValues) {
            long longVal = docValues.get(cursor.getCurrentDoc());
            final float value = Float.intBitsToFloat((int)longVal);
            return Arrays.asList(value);
        }

        return Arrays.asList(NO_VALUE_FOR_ROW);
    }

    @Override
    public List<Float> getValues() {
        if(null != docValues) {
            float value = docValues.get(cursor.getCurrentDoc());
            return Arrays.asList(value);
        }
        return Collections.emptyList();
    }
}
