package io.druid.lucene.segment;

import io.druid.lucene.query.LuceneCursor;
import org.apache.lucene.index.SortedNumericDocValues;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 */
public class FloatMultiDimensionSelector extends FloatDimensionSelector {
    private SortedNumericDocValues docValues;
    private LuceneCursor cursor;

    public FloatMultiDimensionSelector(LuceneCursor cursor, SortedNumericDocValues docValues) {
        this.docValues = docValues;
        this.cursor = cursor;
    }


    @Override
    public List<Float> getIds() {
        if (null != docValues) {
            docValues.setDocument(cursor.getCurrentDoc());
            List<Float> row = new ArrayList<>(docValues.count());
            for (int i=0;i<row.size();i++) {
                row.set(i, Float.intBitsToFloat((int)docValues.valueAt(i)));
            }
            return row;
        }
        return Arrays.asList(NO_VALUE_FOR_ROW);
    }

    @Override
    public List<Float> getValues() {
        if (null != docValues) {
            docValues.setDocument(cursor.getCurrentDoc());
            List<Float> row = new ArrayList<>(docValues.count());
            for (int i=0;i<row.size();i++) {
                row.set(i, Float.intBitsToFloat((int)docValues.valueAt(i)));
            }
            return row;
        }
        return Collections.emptyList();
    }


}
