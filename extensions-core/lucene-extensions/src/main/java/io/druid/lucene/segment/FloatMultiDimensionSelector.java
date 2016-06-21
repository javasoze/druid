package io.druid.lucene.segment;

import io.druid.lucene.query.groupby.LuceneCursor;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;

import java.util.ArrayList;
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
    public List<Float> getRow() {
        docValues.setDocument(cursor.getCurrentDoc());
        List<Float> row = new ArrayList<>(docValues.count());
        for (int i=0;i<row.size();i++) {
            row.set(i, Float.intBitsToFloat((int)docValues.valueAt(i)));
        }
        return row;
    }


}
