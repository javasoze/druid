package io.druid.lucene.segment;

import org.apache.lucene.index.SortedNumericDocValues;

import java.util.ArrayList;
import java.util.List;

/**
 */
public class FloatMultiDimensionSelector extends FloatDimensionSelector {
    private SortedNumericDocValues docValues;

    public FloatMultiDimensionSelector(SortedNumericDocValues docValues) {
        this.docValues = docValues;
    }

    @Override
    public List<Float> getRow(int doc) {
        docValues.setDocument(doc);
        List<Float> row = new ArrayList<>(docValues.count());
        for (int i=0;i<row.size();i++) {
            row.set(i, Float.intBitsToFloat((int)docValues.valueAt(i)));
        }
        return row;
    }


}
