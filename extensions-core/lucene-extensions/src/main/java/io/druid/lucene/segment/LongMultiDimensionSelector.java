package io.druid.lucene.segment;

import org.apache.lucene.index.SortedNumericDocValues;

import java.util.ArrayList;
import java.util.List;

/**
 */
public class LongMultiDimensionSelector extends LongDimensionSelector {
    private SortedNumericDocValues docValues;

    public LongMultiDimensionSelector(SortedNumericDocValues docValues) {
        this.docValues = docValues;
    }

    @Override
    public List<Long> getRow(int doc) {
        docValues.setDocument(doc);
        List<Long> row = new ArrayList<>(docValues.count());
        for (int i=0;i<row.size();i++) {
            row.set(i, docValues.valueAt(i));
        }
        return row;
    }
}
