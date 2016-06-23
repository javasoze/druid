package io.druid.lucene.segment;

import io.druid.lucene.query.groupby.LuceneCursor;
import org.apache.lucene.index.SortedNumericDocValues;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 */
public class LongMultiDimensionSelector extends LongDimensionSelector {
    private SortedNumericDocValues docValues;
    private LuceneCursor cursor;

    public LongMultiDimensionSelector(LuceneCursor cursor, SortedNumericDocValues docValues) {
        this.docValues = docValues;
        this.cursor = cursor;
    }

    @Override
    public List<Long> getIds() {
        if (null != docValues) {
            docValues.setDocument(cursor.getCurrentDoc());
            List<Long> row = new ArrayList<>(docValues.count());
            for (int i=0;i<row.size();i++) {
                row.set(i, docValues.valueAt(i));
            }
            return row;
        }
        return Arrays.asList(NO_VALUE_FOR_ROW);
    }

    @Override
    public List<Long> getValues() {
        if (null != docValues) {
            List<Long> row = new ArrayList<>();
            docValues.setDocument(cursor.getCurrentDoc());
            for (int i=0;i<row.size();i++) {
                row.set(i, docValues.valueAt(i));
            }
            return row;
        }
        return Collections.emptyList();
    }
}
