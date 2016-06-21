package io.druid.lucene.segment;

import io.druid.lucene.query.groupby.LuceneCursor;
import org.apache.lucene.index.NumericDocValues;

import java.util.Arrays;
import java.util.List;

/**
 */
public class LongSingleDimensionSelector extends LongDimensionSelector {
    private NumericDocValues docValues;
    private LuceneCursor cursor;

    public LongSingleDimensionSelector(LuceneCursor cursor, NumericDocValues docValues) {
        this.docValues = docValues;
        this.cursor = cursor;
    }

    @Override
    public List<Long> getRow() {
        long value = docValues.get(cursor.getCurrentDoc());
        return Arrays.asList(value);
    }
}
