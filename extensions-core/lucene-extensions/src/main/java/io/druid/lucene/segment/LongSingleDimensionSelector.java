package io.druid.lucene.segment;

import io.druid.lucene.query.groupby.LuceneCursor;
import org.apache.lucene.index.NumericDocValues;

import java.util.Arrays;
import java.util.Collections;
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
    public List<Long> getIds() {
        if(null != docValues) {
            long value = docValues.get(cursor.getCurrentDoc());
            return Arrays.asList(value);
        }
        return Arrays.asList(NO_VALUE_FOR_ROW);
    }

    @Override
    public List<Long> getValues() {
        if(null != docValues) {
            long value = docValues.get(cursor.getCurrentDoc());
            return Arrays.asList(value);
        }
        return Collections.emptyList();
    }
}
