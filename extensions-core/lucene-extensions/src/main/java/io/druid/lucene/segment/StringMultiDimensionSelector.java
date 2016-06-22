package io.druid.lucene.segment;

import io.druid.lucene.query.groupby.LuceneCursor;
import org.apache.lucene.index.SortedSetDocValues;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 */
public class StringMultiDimensionSelector implements DimensionSelector<Long, String> {
    private long NO_VALUE_FOR_ROW = Long.MAX_VALUE;
    private SortedSetDocValues docValues;
    private LuceneCursor cursor;

    public StringMultiDimensionSelector(LuceneCursor cursor, SortedSetDocValues docValues) {
        this.docValues = docValues;
        this.cursor = cursor;
    }

    @Override
    public List<Long> getIds() {
        if (null != docValues) {
            docValues.setDocument(cursor.getCurrentDoc());
            long ord;
            List<Long> row = new ArrayList<>();
            while (SortedSetDocValues.NO_MORE_ORDS != (ord = docValues.nextOrd())) {
                row.add(ord);
            }
            return row;
        }
        return Arrays.asList(NO_VALUE_FOR_ROW);
    }

    @Override
    public List<String> getValues() {
        List<String> row = new ArrayList<>();
        if (null != docValues) {
            docValues.setDocument(cursor.getCurrentDoc());
            long ord;
            while (SortedSetDocValues.NO_MORE_ORDS != (ord = docValues.nextOrd())) {
                row.add(docValues.lookupOrd(ord).utf8ToString());
            }
            return row;
        }
        return row;
    }

    @Override
    public String lookupName(Long id) {
        if (id == NO_VALUE_FOR_ROW) {
            return null;
        }
        return docValues.lookupOrd(id).utf8ToString();
    }

    @Override
    public Type getType() {
        return Type.LONG;
    }

}
