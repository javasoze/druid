package io.druid.lucene.segment;

import io.druid.lucene.query.LuceneCursor;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 */
public class StringSingleDimensionSelector implements DimensionSelector<Integer,String> {
    private int NO_VALUE_FOR_ROW = Integer.MAX_VALUE;
    private SortedDocValues docValues;
    private LuceneCursor cursor;

    public StringSingleDimensionSelector(LuceneCursor cursor, SortedDocValues docValues) {
        this.docValues = docValues;
        this.cursor = cursor;
    }

    @Override
    public List<Integer> getIds() {
        int ord;
        if (null != docValues && SortedSetDocValues.NO_MORE_ORDS != (ord = docValues.getOrd(cursor.getCurrentDoc()))) {
            return Arrays.asList(ord);
        }
        return Arrays.asList(NO_VALUE_FOR_ROW);
    }

    @Override
    public List<String> getValues() {
        if (null != docValues) {
            String val = docValues.get(cursor.getCurrentDoc()).utf8ToString();
            return Arrays.asList(val);
        }
        return Collections.emptyList();
    }

    @Override
    public String lookupName(Integer id) {
        if (id == NO_VALUE_FOR_ROW) {
            return null;
        }
        return docValues.lookupOrd(id).utf8ToString();
    }


    @Override
    public Type getType() {
        return Type.INT;
    }
}
