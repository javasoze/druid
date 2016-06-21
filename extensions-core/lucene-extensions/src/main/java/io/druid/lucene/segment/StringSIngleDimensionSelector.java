package io.druid.lucene.segment;

import io.druid.lucene.query.groupby.LuceneCursor;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.BytesRef;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

/**
 */
public class StringSIngleDimensionSelector implements DimensionSelector<Integer> {
    private SortedDocValues docValues;
    private LuceneCursor cursor;

    public StringSIngleDimensionSelector(LuceneCursor cursor, SortedDocValues docValues) {
        this.docValues = docValues;
        this.cursor = cursor;
    }

    @Override
    public List<Integer> getRow() {
        int ord = docValues.getOrd(cursor.getCurrentDoc());
        return Arrays.asList(ord);
    }

    @Override
    public String lookupName(Integer id) {
        return docValues.get(id).toString();
    }


    @Override
    public Type getType() {
        return Type.INT;
    }
}
