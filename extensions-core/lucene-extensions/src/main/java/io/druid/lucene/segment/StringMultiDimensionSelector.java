package io.druid.lucene.segment;

import io.druid.lucene.query.groupby.LuceneCursor;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 */
public class StringMultiDimensionSelector implements DimensionSelector<Long> {
    private SortedSetDocValues docValues;
    private LuceneCursor cursor;

    public StringMultiDimensionSelector(LuceneCursor cursor, SortedSetDocValues docValues) {
        this.docValues = docValues;
        this.cursor = cursor;
    }

    @Override
    public List<Long> getRow() {
        docValues.setDocument(cursor.getCurrentDoc());
        long ord;
        List<Long> row = new ArrayList<>();
        while (SortedSetDocValues.NO_MORE_ORDS != (ord = docValues.nextOrd())) {
            row.add(ord);
        }
        return row;
    }

    @Override
    public String lookupName(Long id) {
        return docValues.lookupOrd(id).toString();
    }

    @Override
    public Type getType() {
        return Type.LONG;
    }

}
