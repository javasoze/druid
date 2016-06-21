package io.druid.lucene.segment;

import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 */
public class StringMultiDimensionSelector implements DimensionSelector<Long> {
    private SortedSetDocValues docValues;

    public StringMultiDimensionSelector(SortedSetDocValues docValues) {
        this.docValues = docValues;
    }

    @Override
    public List<Long> getRow(int doc) {
        docValues.setDocument(doc);
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
