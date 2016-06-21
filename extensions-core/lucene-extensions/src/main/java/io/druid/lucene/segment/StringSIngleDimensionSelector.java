package io.druid.lucene.segment;

import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.BytesRef;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

/**
 */
public class StringSIngleDimensionSelector implements DimensionSelector<Integer> {
    private SortedDocValues docValues;

    public StringSIngleDimensionSelector(SortedDocValues docValues) {
        this.docValues = docValues;
    }

    @Override
    public List<Integer> getRow(int doc) {
        int ord = docValues.getOrd(doc);
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
