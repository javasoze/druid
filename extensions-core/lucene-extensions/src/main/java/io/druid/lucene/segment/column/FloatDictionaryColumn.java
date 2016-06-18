package io.druid.lucene.segment.column;

import org.apache.lucene.index.NumericDocValues;

/**
 */
public class FloatDictionaryColumn implements DictionaryColumn<Float> {
    private NumericDocValues docValues;

    public FloatDictionaryColumn(NumericDocValues docValues) {
        this.docValues = docValues;
    }

    @Override
    public Float getValueForDoc(int doc) {
        long l1 = docValues.get(doc);
        return Float.intBitsToFloat((int)l1);
    }

    @Override
    public int getIdForDoc(int doc) {
        throw new UnsupportedOperationException("Not supported.");
    }


    @Override
    public Float lookupValue(int id) {
        throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public int lookupId(String name) {
        throw new UnsupportedOperationException("Not supported.");
    }
}
