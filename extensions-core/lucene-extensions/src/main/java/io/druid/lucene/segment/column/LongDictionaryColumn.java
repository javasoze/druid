package io.druid.lucene.segment.column;

import org.apache.lucene.index.NumericDocValues;

/**
 */
public class LongDictionaryColumn implements DictionaryColumn<Long> {
    private NumericDocValues docValues;

    public LongDictionaryColumn(NumericDocValues docValues) {
        this.docValues = docValues;
    }

    @Override
    public Long getValueForDoc(int doc) {
        return docValues.get(doc);
    }

    @Override
    public int getIdForDoc(int doc) {
        throw new UnsupportedOperationException("Not supported.");
    }


    @Override
    public Long lookupValue(int id) {
        throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public int lookupId(String name) {
        throw new UnsupportedOperationException("Not supported.");
    }
}
