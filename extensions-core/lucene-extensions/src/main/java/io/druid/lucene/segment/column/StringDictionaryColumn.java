package io.druid.lucene.segment.column;

import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.BytesRef;

/**
 */
public class StringDictionaryColumn implements DictionaryColumn<String> {
    private SortedDocValues docValues;

    public StringDictionaryColumn(SortedDocValues docValues) {
        this.docValues = docValues;
    }

    @Override
    public String getValueForDoc(int doc) {
        return docValues.get(doc).toString();
    }

    @Override
    public int getIdForDoc(int doc) {
        return docValues.getOrd(doc);
    }

    @Override
    public String lookupValue(int id) {
        return docValues.lookupOrd(id).toString();
    }

    @Override
    public int lookupId(String name) {
        return docValues.lookupTerm(new BytesRef(name));
    }
}
