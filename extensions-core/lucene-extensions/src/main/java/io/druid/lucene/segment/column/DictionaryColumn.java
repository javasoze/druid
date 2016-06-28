package io.druid.lucene.segment.column;

/**
 */
public interface DictionaryColumn<T> {
    int getIdForDoc(int doc);

    T getValueForDoc(int doc);

    T lookupValue(int id);

    int lookupId(String name);
}