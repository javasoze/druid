package io.druid.lucene.segment;

import java.util.List;

/**
 */
public interface ColumnSelector<T> {
    public List<T> getValues();
}
