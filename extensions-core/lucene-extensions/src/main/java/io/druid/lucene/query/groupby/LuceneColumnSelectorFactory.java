package io.druid.lucene.query.groupby;

import io.druid.lucene.segment.DimensionSelector;

/**
 */
public interface LuceneColumnSelectorFactory {
    public DimensionSelector makeDimensionSelector(String dim);
}
