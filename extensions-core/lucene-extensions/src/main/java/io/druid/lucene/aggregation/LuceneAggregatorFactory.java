package io.druid.lucene.aggregation;

import io.druid.lucene.query.groupby.LuceneCursor;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;

/**
 */
public abstract class LuceneAggregatorFactory extends AggregatorFactory {
    @Override
    public Aggregator factorize(ColumnSelectorFactory metricFactory) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory) {
        throw new UnsupportedOperationException();
    }

    public abstract BufferAggregator factorizeBuffered(LuceneCursor cursor);
}
