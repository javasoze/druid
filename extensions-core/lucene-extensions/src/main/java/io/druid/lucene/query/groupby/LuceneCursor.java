package io.druid.lucene.query.groupby;

import io.druid.lucene.segment.DimensionSelector;
import io.druid.lucene.segment.FloatSingleDimensionSelector;
import io.druid.lucene.segment.LongSingleDimensionSelector;
import io.druid.lucene.segment.StringSIngleDimensionSelector;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.column.ValueType;
import org.apache.lucene.index.LeafReader;

import java.io.IOException;
import java.util.Map;

/**
 */
public class LuceneCursor{
    private final LeafReader leafReader;
    private final Map<String, ValueType> dimTypes;

    public LuceneCursor(LeafReader leafReader, Map<String, ValueType> dimTypes) {
        this.leafReader = leafReader;
        this.dimTypes = dimTypes;
    }

    public DimensionSelector<Long> makeTimestampSelector() throws IOException {
        DimensionSelector selector = new LongSingleDimensionSelector(leafReader.getNumericDocValues("_timestamp"));
        return selector;
    }

    public DimensionSelector makeDimensionSelector(DimensionSpec dimSpec) throws IOException {
        final String dim = dimSpec.getDimension();
        ValueType type = dimTypes.get(dim);
        DimensionSelector selector = null;
        switch (type) {
            case LONG:
                selector = new LongSingleDimensionSelector(leafReader.getNumericDocValues(dim));
                break;
            case FLOAT:
                selector = new FloatSingleDimensionSelector(leafReader.getNumericDocValues(dim));
                break;
            case STRING:
                selector = new StringSIngleDimensionSelector(leafReader.getSortedDocValues(dim));
                break;
            default:
                break;
        }
        return selector;
    }
}
