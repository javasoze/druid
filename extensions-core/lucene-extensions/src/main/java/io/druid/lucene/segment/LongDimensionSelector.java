package io.druid.lucene.segment;

/**
 */
public abstract class LongDimensionSelector implements DimensionSelector<Long, Long> {
    protected long NO_VALUE_FOR_ROW = Long.MAX_VALUE;

    @Override
    public String lookupName(Long id) {
        if(id == NO_VALUE_FOR_ROW) {
            return null;
        }
        return String.valueOf(id);
    }

    @Override
    public Type getType() {
        return Type.LONG;
    }
}
