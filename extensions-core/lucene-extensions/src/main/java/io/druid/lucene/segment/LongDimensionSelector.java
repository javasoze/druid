package io.druid.lucene.segment;

import java.nio.ByteBuffer;

/**
 */
public abstract class LongDimensionSelector implements DimensionSelector<Long> {
    @Override
    public String lookupName(Long id) {
        return String.valueOf(id);
    }

    @Override
    public Type getType() {
        return Type.LONG;
    }
}
