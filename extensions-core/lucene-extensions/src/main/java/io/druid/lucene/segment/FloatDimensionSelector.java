package io.druid.lucene.segment;

/**
 */
public abstract class FloatDimensionSelector implements DimensionSelector<Float> {
//    private static final DecimalFormat FORMAT = new DecimalFormat("#.#########");

    @Override
    public String lookupName(Float id) {
        return String.valueOf(id);
    }

    @Override
    public Type getType() {
        return Type.FLOAT;
    }
}
