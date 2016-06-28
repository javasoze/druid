package io.druid.lucene.segment;

/**
 */
public abstract class FloatDimensionSelector implements DimensionSelector<Float, Float> {
    protected float NO_VALUE_FOR_ROW = Float.MAX_VALUE;
//    private static final DecimalFormat FORMAT = new DecimalFormat("#.#########");

    @Override
    public String lookupName(Float id) {
        if(id == NO_VALUE_FOR_ROW) {
            return null;
        }
        return String.valueOf(id);
    }

    @Override
    public Type getType() {
        return Type.FLOAT;
    }
}
