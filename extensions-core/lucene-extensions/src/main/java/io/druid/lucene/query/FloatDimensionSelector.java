package io.druid.lucene.query;

import java.text.DecimalFormat;

/**
 */
public abstract class FloatDimensionSelector implements DimensionSelector<Float> {
//    private static final DecimalFormat FORMAT = new DecimalFormat("#.#########");

    @Override
    public String lookupName(Float id) {
        return String.valueOf(id);
    }

    @Override
    public Float lookupId(String name) {
        return Float.parseFloat(name);
    }
}
