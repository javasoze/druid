package io.druid.lucene.query;

import java.text.DecimalFormat;
import java.util.List;

/**
 */
public abstract class LongDimensionSelector implements DimensionSelector<Long> {
    @Override
    public String lookupName(Long id) {
        return String.valueOf(id);
    }

    @Override
    public Long lookupId(String name) {
        return Long.parseLong(name);
    }
}
