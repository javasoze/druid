package io.druid.lucene.aggregation;

import com.metamx.common.parsers.ParseException;
import io.druid.lucene.segment.DimensionSelector;
import io.druid.query.aggregation.Aggregator;

import java.util.List;
import java.util.regex.Pattern;

/**
 */
public abstract class BaseLongAggregator implements Aggregator {
    private static final Pattern LONG_PAT = Pattern.compile("[-|+]?\\d+");
    private final DimensionSelector selector;

    public BaseLongAggregator(DimensionSelector selector) {
        this.selector = selector;
    }

    protected long[] getValue(long defaultVal) {
        List row = selector.getValues();
        long[] vals = new long[row.size()];
        for (int i=0;i<row.size();i++){
            Object val = row.get(i);
            if (val == null) {
                vals[i] = defaultVal;
            }

            if (val instanceof Number) {
                vals[i] = ((Number) val).longValue();
            } else if (val instanceof String) {
                try {
                    String s = ((String) val).replace(",", "");
                    vals[i] = LONG_PAT.matcher(s).matches() ? Long.valueOf(s) : Double.valueOf(s).longValue();
                }
                catch (Exception e) {
                    throw new ParseException(e, "Unable to parse value[%s]", val);
                }
            } else {
                throw new ParseException("Unknown type[%s]", val.getClass());
            }
        }
        return vals;
    }
}
