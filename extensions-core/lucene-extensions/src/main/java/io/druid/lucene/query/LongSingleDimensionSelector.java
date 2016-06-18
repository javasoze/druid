package io.druid.lucene.query;

import org.apache.lucene.index.NumericDocValues;
import org.apache.solr.search.DocIterator;

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.List;

/**
 */
public class LongSingleDimensionSelector extends LongDimensionSelector {
    private NumericDocValues docValues;

    public LongSingleDimensionSelector(NumericDocValues docValues) {
        this.docValues = docValues;
    }

    @Override
    public List<Long> getRow(int doc) {
        long value = docValues.get(doc);
        return Arrays.asList(value);
    }
}
