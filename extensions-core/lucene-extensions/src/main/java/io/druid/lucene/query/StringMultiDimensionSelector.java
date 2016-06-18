package io.druid.lucene.query;

import io.druid.segment.data.IndexedInts;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.search.DocIterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 */
public class StringMultiDimensionSelector implements DimensionSelector<Long> {
    private SortedSetDocValues docValues;

    public StringMultiDimensionSelector(SortedSetDocValues docValues) {
        this.docValues = docValues;
    }

    @Override
    public List<Long> getRow(int doc) {
        return null;
    }

    @Override
    public String lookupName(Long id) {
        return docValues.lookupOrd(id).toString();
    }

    @Override
    public Long lookupId(String name) {
        return docValues.lookupTerm(new BytesRef(name));
    }
}
