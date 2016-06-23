package io.druid.lucene.query.groupby;

import com.metamx.common.IAE;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.lucene.segment.DimensionSelector;
import io.druid.lucene.segment.FloatSingleDimensionSelector;
import io.druid.lucene.segment.LongSingleDimensionSelector;
import io.druid.lucene.segment.StringSingleDimensionSelector;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.solr.search.DocIterator;

import java.io.IOException;
import java.util.Map;

/**
 */
public class LuceneCursor implements LuceneColumnSelectorFactory {
    private final LeafReader leafReader;
    private final Map<String, DimensionSchema.ValueType> dimTypes;
    private DocIterator docIterator;
    private int curDoc = DocIdSetIterator.NO_MORE_DOCS;
    private int nextDoc = DocIdSetIterator.NO_MORE_DOCS;
    private boolean isDone;

    public LuceneCursor(LeafReader leafReader, Map<String, DimensionSchema.ValueType> dimTypes) {
        this.leafReader = leafReader;
        this.dimTypes = dimTypes;
        this.isDone = false;
    }

    public DimensionSelector<Long, Long> makeTimestampSelector() {
        try {
            DimensionSelector selector = new LongSingleDimensionSelector(this, leafReader.getNumericDocValues("_timestamp"));
            return selector;
        } catch (IOException e) {
            throw new IAE("");
        }
    }

    public DimensionSelector makeDimensionSelector(String dim)  {
        DimensionSchema.ValueType type = dimTypes.get(dim);
        DimensionSelector selector = null;
        if (null == type) {
            return selector;
        }
        try {
            switch (type) {
                case LONG:
                    selector = new LongSingleDimensionSelector(this, leafReader.getNumericDocValues(dim));
                    break;
                case FLOAT:
                    selector = new FloatSingleDimensionSelector(this, leafReader.getNumericDocValues(dim));
                    break;
                case STRING:
                    selector = new StringSingleDimensionSelector(this, leafReader.getSortedDocValues(dim));
                    break;
                default:
                    break;
            }
            return selector;
        } catch (IOException e) {
            throw new IAE("");
        }
    }

    public int getCurrentDoc() {
        return curDoc;
    }

    public void reset(DocIterator docIterator) {
        this.docIterator = docIterator;
        if (docIterator.hasNext()) {
            curDoc = docIterator.nextDoc();
        }
    }

    public void advance() {
        boolean hasNext = docIterator.hasNext();
        if (hasNext) {
            curDoc = docIterator.nextDoc();
        } else {
            curDoc = DocIdSetIterator.NO_MORE_DOCS;
        }
    }

    public int advanceTo(int offset) {
        return 0;
    }

    public boolean isDone() {
        return curDoc == DocIdSetIterator.NO_MORE_DOCS;
    }
}
