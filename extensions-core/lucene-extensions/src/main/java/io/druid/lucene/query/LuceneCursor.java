package io.druid.lucene.query;

import com.metamx.common.IAE;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.lucene.query.groupby.LuceneColumnSelectorFactory;
import io.druid.lucene.segment.DimensionSelector;
import io.druid.lucene.segment.FloatSingleDimensionSelector;
import io.druid.lucene.segment.LongSingleDimensionSelector;
import io.druid.lucene.segment.StringSingleDimensionSelector;
import io.druid.segment.column.Column;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 */
public class LuceneCursor implements LuceneColumnSelectorFactory {
    private final LeafReader leafReader;
    private final Map<String, DimensionSchema.ValueType> dimTypes;
    private final List<Integer> docList;
    private final int maxDocOffset;
    private int curDocOffset = -1;
    private boolean isDone;

    public LuceneCursor(Query query, LeafReader leafReader, Map<String, DimensionSchema.ValueType> dimTypes) {
        IndexSearcher searcher = new IndexSearcher(leafReader);
        DocListCollector collector = new DocListCollector();
        try {
            searcher.search(query, collector);
        } catch (IOException e) {
            throw new IAE("");
        }

        docList = collector.getDocList();
        maxDocOffset = docList.size() - 1;
        if (docList.isEmpty()) {
            isDone = true;
        } else {
            isDone = false;
            curDocOffset = 0;
        }
        this.leafReader = leafReader;
        this.dimTypes = dimTypes;
    }

    public DimensionSelector<Long, Long> makeTimestampSelector() {
        try {
            DimensionSelector selector = new LongSingleDimensionSelector(this, leafReader.getNumericDocValues(Column.TIME_COLUMN_NAME));
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
        return docList.get(curDocOffset);
    }

    public void advance() {
        if (maxDocOffset > curDocOffset ) {
            curDocOffset++;
        } else {
            isDone = true;
        }
    }

    public int advanceTo(int offset) {
        int resOffset = curDocOffset + offset;
        int left = resOffset - maxDocOffset;
        if (0 >= left) {
            curDocOffset = resOffset;
            return 0;
        } else {
            curDocOffset = maxDocOffset;
            isDone = true;
            return left;
        }
    }

    public boolean isDone() {
        return isDone;
    }
}
