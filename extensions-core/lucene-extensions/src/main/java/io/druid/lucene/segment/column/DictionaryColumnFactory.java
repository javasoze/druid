package io.druid.lucene.segment.column;

import io.druid.data.input.impl.DimensionSchema;
import org.apache.lucene.index.LeafReader;

import java.io.IOException;

/**
 */
public class DictionaryColumnFactory {
    public DictionaryColumn createColumn(LeafReader leafReader, DimensionSchema dimension) throws IOException{
        switch (dimension.getValueType()) {
            case LONG:
                return new LongDictionaryColumn(leafReader.getNumericDocValues(dimension.getName()));
            case FLOAT:
                return new FloatDictionaryColumn(leafReader.getNumericDocValues(dimension.getName()));
            case STRING:
                return new StringDictionaryColumn(leafReader.getSortedDocValues(dimension.getName()));
        }

        return null;
    }
}