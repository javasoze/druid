package io.druid.lucene.segment.mapping;

import io.druid.common.utils.SerializerUtils;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.segment.indexing.DataSchema;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class FieldMappings {

    public static Builder builder() {
        return new Builder();
    }

    private FieldMappings() {}

    public static final String FILE_NAME = ".mapping";
    private static final SerializerUtils serializerUtils = new SerializerUtils();

    private Map<String, DimensionSchema.ValueType> fieldTypes;

    public Map<String, DimensionSchema.ValueType> getFieldTypes() {
        return  fieldTypes;
    }

    public void writeFieldTypesTo(File segmentDir) throws IOException {
        if (segmentDir == null || !segmentDir.exists()) {
            throw new IOException("WTF! Segment directory is not exist.");
        }
        File mappingFile = new File(segmentDir, FILE_NAME);
        try (FileChannel channel = new FileOutputStream(mappingFile).getChannel()) {
            serializerUtils.writeInt(channel, fieldTypes.size());
            for (Map.Entry<String, DimensionSchema.ValueType> entry : fieldTypes.entrySet()) {
                serializerUtils.writeString(channel, entry.getKey());
                serializerUtils.writeInt(channel, entry.getValue().ordinal());
            }
        }
    }

    public static class Builder {

        private Map<String, DimensionSchema.ValueType> fieldTypes;

        public Builder buildFieldTypesFrom(DataSchema schema) {
            DimensionsSpec dimensionsSpec = schema.getParser().getParseSpec().getDimensionsSpec();
            Set<String> excludedDimensions = dimensionsSpec.getDimensionExclusions();
            for (String dimensionName : dimensionsSpec.getDimensionNames()) {
                if (excludedDimensions != null && !excludedDimensions.isEmpty() && excludedDimensions.contains(dimensionName)) {
                    continue;
                }
                DimensionSchema dimensionSchema = dimensionsSpec.getSchema(dimensionName);
                this.fieldTypes.put(dimensionSchema.getName(), dimensionSchema.getValueType());
            }
            return this;
        }

        public Builder buildFieldTypesFrom(File segmentDir) throws IOException {
            if (segmentDir == null || !segmentDir.exists()) {
                throw new IOException("WTF! Segment directory is not exist"
                        + segmentDir == null? ".":", segmentDir: "+ segmentDir.getAbsolutePath());
            }
            File mappingFile = new File(segmentDir, FILE_NAME);
            try (InputStream inputStream = new FileInputStream(mappingFile)) {
                for (int i = 0; i < serializerUtils.readInt(inputStream); i++) {
                    String name = serializerUtils.readString(inputStream);
                    int order = serializerUtils.readInt(inputStream);
                    this.fieldTypes.put(name, DimensionSchema.ValueType.values()[order]);
                }
            }
            return this;
        }

        public FieldMappings build() {
            FieldMappings mappings = new FieldMappings();
            mappings.fieldTypes = Collections.unmodifiableMap(this.fieldTypes);
            return mappings;
        }

    }
}
