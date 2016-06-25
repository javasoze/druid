/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.druid.lucene.segment.realtime;

import io.druid.data.input.InputRow;
import io.druid.data.input.impl.DimensionSchema.ValueType;
import io.druid.lucene.segment.mapping.FieldMappings;
import io.druid.segment.column.Column;
import org.apache.lucene.document.*;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.util.BytesRef;

import java.util.Map;

public class LuceneDocumentBuilder
{
  private final FieldMappings fieldMappings;
  
  public LuceneDocumentBuilder(FieldMappings fieldMappings) {
    this.fieldMappings = fieldMappings;
  }
  
  public Document buildLuceneDocument(InputRow row) {
    Document doc = new Document();
    long timestamp = row.getTimestampFromEpoch();
    doc.add(new LongField(Column.TIME_COLUMN_NAME, timestamp, Store.YES));
    doc.add(new NumericDocValuesField(Column.TIME_COLUMN_NAME, timestamp));
    for (Map.Entry<String, ValueType> entry : fieldMappings.getFieldTypes().entrySet()) {
      Object value = row.getRaw(entry.getKey());
      if (value == null) {
        continue;
      }
      if (ValueType.STRING.equals(entry.getValue())) {
        doc.add(new TextField(entry.getKey(), String.valueOf(value), Store.YES));
        doc.add(new SortedDocValuesField(entry.getKey(), new BytesRef(String.valueOf(value))));
      } else if (ValueType.FLOAT.equals(entry.getValue())) {
        doc.add(new DoubleField(entry.getKey(), (Double)value, Store.YES));
        doc.add(new DoubleDocValuesField(entry.getKey(), (Double)value));
      } else if (ValueType.LONG.equals(entry.getValue())) {
        doc.add(new LongField(entry.getKey(), (Long)value, Store.YES));
        doc.add(new NumericDocValuesField(entry.getKey(), (Long)value));
      }
    }
    return doc;
  }
}
