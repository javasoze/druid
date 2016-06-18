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
import io.druid.data.input.impl.DimensionSchema;
import io.druid.data.input.impl.DimensionSchema.ValueType;
import io.druid.data.input.impl.DimensionsSpec;

import java.util.Set;

import org.apache.lucene.document.*;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.util.BytesRef;

public class LuceneDocumentBuilder
{
  private final DimensionsSpec dimensionsSpec;
  
  public LuceneDocumentBuilder(DimensionsSpec dimensionsSpec) {
    this.dimensionsSpec = dimensionsSpec;
  }
  
  public Document buildLuceneDocument(InputRow row) {
    Set<String> excludedDimensions = dimensionsSpec.getDimensionExclusions();
    Document doc = new Document();
    for (String dimensionName : dimensionsSpec.getDimensionNames()) {
      if (excludedDimensions != null && !excludedDimensions.isEmpty() && excludedDimensions.contains(dimensionName)) {
        continue;
      }
      DimensionSchema schema = dimensionsSpec.getSchema(dimensionName);
      Object value = row.getRaw(dimensionName);
      if (ValueType.STRING.equals(schema.getValueType())) {
        doc.add(new TextField(dimensionName, String.valueOf(value), Store.YES));
        doc.add(new SortedDocValuesField(dimensionName, new BytesRef(String.valueOf(value))));
      } else if (ValueType.FLOAT.equals(schema.getValueType())) {
        doc.add(new DoubleField(dimensionName, (Double)value, Store.YES));
        doc.add(new DoubleDocValuesField(dimensionName, (Double)value));
      } else if (ValueType.LONG.equals(schema.getValueType())) {
        doc.add(new LongField(dimensionName, (Long)value, Store.YES));
        doc.add(new NumericDocValuesField(dimensionName, (Long)value));
      }
    }
    return doc;
  }
}
