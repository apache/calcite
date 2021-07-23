/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.adapter.arrow;

import com.google.common.collect.ImmutableList;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.io.File;
import java.io.FileOutputStream;

public class ArrowData {

  private FileOutputStream fileOutputStream;
  private int batchSize;
  private VectorSchemaRoot vectorSchemaRoot;
  private ArrowFileWriter arrowFileWriter;
  private int entries;

  public ArrowData() {

    this.batchSize = 100;
    this.entries = 10;
  }

  private Schema makeArrowSchema(){
    ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
    childrenBuilder.add(new Field("int", FieldType.nullable(new ArrowType.Int(32, true)), null));
    childrenBuilder.add(new Field("float", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null));
    childrenBuilder.add(new Field("varchar", FieldType.nullable(new ArrowType.Utf8()), null));
    return new Schema(childrenBuilder.build(), null);
  }

  public void writeArrowData() throws Exception {
    File file = new File("");
    this.fileOutputStream = new FileOutputStream(file);
    Schema arrowSchema = makeArrowSchema();
    this.vectorSchemaRoot = VectorSchemaRoot.create(arrowSchema, new RootAllocator(Integer.MAX_VALUE));
    DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
    this.arrowFileWriter = new ArrowFileWriter(vectorSchemaRoot, provider,
        this.fileOutputStream.getChannel());

    this.batchSize = 100;
    this.arrowFileWriter.start();

    for (int i = 0; i < this.entries; ) {
      int numRows = Math.min(this.batchSize, this.entries - i);
      vectorSchemaRoot.setRowCount(numRows);
      for (Field field : vectorSchemaRoot.getSchema().getFields()) {
        FieldVector vector = vectorSchemaRoot.getVector(field.getName());
        switch (vector.getMinorType()) {
        case INT:
          intField(vector, numRows);
          break;
        case FLOAT4:
          floatField(vector, numRows);
          break;
        case VARBINARY:
          varCharField(vector, numRows);
          break;
        default:
          throw new Exception("Not supported yet type: " + vector.getMinorType());
        }
      }
      arrowFileWriter.writeBatch();
      i += numRows;
    }
    arrowFileWriter.end();
    arrowFileWriter.close();
    fileOutputStream.flush();
    fileOutputStream.close();
  }

  private void intField(FieldVector fieldVector, int rowCount){
    IntVector intVector = (IntVector) fieldVector;
    intVector.setInitialCapacity(rowCount);
    intVector.allocateNew();
    for(int i = 0; i < rowCount; i++){
      intVector.set(i, 1, i);
    }
    fieldVector.setValueCount(rowCount);
  }

  private void floatField(FieldVector fieldVector, int rowCount){
    FloatingPointVector floatingPointVector = (FloatingPointVector) fieldVector;
    floatingPointVector.setInitialCapacity(rowCount);
    floatingPointVector.allocateNew();
    for(int i = 0; i < rowCount; i++){
      float floatValue = i;
      floatingPointVector.setWithPossibleTruncate(i, floatValue);
    }
    fieldVector.setValueCount(rowCount);
  }

  private void varCharField(FieldVector fieldVector, int rowCount){
    VarCharVector varCharVector = (VarCharVector) fieldVector;
    varCharVector.setInitialCapacity(rowCount);
    varCharVector.allocateNew();
    for(int i = 0; i < rowCount; i++){
      varCharVector.set(i, new Text("abc"));
    }
    fieldVector.setValueCount(rowCount);
  }

  public static void main(String[] args) {
    ArrowData arrowData = new ArrowData();
    try {
      arrowData.writeArrowData();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
