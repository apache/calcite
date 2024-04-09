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

import org.apache.arrow.adapter.jdbc.ArrowVectorIterator;
import org.apache.arrow.adapter.jdbc.JdbcToArrow;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfig;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfigBuilder;
import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FloatingPointVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import com.google.common.collect.ImmutableList;

import net.hydromatic.scott.data.hsqldb.ScottHsqldb;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.List;

/**
 * Class that can be used to generate Arrow sample data into a data directory.
 */
public class ArrowData {

  private final int batchSize;
  private final int entries;
  private int intValue;
  private int stringValue;
  private float floatValue;
  private long longValue;

  public ArrowData() {
    this.batchSize = 20;
    this.entries = 50;
    this.intValue = 0;
    this.stringValue = 0;
    this.floatValue = 0;
    this.longValue = 0;
  }

  private Schema makeArrowSchema() {
    ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
    FieldType intType = FieldType.nullable(new ArrowType.Int(32, true));
    FieldType stringType = FieldType.nullable(new ArrowType.Utf8());
    FieldType floatType =
        FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
    FieldType longType = FieldType.nullable(new ArrowType.Int(64, true));

    childrenBuilder.add(new Field("intField", intType, null));
    childrenBuilder.add(new Field("stringField", stringType, null));
    childrenBuilder.add(new Field("floatField", floatType, null));
    childrenBuilder.add(new Field("longField", longType, null));

    return new Schema(childrenBuilder.build(), null);
  }

  public void writeScottEmpData(Path arrowDataDirectory) throws IOException, SQLException {
    List<String> tableNames = ImmutableList.of("EMP", "DEPT", "SALGRADE");

    Connection connection =
        DriverManager.getConnection(ScottHsqldb.URI, ScottHsqldb.USER, ScottHsqldb.PASSWORD);

    for (String tableName : tableNames) {
      String sql = "SELECT * FROM " + tableName;
      Statement statement = connection.createStatement();
      ResultSet resultSet = statement.executeQuery(sql);

      Calendar calendar = JdbcToArrowUtils.getUtcCalendar();

      RootAllocator rootAllocator = new RootAllocator();
      JdbcToArrowConfig config = new JdbcToArrowConfigBuilder()
          .setAllocator(rootAllocator)
          .setReuseVectorSchemaRoot(true)
          .setCalendar(calendar)
          .setTargetBatchSize(1024)
          .build();

      ArrowVectorIterator vectorIterator = JdbcToArrow.sqlToArrowVectorIterator(resultSet, config);
      Path tablePath = arrowDataDirectory.resolve(tableName + ".arrow");

      FileOutputStream fileOutputStream = new FileOutputStream(tablePath.toFile());

      VectorSchemaRoot vectorSchemaRoot = vectorIterator.next();

      ArrowFileWriter arrowFileWriter =
          new ArrowFileWriter(vectorSchemaRoot, null, fileOutputStream.getChannel());

      arrowFileWriter.start();
      arrowFileWriter.writeBatch();

      while (vectorIterator.hasNext()) {
        // refreshes the data in the VectorSchemaRoot with the next batch
        vectorIterator.next();
        arrowFileWriter.writeBatch();
      }

      arrowFileWriter.close();
    }
  }

  public void writeArrowData(File file) throws IOException {
    FileOutputStream fileOutputStream = new FileOutputStream(file);
    Schema arrowSchema = makeArrowSchema();
    VectorSchemaRoot vectorSchemaRoot =
        VectorSchemaRoot.create(arrowSchema, new RootAllocator(Integer.MAX_VALUE));
    ArrowFileWriter arrowFileWriter =
        new ArrowFileWriter(vectorSchemaRoot, null, fileOutputStream.getChannel());

    arrowFileWriter.start();

    for (int i = 0; i < this.entries;) {
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
        case VARCHAR:
          varCharField(vector, numRows);
          break;
        case BIGINT:
          longField(vector, numRows);
          break;
        default:
          throw new IllegalStateException("Not supported type yet: " + vector.getMinorType());
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

  private void intField(FieldVector fieldVector, int rowCount) {
    IntVector intVector = (IntVector) fieldVector;
    intVector.setInitialCapacity(rowCount);
    intVector.allocateNew();
    for (int i = 0; i < rowCount; i++) {
      intVector.set(i, 1, intValue);
      this.intValue++;
    }
    fieldVector.setValueCount(rowCount);
  }

  private void floatField(FieldVector fieldVector, int rowCount) {
    FloatingPointVector floatingPointVector = (FloatingPointVector) fieldVector;
    floatingPointVector.setInitialCapacity(rowCount);
    floatingPointVector.allocateNew();
    for (int i = 0; i < rowCount; i++) {
      float value = this.floatValue;
      floatingPointVector.setWithPossibleTruncate(i, value);
      this.floatValue++;
    }
    fieldVector.setValueCount(rowCount);
  }

  private void varCharField(FieldVector fieldVector, int rowCount) {
    VarCharVector varCharVector = (VarCharVector) fieldVector;
    varCharVector.setInitialCapacity(rowCount);
    varCharVector.allocateNew();
    for (int i = 0; i < rowCount; i++) {
      String value = String.valueOf(this.stringValue);
      varCharVector.set(i, new Text(value));
      this.stringValue++;
    }
    fieldVector.setValueCount(rowCount);
  }

  private void longField(FieldVector fieldVector, int rowCount) {
    BigIntVector longVector = (BigIntVector) fieldVector;
    longVector.setInitialCapacity(rowCount);
    longVector.allocateNew();
    for (int i = 0; i < rowCount; i++) {
      longVector.set(i, this.longValue);
      this.longValue++;
    }
    fieldVector.setValueCount(rowCount);
  }
}
