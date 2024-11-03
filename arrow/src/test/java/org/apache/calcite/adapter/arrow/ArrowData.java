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
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.FloatingPointVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.DateUnit;
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
import java.math.BigDecimal;
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
  private byte  tinyIntValue;
  private short smallIntValue;
  private int intValue;
  private int stringValue;
  private float floatValue;
  private long longValue;
  private double doubleValue;
  private boolean booleanValue;
  private BigDecimal decimalValue;

  public ArrowData() {
    this.batchSize = 20;
    this.entries = 50;
    this.tinyIntValue = 0;
    this.smallIntValue = 0;
    this.intValue = 0;
    this.stringValue = 0;
    this.floatValue = 0;
    this.longValue = 0;
    this.doubleValue = 0;
    this.booleanValue = false;
    this.decimalValue = BigDecimal.ZERO;
  }

  private Schema makeArrowDateTypeSchema() {
    ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
    FieldType tinyIntType = FieldType.nullable(new ArrowType.Int(8, true));
    FieldType smallIntType = FieldType.nullable(new ArrowType.Int(16, true));
    FieldType intType = FieldType.nullable(new ArrowType.Int(32, true));
    FieldType stringType = FieldType.nullable(new ArrowType.Utf8());
    FieldType floatType =
        FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
    FieldType longType = FieldType.nullable(new ArrowType.Int(64, true));
    FieldType doubleType =
        FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
    FieldType booleanType = FieldType.nullable(new ArrowType.Bool());
    FieldType decimalType = FieldType.nullable(new ArrowType.Decimal(10, 2, 128));
    FieldType dateType = FieldType.nullable(new ArrowType.Date(DateUnit.DAY));

    childrenBuilder.add(new Field("tinyIntField", tinyIntType, null));
    childrenBuilder.add(new Field("smallIntField", smallIntType, null));
    childrenBuilder.add(new Field("intField", intType, null));
    childrenBuilder.add(new Field("stringField", stringType, null));
    childrenBuilder.add(new Field("floatField", floatType, null));
    childrenBuilder.add(new Field("longField", longType, null));
    childrenBuilder.add(new Field("doubleField", doubleType, null));
    childrenBuilder.add(new Field("booleanField", booleanType, null));
    childrenBuilder.add(new Field("decimalField", decimalType, null));
    childrenBuilder.add(new Field("dateField", dateType, null));

    return new Schema(childrenBuilder.build(), null);
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

  public void writeArrowDataType(File file) throws IOException {
    FileOutputStream fileOutputStream = new FileOutputStream(file);
    Schema arrowSchema = makeArrowDateTypeSchema();
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
        case TINYINT:
          tinyIntField(vector, numRows);
          break;
        case SMALLINT:
          smallIntFiled(vector, numRows);
          break;
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
        case FLOAT8:
          doubleField(vector, numRows);
          break;
        case BIT:
          booleanField(vector, numRows);
          break;
        case DECIMAL:
          decimalField(vector, numRows);
          break;
        case DATEDAY:
          dateField(vector, numRows);
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

  private void tinyIntField(FieldVector fieldVector, int rowCount) {
    TinyIntVector tinyIntVector = (TinyIntVector) fieldVector;
    tinyIntVector.setInitialCapacity(rowCount);
    tinyIntVector.allocateNew();
    for (int i = 0; i < rowCount; i++) {
      tinyIntVector.set(i, this.tinyIntValue);
      this.tinyIntValue++;
    }
    fieldVector.setValueCount(rowCount);
  }

  private void smallIntFiled(FieldVector fieldVector, int rowCount) {
    SmallIntVector smallIntVector = (SmallIntVector) fieldVector;
    smallIntVector.setInitialCapacity(rowCount);
    smallIntVector.allocateNew();
    for (int i = 0; i < rowCount; i++) {
      smallIntVector.set(i, this.smallIntValue);
      this.smallIntValue++;
    }
    fieldVector.setValueCount(rowCount);
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

  private void doubleField(FieldVector fieldVector, int rowCount) {
    Float8Vector float8Vector = (Float8Vector) fieldVector;
    float8Vector.setInitialCapacity(rowCount);
    float8Vector.allocateNew();
    for (int i = 0; i < rowCount; i++) {
      float8Vector.set(i, this.doubleValue);
      this.doubleValue++;
    }
    fieldVector.setValueCount(rowCount);
  }

  private void booleanField(FieldVector fieldVector, int rowCount) {
    BitVector bitVector = (BitVector) fieldVector;
    bitVector.setInitialCapacity(rowCount);
    bitVector.allocateNew();
    for (int i = 0; i < rowCount; i++) {
      if (i % 3 == 0) {
        bitVector.setNull(i);
      } else {
        bitVector.set(i, this.booleanValue ? 1 : 0);
      }
      this.booleanValue = !this.booleanValue;
    }
    fieldVector.setValueCount(rowCount);
  }

  private void decimalField(FieldVector fieldVector, int rowCount) {
    DecimalVector decimalVector = (DecimalVector) fieldVector;
    decimalVector.setInitialCapacity(rowCount);
    decimalVector.allocateNew();
    for (int i = 0; i < rowCount; i++) {
      decimalVector.set(i, this.decimalValue.setScale(2));
      this.decimalValue = this.decimalValue.add(BigDecimal.ONE);
    }
    fieldVector.setValueCount(rowCount);
  }

  private void dateField(FieldVector fieldVector, int rowCount) {
    DateDayVector dateDayVector = (DateDayVector) fieldVector;
    dateDayVector.setInitialCapacity(rowCount);
    dateDayVector.allocateNew();
    for (int i = 0; i < rowCount; i++) {
      dateDayVector.set(i, i);
    }
    fieldVector.setValueCount(rowCount);
  }
}
