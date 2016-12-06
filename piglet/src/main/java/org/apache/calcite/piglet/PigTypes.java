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
package org.apache.calcite.piglet;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeName;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.newplan.logical.relational.LogicalSchema;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility methods for converting Pig data types to SQL types.
 */
class PigTypes {
  private PigTypes() {
  }

  private static final String PIG_TUPLE_WRAPPER = "PIG_WRAPPER";

  // Specialized type factory to handle type conversion
  static final PigRelDataTypeFactory TYPE_FACTORY =
      new PigRelDataTypeFactory(RelDataTypeSystem.DEFAULT);

  /**
   * Type factory that produces types with the nullability when converting
   * from Pig types. It also translates a Pig DataBag type into a multiset of
   * objects type.
   */
  static class PigRelDataTypeFactory extends JavaTypeFactoryImpl {
    private PigRelDataTypeFactory(RelDataTypeSystem typeSystem) {
      super(typeSystem);
    }

    public RelDataType createSqlType(SqlTypeName typeName, boolean nullable) {
      return createTypeWithNullability(super.createSqlType(typeName), nullable);
    }

    public RelDataType createStructType(List<RelDataType> typeList,
        List<String> fieldNameList, boolean nullable) {
      return createTypeWithNullability(
          super.createStructType(typeList, fieldNameList), nullable);
    }

    public RelDataType createMultisetType(RelDataType type,
        long maxCardinality, boolean nullable) {
      return createTypeWithNullability(
          super.createMultisetType(type, maxCardinality), nullable);
    }

    public RelDataType createMapType(RelDataType keyType,
        RelDataType valueType, boolean nullable) {
      return createTypeWithNullability(super.createMapType(keyType, valueType), nullable);
    }

    public RelDataType toSql(RelDataType type) {
      if (type instanceof JavaType
          && ((JavaType) type).getJavaClass() == DataBag.class) {
        // We don't know the structure of each tuple inside the bag until the runtime.
        // Thus just consider a bag as a multiset of unknown objects.
        return createMultisetType(createSqlType(SqlTypeName.ANY, true), -1, true);
      }
      return super.toSql(type);
    }
  }

  /**
   * Converts a Pig schema field to relational type.
   *
   * @param pigField Pig schema field
   * @return Relational type
   */
  static RelDataType convertSchemaField(LogicalSchema.LogicalFieldSchema pigField) {
    return convertSchemaField(pigField, true);
  }

  /**
   * Converts a Pig schema field to relational type.
   *
   * @param pigField Pig schema field
   * @param nullable true if the type is nullable
   * @return Relational type
   */
  static RelDataType convertSchemaField(LogicalSchema.LogicalFieldSchema pigField,
      boolean nullable) {
    switch (pigField.type) {
    case DataType.UNKNOWN:
      return TYPE_FACTORY.createSqlType(SqlTypeName.ANY, nullable);
    case DataType.NULL:
      return TYPE_FACTORY.createSqlType(SqlTypeName.NULL, nullable);
    case DataType.BOOLEAN:
      return TYPE_FACTORY.createSqlType(SqlTypeName.BOOLEAN, nullable);
    case DataType.BYTE:
      return TYPE_FACTORY.createSqlType(SqlTypeName.TINYINT, nullable);
    case DataType.INTEGER:
      return TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER, nullable);
    case DataType.LONG:
      return TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT, nullable);
    case DataType.FLOAT:
      return TYPE_FACTORY.createSqlType(SqlTypeName.FLOAT, nullable);
    case DataType.DOUBLE:
      return TYPE_FACTORY.createSqlType(SqlTypeName.DOUBLE, nullable);
    case DataType.DATETIME:
      return TYPE_FACTORY.createSqlType(SqlTypeName.DATE, nullable);
    case DataType.BYTEARRAY:
      return TYPE_FACTORY.createSqlType(SqlTypeName.BINARY, nullable);
    case DataType.CHARARRAY:
      return TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR, nullable);
    case DataType.BIGINTEGER:
    case DataType.BIGDECIMAL:
      return TYPE_FACTORY.createSqlType(SqlTypeName.DECIMAL, nullable);
    case DataType.TUPLE: {
      if (pigField.alias != null && pigField.alias.equals(PIG_TUPLE_WRAPPER)) {
        if (pigField.schema == null || pigField.schema.size() != 1) {
          throw new IllegalArgumentException("Expect one subfield from " + pigField.schema);
        }
        return convertSchemaField(pigField.schema.getField(0), nullable);
      }
      return convertSchema(pigField.schema, nullable);
    }
    case DataType.MAP: {
      final RelDataType relKey = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR);
      if (pigField.schema == null) {
        // The default type of Pig Map value is bytearray
        return TYPE_FACTORY.createMapType(relKey,
            TYPE_FACTORY.createSqlType(SqlTypeName.BINARY), nullable);
      } else {
        assert pigField.schema.size() == 1;
        return TYPE_FACTORY.createMapType(relKey,
            convertSchemaField(pigField.schema.getField(0), nullable), nullable);
      }
    }
    case DataType.BAG: {
      if (pigField.schema == null) {
        return TYPE_FACTORY.createMultisetType(TYPE_FACTORY.createSqlType(SqlTypeName.ANY, true),
            -1, true);
      }
      assert pigField.schema.size() == 1;
      return TYPE_FACTORY.createMultisetType(
          convertSchemaField(pigField.schema.getField(0), nullable), -1, nullable);
    }
    default:
      throw new IllegalArgumentException(
          "Unsupported conversion for Pig Data type: "
              + DataType.findTypeName(pigField.type));
    }
  }

  /**
   * Converts a Pig tuple schema to a SQL row type.
   *
   * @param pigSchema Pig tuple schema
   * @return a SQL row type
   */
  static RelDataType convertSchema(LogicalSchema pigSchema) {
    return convertSchema(pigSchema, true);
  }

  /**
   * Converts a Pig tuple schema to a SQL row type.
   *
   * @param pigSchema Pig tuple schema
   * @param nullable true if the type is nullable
   * @return a SQL row type
   */
  static RelDataType convertSchema(LogicalSchema pigSchema, boolean nullable) {
    if (pigSchema != null && pigSchema.size() > 0) {
      List<String> fieldNameList = new ArrayList<>();
      List<RelDataType> typeList = new ArrayList<>();
      for (int i = 0; i < pigSchema.size(); i++) {
        final LogicalSchema.LogicalFieldSchema subPigField = pigSchema.getField(i);
        fieldNameList.add(subPigField.alias != null ? subPigField.alias : "$" + i);
        typeList.add(convertSchemaField(subPigField, nullable));
      }
      return TYPE_FACTORY.createStructType(typeList, fieldNameList, nullable);
    }
    return new DynamicTupleRecordType(TYPE_FACTORY);
  }
}

// End PigTypes.java
