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
package org.apache.calcite.sql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.MapSqlType;
import org.apache.calcite.sql.type.MultisetSqlType;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.util.Util;

import org.apiguardian.api.API;

import static java.util.Objects.requireNonNull;

/**
 * The <code>UNNEST</code> operator.
 */
public class SqlUnnestOperator extends SqlFunctionalOperator {
  /** Whether {@code WITH ORDINALITY} was specified.
   *
   * <p>If so, the returned records include a column {@code ORDINALITY}. */
  public final boolean withOrdinality;

  public static final String ORDINALITY_COLUMN_NAME = "ORDINALITY";

  public static final String MAP_KEY_COLUMN_NAME = "KEY";

  public static final String MAP_VALUE_COLUMN_NAME = "VALUE";

  //~ Constructors -----------------------------------------------------------

  public SqlUnnestOperator(boolean withOrdinality) {
    super(
        "UNNEST",
        SqlKind.UNNEST,
        200,
        true,
        null,
        null,
        OperandTypes.repeat(SqlOperandCountRanges.from(1),
            OperandTypes.SCALAR_OR_RECORD_COLLECTION_OR_MAP));
    this.withOrdinality = withOrdinality;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
    final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
    final RelDataTypeFactory.Builder builder = typeFactory.builder();
    for (Integer operand : Util.range(opBinding.getOperandCount())) {
      RelDataType type = opBinding.getOperandType(operand);
      if (type.getSqlTypeName() == SqlTypeName.ANY) {
        // Unnest Operator in schema less systems returns one column as the output
        // $unnest is a placeholder to specify that one column with type ANY is output.
        return builder
            .add("$unnest",
                SqlTypeName.ANY)
            .nullable(true)
            .build();
      }

      type = unwrapOperandType(type);

      assert type instanceof ArraySqlType || type instanceof MultisetSqlType
          || type instanceof MapSqlType;
      // If a type is nullable, all field accesses inside the type are also nullable
      // With multiple collections, zip semantics pad shorter collections with
      // NULL, so all output columns from a multi-collection UNNEST are nullable.
      final boolean padNullable = opBinding.getOperandCount() > 1;
      if (type instanceof MapSqlType) {
        MapSqlType mapType = (MapSqlType) type;
        RelDataType keyType = padNullable
            ? typeFactory.enforceTypeWithNullability(mapType.getKeyType(), true)
            : mapType.getKeyType();
        RelDataType valueType = padNullable
            ? typeFactory.enforceTypeWithNullability(mapType.getValueType(), true)
            : mapType.getValueType();
        builder.add(MAP_KEY_COLUMN_NAME, keyType);
        builder.add(MAP_VALUE_COLUMN_NAME, valueType);
      } else {
        RelDataType componentType = requireNonNull(type.getComponentType(), "componentType");
        boolean isNullable = componentType.isNullable() || padNullable;
        if (expandsStructIntoColumns(componentType, allowAliasUnnestItems(opBinding))) {
          for (RelDataTypeField field : componentType.getFieldList()) {
            RelDataType fieldType = field.getType();
            if (isNullable) {
              fieldType = typeFactory.enforceTypeWithNullability(fieldType, true);
            }
            builder.add(field.getName(), fieldType);
          }
        } else {
          RelDataType elementType = componentType.isStruct()
              ? typeFactory.builder().kind(componentType.getStructKind())
                  .addAll(componentType.getFieldList()).build()
              : componentType;
          // A NULL collection element becomes a NULL value in this column, so
          // the column is nullable whenever the element type is.
          RelDataType colType = isNullable
              ? typeFactory.enforceTypeWithNullability(elementType, true)
              : elementType;
          builder.add(SqlUtil.deriveAliasFromOrdinal(operand), colType);
        }
      }
    }
    if (withOrdinality) {
      builder.add(ORDINALITY_COLUMN_NAME, SqlTypeName.INTEGER);
    }
    return builder.build();
  }

  /**
   * Returns the collection type that {@code operandType} denotes.
   *
   * <p>For a sub-query operand, {@code operandType} is a single-field
   * struct; this returns the type of that field.
   */
  @API(since = "1.43", status = API.Status.INTERNAL)
  public static RelDataType unwrapOperandType(RelDataType operandType) {
    return operandType.isStruct() ? operandType.getFieldList().get(0).getType() : operandType;
  }

  /**
   * Returns whether UNNEST expands an element of type {@code componentType}
   * into one column per field.
   *
   * @param componentType element type of the collection
   * @param allowAliasUnnestItems value of
   *     {@link SqlConformance#allowAliasUnnestItems()}; when true, a ROW
   *     element becomes a single column of the ROW type
   */
  @API(since = "1.43", status = API.Status.INTERNAL)
  public static boolean expandsStructIntoColumns(RelDataType componentType,
      boolean allowAliasUnnestItems) {
    return componentType.isStruct() && !allowAliasUnnestItems;
  }

  private static boolean allowAliasUnnestItems(SqlOperatorBinding operatorBinding) {
    return (operatorBinding instanceof SqlCallBinding)
        && ((SqlCallBinding) operatorBinding)
        .getValidator()
        .config()
        .conformance()
        .allowAliasUnnestItems();
  }

  @Override public void unparse(SqlWriter writer, SqlCall call, int leftPrec,
      int rightPrec) {
    super.unparse(writer, call, leftPrec, rightPrec);
    if (withOrdinality) {
      writer.keyword("WITH ORDINALITY");
    }
  }

  @Override public boolean argumentMustBeScalar(int ordinal) {
    return false;
  }

}
