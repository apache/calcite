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
import org.apache.calcite.util.Util;

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

      if (type.isStruct()) {
        type = type.getFieldList().get(0).getType();
      }

      assert type instanceof ArraySqlType || type instanceof MultisetSqlType
          || type instanceof MapSqlType;
      // If a type is nullable, all field accesses inside the type are also nullable
      if (type instanceof MapSqlType) {
        MapSqlType mapType = (MapSqlType) type;
        builder.add(MAP_KEY_COLUMN_NAME, mapType.getKeyType());
        builder.add(MAP_VALUE_COLUMN_NAME, mapType.getValueType());
      } else {
        RelDataType componentType = requireNonNull(type.getComponentType(), "componentType");
        boolean isNullable = componentType.isNullable();
        if (!allowAliasUnnestItems(opBinding) && componentType.isStruct()) {
          for (RelDataTypeField field : componentType.getFieldList()) {
            RelDataType fieldType = field.getType();
            if (isNullable) {
              fieldType = typeFactory.enforceTypeWithNullability(fieldType, true);
            }
            builder.add(field.getName(), fieldType);
          }
        } else {
          builder.add(SqlUtil.deriveAliasFromOrdinal(operand),
              componentType);
        }
      }
    }
    if (withOrdinality) {
      builder.add(ORDINALITY_COLUMN_NAME, SqlTypeName.INTEGER);
    }
    return builder.build();
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
