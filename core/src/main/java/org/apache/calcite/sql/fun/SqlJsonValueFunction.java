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
package org.apache.calcite.sql.fun;

import org.apache.calcite.linq4j.Nullness;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlJsonValueReturning;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * The <code>JSON_VALUE</code> function.
 */
public class SqlJsonValueFunction extends SqlFunction {

  public SqlJsonValueFunction(String name) {
    super(name, SqlKind.OTHER_FUNCTION,
        opBinding -> explicitTypeSpec(opBinding)
            .map(relDataType -> deriveExplicitType(opBinding, relDataType))
            .orElseGet(() -> getDefaultType(opBinding)),
        null,
        OperandTypes.family(
            ImmutableList.of(SqlTypeFamily.ANY, SqlTypeFamily.CHARACTER),
            ordinal -> ordinal > 1),
        SqlFunctionCategory.SYSTEM);
  }

  private static RelDataType deriveExplicitType(SqlOperatorBinding opBinding, RelDataType type) {
    if (SqlTypeName.ARRAY == type.getSqlTypeName()) {
      RelDataType elementType = requireNonNull(type.getComponentType());
      RelDataType nullableElementType = deriveExplicitType(opBinding, elementType);
      return SqlTypeUtil.createArrayType(
          opBinding.getTypeFactory(),
          nullableElementType,
          true);
    }
    return opBinding.getTypeFactory().createTypeWithNullability(type, true);
  }

  /** Returns VARCHAR(2000) as default. */
  private static RelDataType getDefaultType(SqlOperatorBinding opBinding) {
    final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
    final RelDataType baseType = typeFactory.createSqlType(SqlTypeName.VARCHAR, 2000);
    return typeFactory.createTypeWithNullability(baseType, true);
  }

  /**
   * Returns new operand list with type specification removed.
   */
  public static List<SqlNode> removeTypeSpecOperands(SqlCall call) {
    List<@Nullable SqlNode> operands = new ArrayList<>(call.getOperandList());
    if (hasExplicitTypeSpec(call.getOperandList())) {
      operands.set(2, null);
      operands.set(3, null);
    }
    return operands.stream()
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  @Override public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.between(2, 10);
  }

  /** Returns the optional explicit returning type specification. */
  private static Optional<RelDataType> explicitTypeSpec(SqlOperatorBinding opBinding) {
    if (opBinding.getOperandCount() > 2
        && opBinding.isOperandLiteral(2, false)
        && opBinding.getOperandLiteralValue(2, Object.class)
          instanceof SqlJsonValueReturning) {
      return Optional.of(opBinding.getOperandType(3));
    }
    return Optional.empty();
  }

  /** Returns whether there is an explicit return type specification. */
  public static boolean hasExplicitTypeSpec(List<SqlNode> operands) {
    return operands.size() > 2
        && isReturningTypeSymbol(operands.get(2));
  }

  @Deprecated // to be removed before 2.0
  public static boolean hasExplicitTypeSpec(@Nullable SqlNode[] operands) {
    return hasExplicitTypeSpec(
        Arrays.asList(Nullness.castNonNullArray(operands)));
  }

  private static boolean isReturningTypeSymbol(@Nullable SqlNode node) {
    return node instanceof SqlLiteral
        && ((SqlLiteral) node).getValue() instanceof SqlJsonValueReturning;
  }

  @Override public String getAllowedSignatures(String opNameToUse) {
    return "JSON_VALUE(json_doc, path [RETURNING type] "
        + "[{NULL | ERROR | DEFAULT value} ON EMPTY] "
        + "[{NULL | ERROR | DEFAULT value} ON ERROR])";
  }

  @Override public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    final SqlWriter.Frame frame = writer.startFunCall(getName());
    call.operand(0).unparse(writer, leftPrec, rightPrec);
    writer.sep(",", true);
    for (int i = 1; i < call.operandCount(); i++) {
      call.operand(i).unparse(writer, leftPrec, rightPrec);
    }
    writer.endFunCall(frame);
  }
}
