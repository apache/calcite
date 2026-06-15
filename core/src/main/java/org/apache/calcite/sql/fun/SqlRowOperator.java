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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * SqlRowOperator represents the special ROW constructor, {@code ROW(v1, v2, ...)}.
 *
 * <p>Fields may be given explicit names using AS aliases:
 * {@code ROW(v1 AS f1, v2 AS f2, ...)}. When aliases are present, a
 * per-instance operator (rather than the singleton {@link
 * org.apache.calcite.sql.fun.SqlStdOperatorTable#ROW}) is used to carry the
 * field names through type inference. After type inference the resulting
 * {@link org.apache.calcite.rel.type.RelDataType} carries the names, so
 * downstream code does not need to inspect the operator.
 *
 * <p>When no aliases are given, field names are auto-generated
 * ({@code EXPR$0}, {@code EXPR$1}, …).
 */
public class SqlRowOperator extends SqlSpecialOperator {

  /**
   * Optional explicit field names. When null, field names are auto-generated
   * ({@code EXPR$0}, {@code EXPR$1}, …). Individual entries may be null to
   * mix named and unnamed fields.
   */
  private final @Nullable List<@Nullable String> fieldNames;

  /** Constructor for the singleton (no field-name aliases). */
  public SqlRowOperator(String name) {
    this(name, null);
  }

  /** Returns the explicit field-name aliases, or null if none were specified
   * (in which case names are auto-generated as {@code EXPR$0}, etc.). */
  public @Nullable List<@Nullable String> getFieldNames() {
    return fieldNames;
  }

  /** Constructor for a named ROW operator with explicit field-name aliases.
   * Field names may be null, in which case they are auto-generated. */
  public SqlRowOperator(String name, @Nullable List<? extends @Nullable String> fieldNames) {
    super(name,
        SqlKind.ROW, MDX_PRECEDENCE,
        false,
        null,
        InferTypes.RETURN_TYPE,
        OperandTypes.VARIADIC);
    if (fieldNames == null) {
      this.fieldNames = null;
    } else {
      this.fieldNames = ImmutableNullableList.copyOf(fieldNames);
    }
  }

  //~ Methods ----------------------------------------------------------------

  @Override public RelDataType inferReturnType(
      final SqlOperatorBinding opBinding) {
    // The type of a ROW(e1,e2) expression is a record with the types
    // ROW(e1type,e2type).  Field names come from AS aliases when present;
    // otherwise they are implementation-defined.
    final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
    final RelDataTypeFactory.Builder builder = typeFactory.builder();
    for (int i = 0; i < opBinding.getOperandCount(); i++) {
      final String fieldName =
          fieldNames != null && fieldNames.get(i) != null
              ? fieldNames.get(i)
              : SqlUtil.deriveAliasFromOrdinal(i);
      builder.add(fieldName, opBinding.getOperandType(i));
    }
    final RelDataType recordType = builder.build();

    // The value of ROW(e1,e2) is considered null if and only all of its
    // fields (i.e., e1, e2) are null. Otherwise, ROW can not be null.
    final boolean nullable =
        recordType.getFieldList().stream()
            .allMatch(f -> f.getType().isNullable());
    return typeFactory.createTypeWithNullability(recordType, nullable);
  }

  @Override public RelDataType deriveType(
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlCall call) {
    if (fieldNames == null) {
      return super.deriveType(validator, scope, call);
    }
    // For named ROW: validate operand types without replacing this operator
    // via lookupRoutine (which would substitute the singleton and lose field names).
    for (SqlNode operand : call.getOperandList()) {
      validator.deriveType(scope, operand);
    }
    return validateOperands(validator, scope, call);
  }

  @Override public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    if (fieldNames == null) {
      SqlUtil.unparseFunctionSyntax(this, writer, call, false);
      return;
    }
    writer.print("ROW");
    writer.setNeedWhitespace(false);
    final SqlWriter.Frame frame =
        writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
    for (int i = 0; i < call.operandCount(); i++) {
      writer.sep(",");
      call.operand(i).unparse(writer, 0, 0);
      final String name = fieldNames.get(i);
      if (name != null) {
        writer.keyword("AS");
        writer.identifier(name, true);
      }
    }
    writer.endList(frame);
  }

  // override SqlOperator
  @Override public boolean requiresDecimalExpansion() {
    return false;
  }
}
