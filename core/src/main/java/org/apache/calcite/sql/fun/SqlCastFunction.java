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
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlMonotonicity;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * SqlCastFunction. Note that the std functions are really singleton objects,
 * because they always get fetched via the StdOperatorTable. So you can't store
 * any local info in the class and hence the return type data is maintained in
 * operand[1] through the validation phase.
 */
public class SqlCastFunction extends SqlFunction {
  //~ Instance fields --------------------------------------------------------

  private final Set<TypeFamilyCast> nonMonotonicPreservingCasts =
      createNonMonotonicPreservingCasts();

  //~ Constructors -----------------------------------------------------------

  public SqlCastFunction() {
    super(
        "CAST",
        SqlKind.CAST,
        null,
        InferTypes.FIRST_KNOWN,
        null,
        SqlFunctionCategory.SYSTEM);
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * List all casts that do not preserve monotonicity.
   */
  private Set<TypeFamilyCast> createNonMonotonicPreservingCasts() {
    ImmutableSet.Builder<TypeFamilyCast> builder = ImmutableSet.builder();
    add(builder, SqlTypeFamily.EXACT_NUMERIC, SqlTypeFamily.CHARACTER);
    add(builder, SqlTypeFamily.NUMERIC, SqlTypeFamily.CHARACTER);
    add(builder, SqlTypeFamily.APPROXIMATE_NUMERIC, SqlTypeFamily.CHARACTER);
    add(builder, SqlTypeFamily.DATETIME_INTERVAL, SqlTypeFamily.CHARACTER);
    add(builder, SqlTypeFamily.CHARACTER, SqlTypeFamily.EXACT_NUMERIC);
    add(builder, SqlTypeFamily.CHARACTER, SqlTypeFamily.NUMERIC);
    add(builder, SqlTypeFamily.CHARACTER, SqlTypeFamily.APPROXIMATE_NUMERIC);
    add(builder, SqlTypeFamily.CHARACTER, SqlTypeFamily.DATETIME_INTERVAL);
    add(builder, SqlTypeFamily.DATETIME, SqlTypeFamily.TIME);
    add(builder, SqlTypeFamily.TIMESTAMP, SqlTypeFamily.TIME);
    add(builder, SqlTypeFamily.TIME, SqlTypeFamily.DATETIME);
    add(builder, SqlTypeFamily.TIME, SqlTypeFamily.TIMESTAMP);
    return builder.build();
  }

  private void add(ImmutableSet.Builder<TypeFamilyCast> result,
      SqlTypeFamily from, SqlTypeFamily to) {
    result.add(new TypeFamilyCast(from, to));
  }

  private boolean isMonotonicPreservingCast(
      RelDataTypeFamily castFrom,
      RelDataTypeFamily castTo) {
    return !nonMonotonicPreservingCasts.contains(
        new TypeFamilyCast(castFrom, castTo));
  }

  public RelDataType inferReturnType(
      SqlOperatorBinding opBinding) {
    assert opBinding.getOperandCount() == 2;
    RelDataType ret = opBinding.getOperandType(1);
    RelDataType firstType = opBinding.getOperandType(0);
    ret =
        opBinding.getTypeFactory().createTypeWithNullability(
            ret,
            firstType.isNullable());
    if (opBinding instanceof SqlCallBinding) {
      SqlCallBinding callBinding = (SqlCallBinding) opBinding;
      SqlNode operand0 = callBinding.operand(0);

      // dynamic parameters and null constants need their types assigned
      // to them using the type they are casted to.
      if (((operand0 instanceof SqlLiteral)
          && (((SqlLiteral) operand0).getValue() == null))
          || (operand0 instanceof SqlDynamicParam)) {
        callBinding.getValidator().setValidatedNodeType(
            operand0,
            ret);
      }
    }
    return ret;
  }

  public String getSignatureTemplate(final int operandsCount) {
    assert operandsCount == 2;
    return "{0}({1} AS {2})";
  }

  public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.of(2);
  }

  /**
   * Makes sure that the number and types of arguments are allowable.
   * Operators (such as "ROW" and "AS") which do not check their arguments can
   * override this method.
   */
  public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    final SqlNode left = callBinding.operand(0);
    final SqlNode right = callBinding.operand(1);
    if (SqlUtil.isNullLiteral(left, false)
        || left instanceof SqlDynamicParam) {
      return true;
    }
    RelDataType validatedNodeType =
        callBinding.getValidator().getValidatedNodeType(left);
    RelDataType returnType =
        callBinding.getValidator().deriveType(callBinding.getScope(), right);
    if (!SqlTypeUtil.canCastFrom(returnType, validatedNodeType, true)) {
      if (throwOnFailure) {
        throw callBinding.newError(
            RESOURCE.cannotCastValue(validatedNodeType.toString(),
                returnType.toString()));
      }
      return false;
    }
    if (SqlTypeUtil.areCharacterSetsMismatched(
        validatedNodeType,
        returnType)) {
      if (throwOnFailure) {
        // Include full type string to indicate character
        // set mismatch.
        throw callBinding.newError(
            RESOURCE.cannotCastValue(validatedNodeType.getFullTypeString(),
                returnType.getFullTypeString()));
      }
      return false;
    }
    return true;
  }

  public SqlSyntax getSyntax() {
    return SqlSyntax.SPECIAL;
  }

  public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    assert call.operandCount() == 2;
    final SqlWriter.Frame frame = writer.startFunCall(getName());
    call.operand(0).unparse(writer, 0, 0);
    writer.sep("AS");
    if (call.operand(1) instanceof SqlIntervalQualifier) {
      writer.sep("INTERVAL");
    }
    call.operand(1).unparse(writer, 0, 0);
    writer.endFunCall(frame);
  }

  @Override public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
    RelDataTypeFamily castFrom = call.getOperandType(0).getFamily();
    RelDataTypeFamily castTo = call.getOperandType(1).getFamily();
    if (isMonotonicPreservingCast(castFrom, castTo)) {
      return call.getOperandMonotonicity(0);
    } else {
      return SqlMonotonicity.NOT_MONOTONIC;
    }
  }

  //~ Inner Classes ----------------------------------------------------------

  /** Pair of source-target type families. */
  private class TypeFamilyCast {
    private final RelDataTypeFamily castFrom;
    private final RelDataTypeFamily castTo;

    public TypeFamilyCast(
        RelDataTypeFamily castFrom,
        RelDataTypeFamily castTo) {
      this.castFrom = castFrom;
      this.castTo = castTo;
    }

    @Override public boolean equals(Object obj) {
      // TODO Auto-generated method stub
      if (obj.getClass() != TypeFamilyCast.class) {
        return false;
      }
      TypeFamilyCast other = (TypeFamilyCast) obj;
      return this.castFrom.equals(other.castFrom)
          && this.castTo.equals(other.castTo);
    }

    @Override public int hashCode() {
      // TODO Auto-generated method stub
      return castFrom.hashCode() + castTo.hashCode();
    }
  }
}

// End SqlCastFunction.java
