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
package org.eigenbase.sql.fun;

import java.util.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.*;

import static org.eigenbase.util.Static.RESOURCE;

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
    Set<TypeFamilyCast> result = new HashSet<TypeFamilyCast>();
    result.add(
        new TypeFamilyCast(
            SqlTypeFamily.EXACT_NUMERIC,
            SqlTypeFamily.CHARACTER));
    result.add(
        new TypeFamilyCast(
            SqlTypeFamily.NUMERIC,
            SqlTypeFamily.CHARACTER));
    result.add(
        new TypeFamilyCast(
            SqlTypeFamily.APPROXIMATE_NUMERIC,
            SqlTypeFamily.CHARACTER));
    result.add(
        new TypeFamilyCast(
            SqlTypeFamily.DATETIME_INTERVAL,
            SqlTypeFamily.CHARACTER));
    result.add(
        new TypeFamilyCast(
            SqlTypeFamily.CHARACTER,
            SqlTypeFamily.EXACT_NUMERIC));
    result.add(
        new TypeFamilyCast(
            SqlTypeFamily.CHARACTER,
            SqlTypeFamily.NUMERIC));
    result.add(
        new TypeFamilyCast(
            SqlTypeFamily.CHARACTER,
            SqlTypeFamily.APPROXIMATE_NUMERIC));
    result.add(
        new TypeFamilyCast(
            SqlTypeFamily.CHARACTER,
            SqlTypeFamily.DATETIME_INTERVAL));
    result.add(
        new TypeFamilyCast(
            SqlTypeFamily.DATETIME,
            SqlTypeFamily.TIME));
    result.add(
        new TypeFamilyCast(
            SqlTypeFamily.TIMESTAMP,
            SqlTypeFamily.TIME));
    result.add(
        new TypeFamilyCast(
            SqlTypeFamily.TIME,
            SqlTypeFamily.DATETIME));
    result.add(
        new TypeFamilyCast(
            SqlTypeFamily.TIME,
            SqlTypeFamily.TIMESTAMP));
    return result;
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
      SqlNode operand0 = callBinding.getCall().operand(0);

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
    final SqlNode left = callBinding.getCall().operand(0);
    final SqlNode right = callBinding.getCall().operand(1);
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

  @Override
  public SqlMonotonicity getMonotonicity(
      SqlCall call,
      SqlValidatorScope scope) {
    RelDataTypeFamily castFrom =
        scope.getValidator().deriveType(scope, call.operand(0)).getFamily();
    RelDataTypeFamily castTo =
        scope.getValidator().deriveType(scope, call.operand(1)).getFamily();
    if (isMonotonicPreservingCast(castFrom, castTo)) {
      return call.operand(0).getMonotonicity(scope);
    } else {
      return SqlMonotonicity.NOT_MONOTONIC;
    }
  }

  //~ Inner Classes ----------------------------------------------------------

  private class TypeFamilyCast {
    private final RelDataTypeFamily castFrom;
    private final RelDataTypeFamily castTo;

    public TypeFamilyCast(
        RelDataTypeFamily castFrom,
        RelDataTypeFamily castTo) {
      this.castFrom = castFrom;
      this.castTo = castTo;
    }

    @Override
    public boolean equals(Object obj) {
      // TODO Auto-generated method stub
      if (obj.getClass() != TypeFamilyCast.class) {
        return false;
      }
      TypeFamilyCast other = (TypeFamilyCast) obj;
      return this.castFrom.equals(other.castFrom)
          && this.castTo.equals(other.castTo);
    }

    @Override
    public int hashCode() {
      // TODO Auto-generated method stub
      return castFrom.hashCode() + castTo.hashCode();
    }
  }
}

// End SqlCastFunction.java
