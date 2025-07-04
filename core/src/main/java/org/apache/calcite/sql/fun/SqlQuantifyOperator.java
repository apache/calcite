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
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

import static org.apache.calcite.util.Static.RESOURCE;

import static java.util.Objects.requireNonNull;

/**
 * Definition of the SQL <code>ALL</code> and <code>SOME</code>operators.
 *
 * <p>Each is used in combination with a relational operator:
 * <code>&lt;</code>, <code>&le;</code>,
 * <code>&gt;</code>, <code>&ge;</code>,
 * <code>=</code>, <code>&lt;&gt;</code>.
 *
 * <p><code>ANY</code> is a synonym for <code>SOME</code>.
 */
public class SqlQuantifyOperator extends SqlInOperator {
  //~ Instance fields --------------------------------------------------------

  public final SqlKind comparisonKind;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a SqlQuantifyOperator.
   *
   * @param kind Either ALL or SOME
   * @param comparisonKind Either <code>&lt;</code>, <code>&le;</code>,
   *   <code>&gt;</code>, <code>&ge;</code>,
   *   <code>=</code> or <code>&lt;&gt;</code>.
   */
  SqlQuantifyOperator(SqlKind kind, SqlKind comparisonKind) {
    super(comparisonKind.sql + " " + kind, kind);
    this.comparisonKind = requireNonNull(comparisonKind, "comparisonKind");
    checkArgument(comparisonKind == SqlKind.EQUALS
        || comparisonKind == SqlKind.NOT_EQUALS
        || comparisonKind == SqlKind.LESS_THAN_OR_EQUAL
        || comparisonKind == SqlKind.LESS_THAN
        || comparisonKind == SqlKind.GREATER_THAN_OR_EQUAL
        || comparisonKind == SqlKind.GREATER_THAN);
    checkArgument(kind == SqlKind.SOME
        || kind == SqlKind.ALL);
  }


  @Override public RelDataType deriveType(SqlValidator validator,
      SqlValidatorScope scope, SqlCall call) {
    final List<SqlNode> operands = call.getOperandList();
    assert operands.size() == 2;

    RelDataType typeForCollectionArgument = tryDeriveTypeForCollection(validator, scope, call);
    if (typeForCollectionArgument != null) {
      return typeForCollectionArgument;
    }
    return super.deriveType(validator, scope, call);
  }

  /**
   * Derive type for SOME(collection expression), ANY (collection expression).
   *
   * @param validator Validator
   * @param scope     Scope of validation
   * @param call      Call to this operator
   * @return If SOME or ALL is applied to a collection, then the function
   * returns type of call, otherwise it returns null.
   */
  public @Nullable RelDataType tryDeriveTypeForCollection(SqlValidator validator,
      SqlValidatorScope scope, SqlCall call) {
    final SqlNode left = call.operand(0);
    final SqlNode right = call.operand(1);
    if (right instanceof SqlNodeList && ((SqlNodeList) right).size() == 1) {
      RelDataType rightType =
          validator.deriveType(scope, ((SqlNodeList) right).get(0));
      if (SqlTypeUtil.isCollection(rightType)) {
        RelDataType componentRightType = requireNonNull(rightType.getComponentType());
        RelDataType leftType = validator.deriveType(scope, left);
        boolean isCompatibleType = isCompatibleType(componentRightType, leftType);
        SqlCall sqlCall =
            new SqlBasicCall(call.getOperator(),
                ImmutableList.of(left, ((SqlNodeList) right).get(0)), SqlParserPos.ZERO);
        SqlCallBinding callBinding = new SqlCallBinding(validator, scope, sqlCall);
        if (!isCompatibleType && callBinding.isTypeCoercionEnabled()) {
          boolean coerced = callBinding.getValidator().getTypeCoercion()
              .quantifyOperationCoercion(callBinding);
          if (coerced) {
            // Update the node data type if we coerced any type.
            call.setOperand(0,
                callBinding.operand(0));
            call.setOperand(1,
                new SqlNodeList(ImmutableList.of(callBinding.operand(1)), SqlParserPos.ZERO));
            leftType = validator.deriveType(scope, call.operand(0));
            rightType = validator.deriveType(scope, ((SqlNodeList) right).get(0));
            componentRightType = rightType.getComponentType();
            requireNonNull(componentRightType, "componentRightType");
            isCompatibleType = isCompatibleType(componentRightType, leftType);
          }
        }
        if (isCompatibleType) {
          return validator.getTypeFactory().createTypeWithNullability(
              validator.getTypeFactory().createSqlType(SqlTypeName.BOOLEAN),
              rightType.isNullable()
                  || componentRightType.isNullable()
                  || leftType.isNullable());
        }
        throw validator.newValidationError(call,
            RESOURCE.incompatibleValueType(call.getOperator().getName()));
      }
    }
    return null;
  }

  private static boolean isCompatibleType(RelDataType leftType, RelDataType rightType) {
    return SqlTypeUtil.sameNamedType(rightType, leftType)
        || SqlTypeUtil.isNull(leftType)
        || SqlTypeUtil.isNull(rightType);
  }

  @Override public SqlOperator not() {
    switch (kind) {
    case SOME:
      switch (comparisonKind) {
      case EQUALS:
        return SqlStdOperatorTable.ALL_NE;
      case NOT_EQUALS:
        return SqlStdOperatorTable.ALL_EQ;
      case LESS_THAN_OR_EQUAL:
        return SqlStdOperatorTable.ALL_GT;
      case LESS_THAN:
        return SqlStdOperatorTable.ALL_GE;
      case GREATER_THAN_OR_EQUAL:
        return SqlStdOperatorTable.ALL_LT;
      case GREATER_THAN:
        return SqlStdOperatorTable.ALL_LE;
      default:
        throw new AssertionError("unexpected SOME comparisonKind " + kind);
      }
    case ALL:
      switch (comparisonKind) {
      case EQUALS:
        return SqlStdOperatorTable.SOME_NE;
      case NOT_EQUALS:
        return SqlStdOperatorTable.SOME_EQ;
      case LESS_THAN_OR_EQUAL:
        return SqlStdOperatorTable.SOME_GT;
      case LESS_THAN:
        return SqlStdOperatorTable.SOME_GE;
      case GREATER_THAN_OR_EQUAL:
        return SqlStdOperatorTable.SOME_LT;
      case GREATER_THAN:
        return SqlStdOperatorTable.SOME_LE;
      default:
        throw new AssertionError("unexpected ALL comparisonKind " + kind);
      }
    default:
      throw new AssertionError("unexpected " + kind);
    }
  }
}
