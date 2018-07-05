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
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SelectScope;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.NlsString;

import com.google.common.collect.Lists;

import java.math.BigDecimal;
import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * <code>SqlCallBinding</code> implements {@link SqlOperatorBinding} by
 * analyzing to the operands of a {@link SqlCall} with a {@link SqlValidator}.
 */
public class SqlCallBinding extends SqlOperatorBinding {
  private static final SqlCall DEFAULT_CALL =
      SqlStdOperatorTable.DEFAULT.createCall(SqlParserPos.ZERO);
  //~ Instance fields --------------------------------------------------------

  private final SqlValidator validator;
  private final SqlValidatorScope scope;
  private final SqlCall call;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a call binding.
   *
   * @param validator Validator
   * @param scope     Scope of call
   * @param call      Call node
   */
  public SqlCallBinding(
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlCall call) {
    super(
        validator.getTypeFactory(),
        call.getOperator());
    this.validator = validator;
    this.scope = scope;
    this.call = call;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public int getGroupCount() {
    final SelectScope selectScope =
        SqlValidatorUtil.getEnclosingSelectScope(scope);
    if (selectScope == null) {
      // Probably "VALUES expr". Treat same as "SELECT expr GROUP BY ()"
      return 0;
    }
    final SqlSelect select = selectScope.getNode();
    final SqlNodeList group = select.getGroup();
    if (group != null) {
      int n = 0;
      for (SqlNode groupItem : group) {
        if (!(groupItem instanceof SqlNodeList)
            || ((SqlNodeList) groupItem).size() != 0) {
          ++n;
        }
      }
      return n;
    }
    return validator.isAggregate(select) ? 0 : -1;
  }

  /**
   * Returns the validator.
   */
  public SqlValidator getValidator() {
    return validator;
  }

  /**
   * Returns the scope of the call.
   */
  public SqlValidatorScope getScope() {
    return scope;
  }

  /**
   * Returns the call node.
   */
  public SqlCall getCall() {
    return call;
  }

  /** Returns the operands to a call permuted into the same order as the
   * formal parameters of the function. */
  public List<SqlNode> operands() {
    if (hasAssignment() && !(call.getOperator() instanceof SqlUnresolvedFunction)) {
      return permutedOperands(call);
    } else {
      final List<SqlNode> operandList = call.getOperandList();
      if (call.getOperator() instanceof SqlFunction) {
        final List<RelDataType> paramTypes =
            ((SqlFunction) call.getOperator()).getParamTypes();
        if (paramTypes != null && operandList.size() < paramTypes.size()) {
          final List<SqlNode> list = Lists.newArrayList(operandList);
          while (list.size() < paramTypes.size()) {
            list.add(DEFAULT_CALL);
          }
          return list;
        }
      }
      return operandList;
    }
  }

  /** Returns whether arguments have name assignment. */
  private boolean hasAssignment() {
    for (SqlNode operand : call.getOperandList()) {
      if (operand != null
          && operand.getKind() == SqlKind.ARGUMENT_ASSIGNMENT) {
        return true;
      }
    }
    return false;
  }

  /** Returns the operands to a call permuted into the same order as the
   * formal parameters of the function. */
  private List<SqlNode> permutedOperands(final SqlCall call) {
    final SqlFunction operator = (SqlFunction) call.getOperator();
    return Lists.transform(operator.getParamNames(), paramName -> {
      for (SqlNode operand2 : call.getOperandList()) {
        final SqlCall call2 = (SqlCall) operand2;
        assert operand2.getKind() == SqlKind.ARGUMENT_ASSIGNMENT;
        final SqlIdentifier id = call2.operand(1);
        if (id.getSimple().equals(paramName)) {
          return call2.operand(0);
        }
      }
      return DEFAULT_CALL;
    });
  }

  /**
   * Returns a particular operand.
   */
  public SqlNode operand(int i) {
    return operands().get(i);
  }

  /** Returns a call that is equivalent except that arguments have been
   * permuted into the logical order. Any arguments whose default value is being
   * used are null. */
  public SqlCall permutedCall() {
    final List<SqlNode> operandList = operands();
    if (operandList.equals(call.getOperandList())) {
      return call;
    }
    return call.getOperator().createCall(call.pos, operandList);
  }

  public SqlMonotonicity getOperandMonotonicity(int ordinal) {
    return call.getOperandList().get(ordinal).getMonotonicity(scope);
  }

  @SuppressWarnings("deprecation")
  @Override public String getStringLiteralOperand(int ordinal) {
    SqlNode node = call.operand(ordinal);
    final Object o = SqlLiteral.value(node);
    return o instanceof NlsString ? ((NlsString) o).getValue() : null;
  }

  @SuppressWarnings("deprecation")
  @Override public int getIntLiteralOperand(int ordinal) {
    SqlNode node = call.operand(ordinal);
    final Object o = SqlLiteral.value(node);
    if (o instanceof BigDecimal) {
      BigDecimal bd = (BigDecimal) o;
      try {
        return bd.intValueExact();
      } catch (ArithmeticException e) {
        throw SqlUtil.newContextException(node.pos,
            RESOURCE.numberLiteralOutOfRange(bd.toString()));
      }
    }
    throw new AssertionError();
  }

  @Override public <T> T getOperandLiteralValue(int ordinal, Class<T> clazz) {
    try {
      final SqlNode node = call.operand(ordinal);
      return SqlLiteral.unchain(node).getValueAs(clazz);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  @Override public boolean isOperandNull(int ordinal, boolean allowCast) {
    return SqlUtil.isNullLiteral(call.operand(ordinal), allowCast);
  }

  @Override public boolean isOperandLiteral(int ordinal, boolean allowCast) {
    return SqlUtil.isLiteral(call.operand(ordinal), allowCast);
  }

  @Override public int getOperandCount() {
    return call.getOperandList().size();
  }

  @Override public RelDataType getOperandType(int ordinal) {
    final SqlNode operand = call.operand(ordinal);
    final RelDataType type = validator.deriveType(scope, operand);
    final SqlValidatorNamespace namespace = validator.getNamespace(operand);
    if (namespace != null) {
      return namespace.getType();
    }
    return type;
  }

  @Override public RelDataType getCursorOperand(int ordinal) {
    final SqlNode operand = call.operand(ordinal);
    if (!SqlUtil.isCallTo(operand, SqlStdOperatorTable.CURSOR)) {
      return null;
    }
    final SqlCall cursorCall = (SqlCall) operand;
    final SqlNode query = cursorCall.operand(0);
    return validator.deriveType(scope, query);
  }

  @Override public String getColumnListParamInfo(
      int ordinal,
      String paramName,
      List<String> columnList) {
    final SqlNode operand = call.operand(ordinal);
    if (!SqlUtil.isCallTo(operand, SqlStdOperatorTable.ROW)) {
      return null;
    }
    for (SqlNode id : ((SqlCall) operand).getOperandList()) {
      columnList.add(((SqlIdentifier) id).getSimple());
    }
    return validator.getParentCursor(paramName);
  }

  public CalciteException newError(
      Resources.ExInst<SqlValidatorException> e) {
    return validator.newValidationError(call, e);
  }

  /**
   * Constructs a new validation signature error for the call.
   *
   * @return signature exception
   */
  public CalciteException newValidationSignatureError() {
    return validator.newValidationError(call,
        RESOURCE.canNotApplyOp2Type(getOperator().getName(),
            call.getCallSignature(validator, scope),
            getOperator().getAllowedSignatures()));
  }

  /**
   * Constructs a new validation error for the call. (Do not use this to
   * construct a validation error for other nodes such as an operands.)
   *
   * @param ex underlying exception
   * @return wrapped exception
   */
  public CalciteException newValidationError(
      Resources.ExInst<SqlValidatorException> ex) {
    return validator.newValidationError(call, ex);
  }
}

// End SqlCallBinding.java
