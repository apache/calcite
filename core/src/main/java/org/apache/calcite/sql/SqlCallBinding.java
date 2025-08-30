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

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.TimeFrame;
import org.apache.calcite.rel.type.TimeFrameSet;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.fun.SqlLiteralChainOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SelectScope;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.NlsString;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

import static java.util.Objects.requireNonNull;

/**
 * <code>SqlCallBinding</code> implements {@link SqlOperatorBinding} by
 * analyzing to the operands of a {@link SqlCall} with a {@link SqlValidator}.
 */
public class SqlCallBinding extends SqlOperatorBinding {

  /** Static nested class required due to
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4393">[CALCITE-4393]
   * ExceptionInInitializerError due to NPE in SqlCallBinding caused by circular dependency</a>.
   * The static field inside it cannot be part of the outer class: it must be defined
   * within a nested class in order to break the cycle during class loading. */
  private static class DefaultCallHolder {
    private static final SqlCall DEFAULT_CALL =
        SqlStdOperatorTable.DEFAULT.createCall(SqlParserPos.ZERO);
  }

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
  public SqlCallBinding(SqlValidator validator, SqlValidatorScope scope,
      SqlCall call) {
    super(validator.getTypeFactory(), call.getOperator());
    this.validator = validator;
    this.scope = requireNonNull(scope, "scope");
    this.call = call;
  }

  //~ Methods ----------------------------------------------------------------

  @Deprecated
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

  @Override public boolean hasEmptyGroup() {
    final SelectScope selectScope =
        SqlValidatorUtil.getEnclosingSelectScope(scope);
    if (selectScope == null) {
      // Probably "VALUES expr". Treat same as "SELECT expr GROUP BY ()"
      return true;
    }
    final SqlSelect select = selectScope.getNode();
    final SqlNodeList group = select.getGroup();
    if (group != null) {
      return SqlValidatorUtil.hasEmptyGroup(group);
    }
    return validator.isAggregate(select);
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
    if (hasAssignment()
        && !(call.getOperator() instanceof SqlUnresolvedFunction)) {
      return permutedOperands(call);
    } else {
      final List<SqlNode> operandList = call.getOperandList();
      final SqlOperandTypeChecker checker =
          call.getOperator().getOperandTypeChecker();
      if (checker == null) {
        return operandList;
      }
      final SqlOperandCountRange range = checker.getOperandCountRange();
      final List<SqlNode> list = Lists.newArrayList(operandList);
      while (list.size() < range.getMax()
          && checker.isOptional(list.size())
          && checker.isFixedParameters()) {
        list.add(DefaultCallHolder.DEFAULT_CALL);
      }
      return list;
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
  private static List<SqlNode> permutedOperands(final SqlCall call) {
    final SqlOperator operator = call.getOperator();
    final SqlOperandMetadata operandMetadata =
        requireNonNull((SqlOperandMetadata) operator.getOperandTypeChecker(),
            () -> "operandTypeChecker is null for " + call
                + ", operator " + operator);
    final List<String> paramNames = operandMetadata.paramNames();
    final List<SqlNode> permuted = new ArrayList<>();
    // Always use case-insensitive lookup for parameter names
    final SqlNameMatcher nameMatcher =
        SqlNameMatchers.withCaseSensitive(false);
    for (final String paramName : paramNames) {
      for (int j = 0; j < call.getOperandList().size(); j++) {
        final SqlCall call2 = call.operand(j);
        assert call2.getKind() == SqlKind.ARGUMENT_ASSIGNMENT;
        final SqlIdentifier operandID = call2.operand(1);
        final String operandName = operandID.getSimple();
        if (nameMatcher.matches(operandName, paramName)) {
          permuted.add(call2.operand(0));
          break;
        }
        // the last operand, there is still no match.
        if (j == call.getOperandList().size() - 1) {
          if (operandMetadata.isFixedParameters()) {
            // Not like user defined functions, we do not patch up the operands
            // with DEFAULT and then convert to nulls during sql-to-rel conversion.
            // Thus, there is no need to show the optional operands in the plan and
            // decide if the optional operand is null when code generation.
            permuted.add(DefaultCallHolder.DEFAULT_CALL);
          }
        }
      }
    }
    return permuted;
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

  @Override public SqlMonotonicity getOperandMonotonicity(int ordinal) {
    return call.getOperandList().get(ordinal).getMonotonicity(scope);
  }

  @SuppressWarnings("deprecation")
  @Override public @Nullable String getStringLiteralOperand(int ordinal) {
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

  @Override public <T extends Object> @Nullable T getOperandLiteralValue(int ordinal,
      Class<T> clazz) {
    final SqlNode node = operand(ordinal);
    return valueAs(node, clazz);
  }

  private <T extends Object> @Nullable T valueAs(SqlNode node, Class<T> clazz) {
    final SqlLiteral literal;
    switch (node.getKind()) {
    case ARRAY_VALUE_CONSTRUCTOR:
      final List<@Nullable Object> list = new ArrayList<>();
      for (SqlNode o : ((SqlCall) node).getOperandList()) {
        list.add(valueAs(o, Object.class));
      }
      return clazz.cast(ImmutableNullableList.copyOf(list));

    case MAP_VALUE_CONSTRUCTOR:
      final ImmutableMap.Builder<Object, Object> builder2 =
          ImmutableMap.builder();
      final List<SqlNode> operands = ((SqlCall) node).getOperandList();
      for (int i = 0; i < operands.size(); i += 2) {
        final SqlNode key = operands.get(i);
        final SqlNode value = operands.get(i + 1);
        builder2.put(requireNonNull(valueAs(key, Object.class), "key"),
            requireNonNull(valueAs(value, Object.class), "value"));
      }
      return clazz.cast(builder2.build());

    case CAST:
      return valueAs(((SqlCall) node).operand(0), clazz);

    case LITERAL:
      literal = (SqlLiteral) node;
      if (literal.getTypeName() == SqlTypeName.NULL) {
        return null;
      }
      return literal.getValueAs(clazz);

    case LITERAL_CHAIN:
      literal = SqlLiteralChainOperator.concatenateOperands((SqlCall) node);
      return literal.getValueAs(clazz);

    case INTERVAL_QUALIFIER:
      final SqlIntervalQualifier q = (SqlIntervalQualifier) node;
      if (q.timeFrameName != null) {
        // Custom time frames can only be cast to String. You can do more with
        // them when validator has resolved to a TimeFrame.
        final TimeFrameSet timeFrameSet = validator.getTimeFrameSet();
        final TimeFrame timeFrame = timeFrameSet.getOpt(q.timeFrameName);
        if (clazz == String.class) {
          return clazz.cast(q.timeFrameName);
        }
        if (clazz == TimeUnit.class
            && timeFrame != null) {
          TimeUnit timeUnit = timeFrameSet.getUnit(timeFrame);
          return clazz.cast(timeUnit);
        }
        return null;
      }
      final SqlIntervalLiteral.IntervalValue intervalValue =
          new SqlIntervalLiteral.IntervalValue(q, 1, q.toString());
      literal = new SqlLiteral(intervalValue, q.typeName(), q.pos);
      return literal.getValueAs(clazz);

    case DEFAULT:
      return null; // currently NULL is the only default value

    default:
      if (SqlUtil.isNullLiteral(node, true)) {
        return null; // NULL literal
      }
      return null; // not a literal
    }
  }

  @Override public boolean isOperandNull(int ordinal, boolean allowCast) {
    return SqlUtil.isNullLiteral(operand(ordinal), allowCast);
  }

  @Override public boolean isOperandLiteral(int ordinal, boolean allowCast) {
    return SqlUtil.isLiteral(operand(ordinal), allowCast);
  }

  @Override public int getOperandCount() {
    return call.getOperandList().size();
  }

  @Override public RelDataType getOperandType(int ordinal) {
    final SqlNode operand = call.operand(ordinal);
    final RelDataType type = SqlTypeUtil.deriveType(this, operand);
    final SqlValidatorNamespace namespace = validator.getNamespace(operand);
    if (namespace != null) {
      return namespace.getType();
    }
    return type;
  }

  @Override public @Nullable RelDataType getCursorOperand(int ordinal) {
    final SqlNode operand = call.operand(ordinal);
    if (!SqlUtil.isCallTo(operand, SqlStdOperatorTable.CURSOR)) {
      return null;
    }
    final SqlCall cursorCall = (SqlCall) operand;
    final SqlNode query = cursorCall.operand(0);
    return SqlTypeUtil.deriveType(this, query);
  }

  @Override public @Nullable String getColumnListParamInfo(
      int ordinal,
      String paramName,
      List<String> columnList) {
    final SqlNode operand = call.operand(ordinal);
    if (!SqlUtil.isCallTo(operand, SqlStdOperatorTable.ROW)) {
      return null;
    }
    columnList.addAll(
        SqlIdentifier.simpleNames(((SqlCall) operand).getOperandList()));
    return validator.getParentCursor(paramName);
  }

  @Override public CalciteException newError(
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

  /**
   * Returns whether to allow implicit type coercion when validation.
   * This is a short-cut method.
   */
  public boolean isTypeCoercionEnabled() {
    return validator.config().typeCoercionEnabled();
  }
}
