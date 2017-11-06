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
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidatorException;

import java.util.AbstractList;
import java.util.List;

/**
 * <code>SqlOperatorBinding</code> represents the binding of an
 * {@link SqlOperator} to actual operands, along with any additional information
 * required to validate those operands if needed.
 */
public abstract class SqlOperatorBinding {
  //~ Instance fields --------------------------------------------------------

  protected final RelDataTypeFactory typeFactory;
  private final SqlOperator sqlOperator;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a SqlOperatorBinding.
   *
   * @param typeFactory Type factory
   * @param sqlOperator Operator which is subject of this call
   */
  protected SqlOperatorBinding(
      RelDataTypeFactory typeFactory,
      SqlOperator sqlOperator) {
    this.typeFactory = typeFactory;
    this.sqlOperator = sqlOperator;
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * If the operator call occurs in an aggregate query, returns the number of
   * columns in the GROUP BY clause. For example, for "SELECT count(*) FROM emp
   * GROUP BY deptno, gender", returns 2.
   *
   * <p>Returns 0 if the query is implicitly "GROUP BY ()" because of an
   * aggregate expression. For example, "SELECT sum(sal) FROM emp".</p>
   *
   * <p>Returns -1 if the query is not an aggregate query.</p>
   */
  public int getGroupCount() {
    return -1;
  }

  /**
   * Returns whether the operator is an aggregate function with a filter.
   */
  public boolean hasFilter() {
    return false;
  }

  /**
   * @return bound operator
   */
  public SqlOperator getOperator() {
    return sqlOperator;
  }

  /**
   * @return factory for type creation
   */
  public RelDataTypeFactory getTypeFactory() {
    return typeFactory;
  }

  /**
   * Gets the string value of a string literal operand.
   *
   * @param ordinal zero-based ordinal of operand of interest
   * @return string value
   */
  @Deprecated // to be removed before 2.0
  public String getStringLiteralOperand(int ordinal) {
    throw new UnsupportedOperationException();
  }

  /**
   * Gets the integer value of a numeric literal operand.
   *
   * @param ordinal zero-based ordinal of operand of interest
   * @return integer value
   */
  @Deprecated // to be removed before 2.0
  public int getIntLiteralOperand(int ordinal) {
    throw new UnsupportedOperationException();
  }

  /**
   * Gets the value of a literal operand.
   *
   * <p>Cases:
   * <ul>
   * <li>If the operand is not a literal, the value is null.
   *
   * <li>If the operand is a string literal,
   * the value will be of type {@link org.apache.calcite.util.NlsString}.
   *
   * <li>If the operand is a numeric literal,
   * the value will be of type {@link java.math.BigDecimal}.
   *
   * <li>If the operand is an interval qualifier,
   * the value will be of type {@link SqlIntervalQualifier}</li>
   *
   * <li>Otherwise the type is undefined, and the value may be null.
   * </ul>
   *
   * @param ordinal zero-based ordinal of operand of interest
   * @param clazz Desired valued type
   *
   * @return value of operand
   */
  public <T> T getOperandLiteralValue(int ordinal, Class<T> clazz) {
    throw new UnsupportedOperationException();
  }

  @Deprecated // to be removed before 2.0
  public Comparable getOperandLiteralValue(int ordinal) {
    return getOperandLiteralValue(ordinal, Comparable.class);
  }

  /**
   * Determines whether a bound operand is NULL.
   *
   * <p>This is only relevant for SQL validation.
   *
   * @param ordinal   zero-based ordinal of operand of interest
   * @param allowCast whether to regard CAST(constant) as a constant
   * @return whether operand is null; false for everything except SQL
   * validation
   */
  public boolean isOperandNull(int ordinal, boolean allowCast) {
    throw new UnsupportedOperationException();
  }

  /**
   * Determines whether an operand is a literal.
   *
   * @param ordinal   zero-based ordinal of operand of interest
   * @param allowCast whether to regard CAST(literal) as a literal
   * @return whether operand is literal
   */
  public boolean isOperandLiteral(int ordinal, boolean allowCast) {
    throw new UnsupportedOperationException();
  }

  /**
   * @return the number of bound operands
   */
  public abstract int getOperandCount();

  /**
   * Gets the type of a bound operand.
   *
   * @param ordinal zero-based ordinal of operand of interest
   * @return bound operand type
   */
  public abstract RelDataType getOperandType(int ordinal);

  /**
   * Gets the monotonicity of a bound operand.
   *
   * @param ordinal zero-based ordinal of operand of interest
   * @return monotonicity of operand
   */
  public SqlMonotonicity getOperandMonotonicity(int ordinal) {
    return SqlMonotonicity.NOT_MONOTONIC;
  }

  /**
   * Collects the types of the bound operands into a list.
   *
   * @return collected list
   */
  public List<RelDataType> collectOperandTypes() {
    return new AbstractList<RelDataType>() {
      public RelDataType get(int index) {
        return getOperandType(index);
      }

      public int size() {
        return getOperandCount();
      }
    };
  }

  /**
   * Returns the rowtype of the <code>ordinal</code>th operand, which is a
   * cursor.
   *
   * <p>This is only implemented for {@link SqlCallBinding}.
   *
   * @param ordinal Ordinal of the operand
   * @return Rowtype of the query underlying the cursor
   */
  public RelDataType getCursorOperand(int ordinal) {
    throw new UnsupportedOperationException();
  }

  /**
   * Retrieves information about a column list parameter.
   *
   * @param ordinal    ordinal position of the column list parameter
   * @param paramName  name of the column list parameter
   * @param columnList returns a list of the column names that are referenced
   *                   in the column list parameter
   * @return the name of the parent cursor referenced by the column list
   * parameter if it is a column list parameter; otherwise, null is returned
   */
  public String getColumnListParamInfo(
      int ordinal,
      String paramName,
      List<String> columnList) {
    throw new UnsupportedOperationException();
  }

  /**
   * Wraps a validation error with context appropriate to this operator call.
   *
   * @param e Validation error, not null
   * @return Error wrapped, if possible, with positional information
   */
  public abstract CalciteException newError(
      Resources.ExInst<SqlValidatorException> e);
}

// End SqlOperatorBinding.java
