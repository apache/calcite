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
package org.apache.calcite.sql.validate.implicit;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.List;

/**
 * Default Strategies to coerce differing types that participate in
 * operations into compatible ones.
 *
 * <p>Notes about type widening / tightest common types: Broadly, there are two cases that need
 * to widen data types (i.e. set operations, binary comparison):
 *
 * <ul>
 * <li>Case1: Look for a common data type for two or more data types,
 * and no loss of precision is allowed. Including type inference for returned/operands
 * type (i.e. what's the column's common data type if one row is an integer while the
 * other row is a long?).</li>
 * <li>Case2: Look for a widen data type with some acceptable loss of precision
 * (i.e. there is no common type for double and decimal because double's range is larger than
 * decimal(with default max precision), and yet decimal is more precise than double,
 * but in union we would cast the decimal to double).</li>
 * </ul>
 * <p>REFERENCE: <a href="https://docs.microsoft.com/en-us/sql/t-sql/data-types/data-type-conversion-database-engine?">SQL-SERVER</a>
 * <a href="https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types">HIVE</a></p>
 */
public interface TypeCoercion {
  /**
   * Case1: type widening with no precision loss.
   * Find the tightest common type of two types that might be used in binary expression.
   *
   * @return common type
   */
  RelDataType getTightestCommonType(RelDataType type1, RelDataType type2);

  /**
   * Case2: type widening. The main difference with
   * {@link #getTightestCommonType} is that we allow
   * some precision loss when widening decimal to fractional, or promote to string type.
   */
  RelDataType getWiderTypeForTwo(RelDataType type1, RelDataType type2, boolean stringPromotion);

  /**
   * Similar to {@link #getWiderTypeForTwo}, but can handle
   * sequence types. {@link #getWiderTypeForTwo} doesn't satisfy the associative law,
   * i.e. (a op b) op c may not equal to a op (b op c). This is only a problem for StringType or
   * nested StringType in collection type like Array. Excluding these types,
   * {@link #getWiderTypeForTwo} satisfies the associative law. For instance,
   * (DATE, INTEGER, VARCHAR) should have VARCHAR as the wider common type.
   */
  RelDataType getWiderTypeFor(List<RelDataType> typeList, boolean stringPromotion);

  /**
   * Finds a wider type when one or both types are decimal type.
   *
   * <p>If the wider decimal type's precision/scale exceeds system limitation,
   * this rule will truncate the decimal type to the max precision/scale.
   * For decimal and fractional types, returns a decimal type
   * which has the higher precision of the two.
   *
   * <p>The default implementation depends on the max precision/scale of the type system,
   * you can override it based on the specific system requirement in
   * {@link org.apache.calcite.rel.type.RelDataTypeSystem}.
   */
  RelDataType getWiderTypeForDecimal(RelDataType type1, RelDataType type2);

  /**
   * Determines common type for a comparison operator whose operands are String
   * type and the other (non String) type.
   */
  RelDataType commonTypeForBinaryComparison(RelDataType type1, RelDataType type2);

  /**
   * Widen a SqlNode ith column type to target type, mainly used for set
   * operations like UNION, INTERSECT and EXCEPT.
   *
   * @param scope       scope to query
   * @param query       SqlNode which have children nodes as columns
   * @param columnIndex target column index
   * @param targetType  target type to cast to
   * @return true if we add any cast in successfully.
   */
  boolean rowTypeCoercion(
      SqlValidatorScope scope,
      SqlNode query,
      int columnIndex,
      RelDataType targetType);

  /**
   * Handles type coercion for IN operation with or without sub-query.
   *
   * <p>See {@link TypeCoercionImpl} for default strategies.
   */
  boolean inOperationCoercion(SqlCallBinding binding);

  /** Coerce operand of binary arithmetic expressions to Numeric type.*/
  boolean binaryArithmeticCoercion(SqlCallBinding binding);

  /**
   * Coerce CASE WHEN statement branches to one common type.
   *
   * <p>Rules: Find common type for all the then operands and else operands,
   * then try to coerce the then/else operands to the type if needed.
   */
  boolean caseWhenCoercion(SqlCallBinding binding);

  /**
   * Type coercion with inferred type from passed in arguments and the
   * {@link SqlTypeFamily} defined in the checkers, e.g. the
   * {@link org.apache.calcite.sql.type.FamilyOperandTypeChecker}.
   *
   * <p>Caution that we do not cast from numeric if desired type family is also
   * {@link SqlTypeFamily#NUMERIC}.
   *
   * <p>If the {@link org.apache.calcite.sql.type.FamilyOperandTypeChecker}s are
   * subsumed in a
   * {@link org.apache.calcite.sql.type.CompositeOperandTypeChecker}, check them
   * based on their combination order. i.e. If we allows a (numeric, numeric) OR
   * (string, numeric) family but with arguments (op1, op2) of types
   * (varchar(20), boolean), try to coerce op1 to numeric and op2 to numeric if
   * the type coercion rules allow it, or else try to coerce op2 to numeric and
   * keep op1 the type as it is.
   *
   * <p>This is also very interrelated to the composition predicate for the
   * checkers: if the predicate is AND, we would fail fast if the first family
   * type coercion fails.
   *
   * @param binding          Call binding
   * @param operandTypes     Types of the operands passed in
   * @param expectedFamilies Expected SqlTypeFamily list by user specified
   * @return true if we successfully do any implicit cast
   */
  boolean builtinFunctionCoercion(
      SqlCallBinding binding,
      List<RelDataType> operandTypes,
      List<SqlTypeFamily> expectedFamilies);

  /**
   * Non built-in functions (UDFs) type coercion, compare the types of arguments
   * with rules:
   *
   * <ol>
   * <li>named param: find the desired type by the passed in operand's name
   * <li>non-named param: find the desired type by formal parameter ordinal
   * </ol>
   *
   * <p>Try to make type coercion only of the desired type is found.
   *
   * @return true if any operands is coerced
   */
  boolean userDefinedFunctionCoercion(SqlValidatorScope scope, SqlCall call,
      SqlFunction function);
}

// End TypeCoercion.java
