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
package org.apache.calcite.sql.type;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.ExplicitOperatorBinding;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.util.Glossary;
import org.apache.calcite.util.Util;

import com.google.common.base.Preconditions;

import java.util.AbstractList;
import java.util.List;
import java.util.function.UnaryOperator;

import static org.apache.calcite.sql.type.NonNullableAccessors.getCharset;
import static org.apache.calcite.sql.type.NonNullableAccessors.getCollation;
import static org.apache.calcite.sql.validate.SqlNonNullableAccessors.getNamespace;
import static org.apache.calcite.util.Static.RESOURCE;

import static java.util.Objects.requireNonNull;

/**
 * A collection of return-type inference strategies.
 */
public abstract class ReturnTypes {
  private ReturnTypes() {
  }

  /** Creates a return-type inference that applies a rule then a sequence of
   * rules, returning the first non-null result.
   *
   * @see SqlReturnTypeInference#orElse(SqlReturnTypeInference) */
  public static SqlReturnTypeInferenceChain chain(
      SqlReturnTypeInference... rules) {
    return new SqlReturnTypeInferenceChain(rules);
  }

  /** Creates a return-type inference that applies a rule then a sequence of
   * transforms.
   *
   * @see SqlReturnTypeInference#andThen(SqlTypeTransform) */
  public static SqlTypeTransformCascade cascade(SqlReturnTypeInference rule,
      SqlTypeTransform... transforms) {
    return new SqlTypeTransformCascade(rule, transforms);
  }

  public static ExplicitReturnTypeInference explicit(
      RelProtoDataType protoType) {
    return new ExplicitReturnTypeInference(protoType);
  }

  /**
   * Creates an inference rule which returns a copy of a given data type.
   */
  public static ExplicitReturnTypeInference explicit(RelDataType type) {
    return explicit(RelDataTypeImpl.proto(type));
  }

  /**
   * Creates an inference rule which returns a type with no precision or scale,
   * such as {@code DATE}.
   */
  public static ExplicitReturnTypeInference explicit(SqlTypeName typeName) {
    return explicit(RelDataTypeImpl.proto(typeName, false));
  }

  /**
   * Creates an inference rule which returns a type with precision but no scale,
   * such as {@code VARCHAR(100)}.
   */
  public static ExplicitReturnTypeInference explicit(SqlTypeName typeName,
      int precision) {
    return explicit(RelDataTypeImpl.proto(typeName, precision, false));
  }

  /** Returns a return-type inference that first transforms a binding and
   * then applies an inference.
   *
   * <p>{@link #stripOrderBy} is an example of {@code bindingTransform}. */
  public static SqlReturnTypeInference andThen(
      UnaryOperator<SqlOperatorBinding> bindingTransform,
      SqlReturnTypeInference typeInference) {
    return opBinding ->
        typeInference.inferReturnType(bindingTransform.apply(opBinding));
  }

  /** Converts a binding of {@code FOO(x, y ORDER BY z)}
   * or {@code FOO(x, y ORDER BY z SEPARATOR s)}
   * to a binding of {@code FOO(x, y)}.
   * Used for {@code STRING_AGG} and {@code GROUP_CONCAT}. */
  public static SqlOperatorBinding stripOrderBy(
      SqlOperatorBinding operatorBinding) {
    if (operatorBinding instanceof SqlCallBinding) {
      final SqlCallBinding callBinding = (SqlCallBinding) operatorBinding;
      final SqlCall call2 = stripSeparator(callBinding.getCall());
      final SqlCall call3 = stripOrderBy(call2);
      if (call3 != callBinding.getCall()) {
        return new SqlCallBinding(callBinding.getValidator(),
            callBinding.getScope(), call3);
      }
    }
    return operatorBinding;
  }

  public static SqlCall stripOrderBy(SqlCall call) {
    if (!call.getOperandList().isEmpty()
        && Util.last(call.getOperandList()) instanceof SqlNodeList) {
      // Remove the last argument if it is "ORDER BY". The parser stashes the
      // ORDER BY clause in the argument list but it does not take part in
      // type derivation.
      return call.getOperator().createCall(call.getFunctionQuantifier(),
          call.getParserPosition(), Util.skipLast(call.getOperandList()));
    }
    return call;
  }

  public static SqlCall stripSeparator(SqlCall call) {
    if (!call.getOperandList().isEmpty()
        && Util.last(call.getOperandList()).getKind() == SqlKind.SEPARATOR) {
      // Remove the last argument if it is "SEPARATOR literal".
      return call.getOperator().createCall(call.getFunctionQuantifier(),
          call.getParserPosition(), Util.skipLast(call.getOperandList()));
    }
    return call;
  }

  /**
   * Type-inference strategy whereby the result type of a call is the type of
   * the operand #0 (0-based).
   */
  public static final SqlReturnTypeInference ARG0 =
      new OrdinalReturnTypeInference(0);

  /**
   * Type-inference strategy whereby the result type of a call is VARYING the
   * type of the first argument. The length returned is the same as length of
   * the first argument. If any of the other operands are nullable the
   * returned type will also be nullable. First Arg must be of string type.
   */
  public static final SqlReturnTypeInference ARG0_NULLABLE_VARYING =
      ARG0.andThen(SqlTypeTransforms.TO_NULLABLE)
          .andThen(SqlTypeTransforms.TO_VARYING);

  /**
   * Type-inference strategy whereby the result type of a call is the type of
   * the operand #0 (0-based). If any of the other operands are nullable the
   * returned type will also be nullable.
   */
  public static final SqlReturnTypeInference ARG0_NULLABLE =
      ARG0.andThen(SqlTypeTransforms.TO_NULLABLE);

  /**
   * Type-inference strategy whereby the result type of a call is the type of
   * the operand #0 (0-based), with nulls always allowed.
   */
  public static final SqlReturnTypeInference ARG0_FORCE_NULLABLE =
      ARG0.andThen(SqlTypeTransforms.FORCE_NULLABLE);

  public static final SqlReturnTypeInference ARG0_INTERVAL =
      new MatchReturnTypeInference(0,
          SqlTypeFamily.DATETIME_INTERVAL.getTypeNames());

  public static final SqlReturnTypeInference ARG0_INTERVAL_NULLABLE =
      ARG0_INTERVAL.andThen(SqlTypeTransforms.TO_NULLABLE);

  /**
   * Type-inference strategy whereby the result type of a call is the type of
   * the operand #0 (0-based), and nullable if the call occurs within a
   * "GROUP BY ()" query. E.g. in "select sum(1) as s from empty", s may be
   * null.
   */
  public static final SqlReturnTypeInference ARG0_NULLABLE_IF_EMPTY =
      new OrdinalReturnTypeInference(0) {
        @Override public RelDataType
        inferReturnType(SqlOperatorBinding opBinding) {
          final RelDataType type = super.inferReturnType(opBinding);
          if (opBinding.getGroupCount() == 0 || opBinding.hasFilter()) {
            return opBinding.getTypeFactory()
                .createTypeWithNullability(type, true);
          } else {
            return type;
          }
        }
      };

  /**
   * Type-inference strategy whereby the result type of a call is the type of
   * the operand #1 (0-based).
   */
  public static final SqlReturnTypeInference ARG1 =
      new OrdinalReturnTypeInference(1);

  /**
   * Type-inference strategy whereby the result type of a call is the type of
   * the operand #1 (0-based). If any of the other operands are nullable the
   * returned type will also be nullable.
   */
  public static final SqlReturnTypeInference ARG1_NULLABLE =
      ARG1.andThen(SqlTypeTransforms.TO_NULLABLE);

  /**
   * Type-inference strategy whereby the result type of a call is the type of
   * operand #2 (0-based).
   */
  public static final SqlReturnTypeInference ARG2 =
      new OrdinalReturnTypeInference(2);

  /**
   * Type-inference strategy whereby the result type of a call is the type of
   * operand #2 (0-based). If any of the other operands are nullable the
   * returned type will also be nullable.
   */
  public static final SqlReturnTypeInference ARG2_NULLABLE =
      ARG2.andThen(SqlTypeTransforms.TO_NULLABLE);

  /**
   * Type-inference strategy whereby the result type of a call is Boolean.
   */
  public static final SqlReturnTypeInference BOOLEAN =
      explicit(SqlTypeName.BOOLEAN);
  /**
   * Type-inference strategy whereby the result type of a call is Boolean,
   * with nulls allowed if any of the operands allow nulls.
   */
  public static final SqlReturnTypeInference BOOLEAN_NULLABLE =
      BOOLEAN.andThen(SqlTypeTransforms.TO_NULLABLE);

  /**
   * Type-inference strategy with similar effect to {@link #BOOLEAN_NULLABLE},
   * which is more efficient, but can only be used if all arguments are
   * BOOLEAN.
   */
  public static final SqlReturnTypeInference BOOLEAN_NULLABLE_OPTIMIZED =
      opBinding -> {
        // Equivalent to
        //   cascade(ARG0, SqlTypeTransforms.TO_NULLABLE);
        // but implemented by hand because used in AND, which is a very common
        // operator.
        final int n = opBinding.getOperandCount();
        RelDataType type1 = null;
        for (int i = 0; i < n; i++) {
          type1 = opBinding.getOperandType(i);
          if (type1.isNullable()) {
            break;
          }
        }
        return type1;
      };

  /**
   * Type-inference strategy whereby the result type of a call is a nullable
   * Boolean.
   */
  public static final SqlReturnTypeInference BOOLEAN_FORCE_NULLABLE =
      BOOLEAN.andThen(SqlTypeTransforms.FORCE_NULLABLE);

  /**
   * Type-inference strategy whereby the result type of a call is BOOLEAN
   * NOT NULL.
   */
  public static final SqlReturnTypeInference BOOLEAN_NOT_NULL =
      BOOLEAN.andThen(SqlTypeTransforms.TO_NOT_NULLABLE);

  /**
   * Type-inference strategy whereby the result type of a call is DATE.
   */
  public static final SqlReturnTypeInference DATE =
      explicit(SqlTypeName.DATE);

  /**
   * Type-inference strategy whereby the result type of a call is nullable
   * DATE.
   */
  public static final SqlReturnTypeInference DATE_NULLABLE =
      DATE.andThen(SqlTypeTransforms.TO_NULLABLE);

  /**
   * Type-inference strategy whereby the result type of a call is TIME(0).
   */
  public static final SqlReturnTypeInference TIME =
      explicit(SqlTypeName.TIME, 0);

  /**
   * Type-inference strategy whereby the result type of a call is nullable
   * TIME(0).
   */
  public static final SqlReturnTypeInference TIME_NULLABLE =
      TIME.andThen(SqlTypeTransforms.TO_NULLABLE);

  /**
   * Type-inference strategy whereby the result type of a call is TIMESTAMP.
   */
  public static final SqlReturnTypeInference TIMESTAMP =
      explicit(SqlTypeName.TIMESTAMP);

  /**
   * Type-inference strategy whereby the result type of a call is nullable
   * TIMESTAMP.
   */
  public static final SqlReturnTypeInference TIMESTAMP_NULLABLE =
      TIMESTAMP.andThen(SqlTypeTransforms.TO_NULLABLE);

  /**
   * Type-inference strategy whereby the result type of a call is Double.
   */
  public static final SqlReturnTypeInference DOUBLE =
      explicit(SqlTypeName.DOUBLE);

  /**
   * Type-inference strategy whereby the result type of a call is Double with
   * nulls allowed if any of the operands allow nulls.
   */
  public static final SqlReturnTypeInference DOUBLE_NULLABLE =
      DOUBLE.andThen(SqlTypeTransforms.TO_NULLABLE);

  /**
   * Type-inference strategy whereby the result type of a call is a Char.
   */
  public static final SqlReturnTypeInference CHAR =
          explicit(SqlTypeName.CHAR);

  /**
   * Type-inference strategy whereby the result type of a call is a nullable
   * CHAR(1).
   */
  public static final SqlReturnTypeInference CHAR_FORCE_NULLABLE =
      CHAR.andThen(SqlTypeTransforms.FORCE_NULLABLE);

  /**
   * Type-inference strategy whereby the result type of a call is an Integer.
   */
  public static final SqlReturnTypeInference INTEGER =
      explicit(SqlTypeName.INTEGER);

  /**
   * Type-inference strategy whereby the result type of a call is an Integer
   * with nulls allowed if any of the operands allow nulls.
   */
  public static final SqlReturnTypeInference INTEGER_NULLABLE =
      INTEGER.andThen(SqlTypeTransforms.TO_NULLABLE);

  /**
   * Type-inference strategy whereby the result type of a call is a BIGINT.
   */
  public static final SqlReturnTypeInference BIGINT =
      explicit(SqlTypeName.BIGINT);

  /**
   * Type-inference strategy whereby the result type of a call is a nullable
   * BIGINT.
   */
  public static final SqlReturnTypeInference BIGINT_FORCE_NULLABLE =
      BIGINT.andThen(SqlTypeTransforms.FORCE_NULLABLE);

  /**
   * Type-inference strategy whereby the result type of a call is a BIGINT
   * with nulls allowed if any of the operands allow nulls.
   */
  public static final SqlReturnTypeInference BIGINT_NULLABLE =
      BIGINT.andThen(SqlTypeTransforms.TO_NULLABLE);

  /**
   * Type-inference strategy that always returns "VARCHAR(4)".
   */
  public static final SqlReturnTypeInference VARCHAR_4 =
      explicit(SqlTypeName.VARCHAR, 4);

  /**
   * Type-inference strategy that always returns "VARCHAR(4)" with nulls
   * allowed if any of the operands allow nulls.
   */
  public static final SqlReturnTypeInference VARCHAR_4_NULLABLE =
      VARCHAR_4.andThen(SqlTypeTransforms.TO_NULLABLE);

  /**
   * Type-inference strategy that always returns "VARCHAR(2000)".
   */
  public static final SqlReturnTypeInference VARCHAR_2000 =
      explicit(SqlTypeName.VARCHAR, 2000);

  /**
   * Type-inference strategy that always returns "VARCHAR(2000)" with nulls
   * allowed if any of the operands allow nulls.
   */
  public static final SqlReturnTypeInference VARCHAR_2000_NULLABLE =
      VARCHAR_2000.andThen(SqlTypeTransforms.TO_NULLABLE);

  /**
   * Type-inference strategy for Histogram agg support.
   */
  public static final SqlReturnTypeInference HISTOGRAM =
      explicit(SqlTypeName.VARBINARY, 8);

  /**
   * Type-inference strategy that always returns "CURSOR".
   */
  public static final SqlReturnTypeInference CURSOR =
      explicit(SqlTypeName.CURSOR);

  /**
   * Type-inference strategy that always returns "COLUMN_LIST".
   */
  public static final SqlReturnTypeInference COLUMN_LIST =
      explicit(SqlTypeName.COLUMN_LIST);

  /**
   * Type-inference strategy whereby the result type of a call is using its
   * operands biggest type, using the SQL:1999 rules described in "Data types
   * of results of aggregations". These rules are used in union, except,
   * intersect, case and other places.
   *
   * @see Glossary#SQL99 SQL:1999 Part 2 Section 9.3
   */
  public static final SqlReturnTypeInference LEAST_RESTRICTIVE =
      opBinding -> opBinding.getTypeFactory().leastRestrictive(
          opBinding.collectOperandTypes());

  /**
   * Returns the same type as the multiset carries. The multiset type returned
   * is the least restrictive of the call's multiset operands
   */
  public static final SqlReturnTypeInference MULTISET = opBinding -> {
    ExplicitOperatorBinding newBinding =
        new ExplicitOperatorBinding(
            opBinding,
            new AbstractList<RelDataType>() {
              // CHECKSTYLE: IGNORE 12
              @Override public RelDataType get(int index) {
                RelDataType type =
                    opBinding.getOperandType(index)
                        .getComponentType();
                assert type != null;
                return type;
              }

              @Override public int size() {
                return opBinding.getOperandCount();
              }
            });
    RelDataType biggestElementType =
        LEAST_RESTRICTIVE.inferReturnType(newBinding);
    return opBinding.getTypeFactory().createMultisetType(
        requireNonNull(biggestElementType,
            () -> "can't infer element type for multiset of " + newBinding),
        -1);
  };

  /**
   * Returns a MULTISET type.
   *
   * <p>For example, given <code>INTEGER</code>, returns
   * <code>INTEGER MULTISET</code>.
   */
  public static final SqlReturnTypeInference TO_MULTISET =
      ARG0.andThen(SqlTypeTransforms.TO_MULTISET);

  /**
   * Returns the element type of a MULTISET.
   */
  public static final SqlReturnTypeInference MULTISET_ELEMENT_NULLABLE =
      MULTISET.andThen(SqlTypeTransforms.TO_MULTISET_ELEMENT_TYPE);

  /**
   * Same as {@link #MULTISET} but returns with nullability if any of the
   * operands is nullable.
   */
  public static final SqlReturnTypeInference MULTISET_NULLABLE =
      MULTISET.andThen(SqlTypeTransforms.TO_NULLABLE);

  /**
   * Returns the type of the only column of a multiset.
   *
   * <p>For example, given <code>RECORD(x INTEGER) MULTISET</code>, returns
   * <code>INTEGER MULTISET</code>.
   */
  public static final SqlReturnTypeInference MULTISET_PROJECT_ONLY =
      MULTISET.andThen(SqlTypeTransforms.ONLY_COLUMN);

  /**
   * Returns an ARRAY type.
   *
   * <p>For example, given <code>INTEGER</code>, returns
   * <code>INTEGER ARRAY</code>.
   */
  public static final SqlReturnTypeInference TO_ARRAY =
      ARG0.andThen(SqlTypeTransforms.TO_ARRAY);

  /**
   * Returns a MAP type.
   *
   * <p>For example, given {@code Record(f0: INTEGER, f1: DATE)}, returns
   * {@code (INTEGER, DATE) MAP}.
   */
  public static final SqlReturnTypeInference TO_MAP =
      ARG0.andThen(SqlTypeTransforms.TO_MAP);

  /**
   * Type-inference strategy that always returns GEOMETRY.
   */
  public static final SqlReturnTypeInference GEOMETRY =
      explicit(SqlTypeName.GEOMETRY);

  /**
   * Type-inference strategy whereby the result type of a call is
   * {@link #ARG0_INTERVAL_NULLABLE} and {@link #LEAST_RESTRICTIVE}. These rules
   * are used for integer division.
   */
  public static final SqlReturnTypeInference INTEGER_QUOTIENT_NULLABLE =
      ARG0_INTERVAL_NULLABLE.orElse(LEAST_RESTRICTIVE);

  /**
   * Type-inference strategy for a call where the first argument is a decimal.
   * The result type of a call is a decimal with a scale of 0, and the same
   * precision and nullability as the first argument.
   */
  public static final SqlReturnTypeInference DECIMAL_SCALE0 = opBinding -> {
    RelDataType type1 = opBinding.getOperandType(0);
    if (SqlTypeUtil.isDecimal(type1)) {
      if (type1.getScale() == 0) {
        return type1;
      } else {
        int p = type1.getPrecision();
        RelDataType ret;
        ret =
            opBinding.getTypeFactory().createSqlType(
                SqlTypeName.DECIMAL,
                p,
                0);
        if (type1.isNullable()) {
          ret =
              opBinding.getTypeFactory()
                  .createTypeWithNullability(ret, true);
        }
        return ret;
      }
    }
    return null;
  };

  /**
   * Type-inference strategy whereby the result type of a call is
   * {@link #DECIMAL_SCALE0} with a fallback to {@link #ARG0} This rule
   * is used for floor, ceiling.
   */
  public static final SqlReturnTypeInference ARG0_OR_EXACT_NO_SCALE =
      DECIMAL_SCALE0.orElse(ARG0);

  /**
   * Type-inference strategy whereby the result type of a call is the decimal
   * product of two exact numeric operands where at least one of the operands
   * is a decimal.
   */
  public static final SqlReturnTypeInference DECIMAL_PRODUCT = opBinding -> {
    RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
    RelDataType type1 = opBinding.getOperandType(0);
    RelDataType type2 = opBinding.getOperandType(1);
    return typeFactory.getTypeSystem().deriveDecimalMultiplyType(typeFactory, type1, type2);
  };

  /**
   * Same as {@link #DECIMAL_PRODUCT} but returns with nullability if any of
   * the operands is nullable by using
   * {@link org.apache.calcite.sql.type.SqlTypeTransforms#TO_NULLABLE}.
   */
  public static final SqlReturnTypeInference DECIMAL_PRODUCT_NULLABLE =
      DECIMAL_PRODUCT.andThen(SqlTypeTransforms.TO_NULLABLE);

  /**
   * Type-inference strategy whereby the result type of a call is
   * {@link #DECIMAL_PRODUCT_NULLABLE} with a fallback to
   * {@link #ARG0_INTERVAL_NULLABLE}
   * and {@link #LEAST_RESTRICTIVE}.
   * These rules are used for multiplication.
   */
  public static final SqlReturnTypeInference PRODUCT_NULLABLE =
      DECIMAL_PRODUCT_NULLABLE.orElse(ARG0_INTERVAL_NULLABLE)
          .orElse(LEAST_RESTRICTIVE);

  /**
   * Type-inference strategy whereby the result type of a call is the decimal
   * quotient of two exact numeric operands where at least one of the operands
   * is a decimal.
   */
  public static final SqlReturnTypeInference DECIMAL_QUOTIENT = opBinding -> {
    RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
    RelDataType type1 = opBinding.getOperandType(0);
    RelDataType type2 = opBinding.getOperandType(1);
    return typeFactory.getTypeSystem().deriveDecimalDivideType(typeFactory, type1, type2);
  };

  /**
   * Same as {@link #DECIMAL_QUOTIENT} but returns with nullability if any of
   * the operands is nullable by using
   * {@link org.apache.calcite.sql.type.SqlTypeTransforms#TO_NULLABLE}.
   */
  public static final SqlReturnTypeInference DECIMAL_QUOTIENT_NULLABLE =
      DECIMAL_QUOTIENT.andThen(SqlTypeTransforms.TO_NULLABLE);

  /**
   * Type-inference strategy whereby the result type of a call is
   * {@link #DECIMAL_QUOTIENT_NULLABLE} with a fallback to
   * {@link #ARG0_INTERVAL_NULLABLE} and {@link #LEAST_RESTRICTIVE}. These rules
   * are used for division.
   */
  public static final SqlReturnTypeInference QUOTIENT_NULLABLE =
      DECIMAL_QUOTIENT_NULLABLE.orElse(ARG0_INTERVAL_NULLABLE)
          .orElse(LEAST_RESTRICTIVE);

  /**
   * Type-inference strategy whereby the result type of a call is the decimal
   * sum of two exact numeric operands where at least one of the operands is a
   * decimal.
   */
  public static final SqlReturnTypeInference DECIMAL_SUM = opBinding -> {
    RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
    RelDataType type1 = opBinding.getOperandType(0);
    RelDataType type2 = opBinding.getOperandType(1);
    return typeFactory.getTypeSystem().deriveDecimalPlusType(typeFactory, type1, type2);
  };

  /**
   * Same as {@link #DECIMAL_SUM} but returns with nullability if any
   * of the operands is nullable by using
   * {@link org.apache.calcite.sql.type.SqlTypeTransforms#TO_NULLABLE}.
   */
  public static final SqlReturnTypeInference DECIMAL_SUM_NULLABLE =
      DECIMAL_SUM.andThen(SqlTypeTransforms.TO_NULLABLE);

  /**
   * Type-inference strategy whereby the result type of a call is
   * {@link #DECIMAL_SUM_NULLABLE} with a fallback to {@link #LEAST_RESTRICTIVE}
   * These rules are used for addition and subtraction.
   */
  public static final SqlReturnTypeInference NULLABLE_SUM =
      new SqlReturnTypeInferenceChain(DECIMAL_SUM_NULLABLE, LEAST_RESTRICTIVE);

  public static final SqlReturnTypeInference DECIMAL_MOD = opBinding -> {
    RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
    RelDataType type1 = opBinding.getOperandType(0);
    RelDataType type2 = opBinding.getOperandType(1);
    return typeFactory.getTypeSystem().deriveDecimalModType(typeFactory, type1, type2);
  };

  /**
   * Type-inference strategy whereby the result type of a call is the decimal
   * modulus of two exact numeric operands where at least one of the operands is a
   * decimal.
   */
  public static final SqlReturnTypeInference DECIMAL_MOD_NULLABLE =
      DECIMAL_MOD.andThen(SqlTypeTransforms.TO_NULLABLE);

  /**
   * Type-inference strategy whereby the result type of a call is
   * {@link #DECIMAL_MOD_NULLABLE} with a fallback to {@link #ARG1_NULLABLE}
   * These rules are used for modulus.
   */
  public static final SqlReturnTypeInference NULLABLE_MOD =
      DECIMAL_MOD_NULLABLE.orElse(ARG1_NULLABLE);

  /**
   * Type-inference strategy for concatenating two string arguments. The result
   * type of a call is:
   *
   * <ul>
   * <li>the same type as the input types but with the combined length of the
   * two first types</li>
   * <li>if types are of char type the type with the highest coercibility will
   * be used</li>
   * <li>result is varying if either input is; otherwise fixed
   * </ul>
   *
   * <p>Pre-requisites:
   *
   * <ul>
   * <li>input types must be of the same string type
   * <li>types must be comparable without casting
   * </ul>
   */
  public static final SqlReturnTypeInference DYADIC_STRING_SUM_PRECISION =
      opBinding -> {
        final RelDataType argType0 = opBinding.getOperandType(0);
        final RelDataType argType1 = opBinding.getOperandType(1);

        final boolean containsAnyType =
            (argType0.getSqlTypeName() == SqlTypeName.ANY)
                || (argType1.getSqlTypeName() == SqlTypeName.ANY);

        final boolean containsNullType =
            (argType0.getSqlTypeName() == SqlTypeName.NULL)
                || (argType1.getSqlTypeName() == SqlTypeName.NULL);

        if (!containsAnyType
            && !containsNullType
            && !(SqlTypeUtil.inCharOrBinaryFamilies(argType0)
            && SqlTypeUtil.inCharOrBinaryFamilies(argType1))) {
          Preconditions.checkArgument(
              SqlTypeUtil.sameNamedType(argType0, argType1));
        }
        SqlCollation pickedCollation = null;
        if (!containsAnyType
            && !containsNullType
            && SqlTypeUtil.inCharFamily(argType0)) {
          if (!SqlTypeUtil.isCharTypeComparable(
              opBinding.collectOperandTypes().subList(0, 2))) {
            throw opBinding.newError(
                RESOURCE.typeNotComparable(
                    argType0.getFullTypeString(),
                    argType1.getFullTypeString()));
          }

          pickedCollation = requireNonNull(
              SqlCollation.getCoercibilityDyadicOperator(
                  getCollation(argType0), getCollation(argType1)),
              () -> "getCoercibilityDyadicOperator is null for " + argType0 + " and " + argType1);
        }

        // Determine whether result is variable-length
        SqlTypeName typeName =
            argType0.getSqlTypeName();
        if (SqlTypeUtil.isBoundedVariableWidth(argType1)) {
          typeName = argType1.getSqlTypeName();
        }

        RelDataType ret;
        int typePrecision;
        final long x =
            (long) argType0.getPrecision() + (long) argType1.getPrecision();
        final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
        final RelDataTypeSystem typeSystem = typeFactory.getTypeSystem();
        if (argType0.getPrecision() == RelDataType.PRECISION_NOT_SPECIFIED
            || argType1.getPrecision() == RelDataType.PRECISION_NOT_SPECIFIED
            || x > typeSystem.getMaxPrecision(typeName)) {
          typePrecision = RelDataType.PRECISION_NOT_SPECIFIED;
        } else {
          typePrecision = (int) x;
        }

        ret = typeFactory.createSqlType(typeName, typePrecision);
        if (null != pickedCollation) {
          RelDataType pickedType;
          if (getCollation(argType0).equals(pickedCollation)) {
            pickedType = argType0;
          } else if (getCollation(argType1).equals(pickedCollation)) {
            pickedType = argType1;
          } else {
            throw new AssertionError("should never come here, "
                + "argType0=" + argType0 + ", argType1=" + argType1);
          }
          ret =
              typeFactory.createTypeWithCharsetAndCollation(ret,
                  getCharset(pickedType), getCollation(pickedType));
        }
        if (ret.getSqlTypeName() == SqlTypeName.NULL) {
          ret = typeFactory.createTypeWithNullability(
              typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        }
        return ret;
      };

  /**
   * Type-inference strategy for String concatenation.
   * Result is varying if either input is; otherwise fixed.
   * For example,
   *
   * <p>concat(cast('a' as varchar(2)), cast('b' as varchar(3)),cast('c' as varchar(2)))
   * returns varchar(7).</p>
   *
   * <p>concat(cast('a' as varchar), cast('b' as varchar(2), cast('c' as varchar(2))))
   * returns varchar.</p>
   *
   * <p>concat(cast('a' as varchar(65535)), cast('b' as varchar(2)), cast('c' as varchar(2)))
   * returns varchar.</p>
   */
  public static final SqlReturnTypeInference MULTIVALENT_STRING_SUM_PRECISION =
      opBinding -> {
        boolean hasPrecisionNotSpecifiedOperand = false;
        boolean precisionOverflow = false;
        int typePrecision;
        long amount = 0;
        List<RelDataType> operandTypes = opBinding.collectOperandTypes();
        final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
        final RelDataTypeSystem typeSystem = typeFactory.getTypeSystem();
        for (RelDataType operandType: operandTypes) {
          int operandPrecision = operandType.getPrecision();
          amount = (long) operandPrecision + amount;
          if (operandPrecision == RelDataType.PRECISION_NOT_SPECIFIED) {
            hasPrecisionNotSpecifiedOperand = true;
            break;
          }
          if (amount > typeSystem.getMaxPrecision(SqlTypeName.VARCHAR)) {
            precisionOverflow = true;
            break;
          }
        }
        if (hasPrecisionNotSpecifiedOperand || precisionOverflow) {
          typePrecision = RelDataType.PRECISION_NOT_SPECIFIED;
        } else {
          typePrecision = (int) amount;
        }

        return opBinding.getTypeFactory()
            .createSqlType(SqlTypeName.VARCHAR, typePrecision);
      };

  /**
   * Same as {@link #MULTIVALENT_STRING_SUM_PRECISION} and using
   * {@link org.apache.calcite.sql.type.SqlTypeTransforms#TO_NULLABLE}.
   */
  public static final SqlReturnTypeInference MULTIVALENT_STRING_SUM_PRECISION_NULLABLE =
      MULTIVALENT_STRING_SUM_PRECISION.andThen(SqlTypeTransforms.TO_NULLABLE);

  /**
   * Same as {@link #DYADIC_STRING_SUM_PRECISION} and using
   * {@link org.apache.calcite.sql.type.SqlTypeTransforms#TO_NULLABLE},
   * {@link org.apache.calcite.sql.type.SqlTypeTransforms#TO_VARYING}.
   */
  public static final SqlReturnTypeInference DYADIC_STRING_SUM_PRECISION_NULLABLE_VARYING =
      DYADIC_STRING_SUM_PRECISION.andThen(SqlTypeTransforms.TO_NULLABLE)
          .andThen(SqlTypeTransforms.TO_VARYING);

  /**
   * Same as {@link #DYADIC_STRING_SUM_PRECISION} and using
   * {@link org.apache.calcite.sql.type.SqlTypeTransforms#TO_NULLABLE}.
   */
  public static final SqlReturnTypeInference DYADIC_STRING_SUM_PRECISION_NULLABLE =
      DYADIC_STRING_SUM_PRECISION.andThen(SqlTypeTransforms.TO_NULLABLE);

  /**
   * Type-inference strategy where the expression is assumed to be registered
   * as a {@link org.apache.calcite.sql.validate.SqlValidatorNamespace}, and
   * therefore the result type of the call is the type of that namespace.
   */
  public static final SqlReturnTypeInference SCOPE = opBinding -> {
    SqlCallBinding callBinding = (SqlCallBinding) opBinding;
    SqlValidatorNamespace ns = getNamespace(callBinding);
    return ns.getRowType();
  };

  /**
   * Returns a multiset of column #0 of a multiset. For example, given
   * <code>RECORD(x INTEGER, y DATE) MULTISET</code>, returns <code>INTEGER
   * MULTISET</code>.
   */
  public static final SqlReturnTypeInference MULTISET_PROJECT0 = opBinding -> {
    assert opBinding.getOperandCount() == 1;
    final RelDataType recordMultisetType =
        opBinding.getOperandType(0);
    RelDataType multisetType =
        recordMultisetType.getComponentType();
    assert multisetType != null : "expected a multiset type: "
        + recordMultisetType;
    final List<RelDataTypeField> fields =
        multisetType.getFieldList();
    assert fields.size() > 0;
    final RelDataType firstColType = fields.get(0).getType();
    return opBinding.getTypeFactory().createMultisetType(
        firstColType,
        -1);
  };

  /**
   * Returns a multiset of the first column of a multiset. For example, given
   * <code>INTEGER MULTISET</code>, returns <code>RECORD(x INTEGER)
   * MULTISET</code>.
   */
  public static final SqlReturnTypeInference MULTISET_RECORD = opBinding -> {
    assert opBinding.getOperandCount() == 1;
    final RelDataType multisetType = opBinding.getOperandType(0);
    RelDataType componentType = multisetType.getComponentType();
    assert componentType != null : "expected a multiset type: "
        + multisetType;
    final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
    final RelDataType type = typeFactory.builder()
        .add(SqlUtil.deriveAliasFromOrdinal(0), componentType).build();
    return typeFactory.createMultisetType(type, -1);
  };

  /**
   * Returns the field type of a structured type which has only one field. For
   * example, given {@code RECORD(x INTEGER)} returns {@code INTEGER}.
   */
  public static final SqlReturnTypeInference RECORD_TO_SCALAR = opBinding -> {
    assert opBinding.getOperandCount() == 1;

    final RelDataType recordType = opBinding.getOperandType(0);

    boolean isStruct = recordType.isStruct();
    int fieldCount = recordType.getFieldCount();

    assert isStruct && (fieldCount == 1);

    RelDataTypeField fieldType = recordType.getFieldList().get(0);
    assert fieldType != null
        : "expected a record type with one field: "
        + recordType;
    final RelDataType firstColType = fieldType.getType();
    return opBinding.getTypeFactory().createTypeWithNullability(
        firstColType,
        true);
  };

  /**
   * Type-inference strategy for SUM aggregate function inferred from the
   * operand type, and nullable if the call occurs within a "GROUP BY ()"
   * query. E.g. in "select sum(x) as s from empty", s may be null. Also,
   * with the default implementation of RelDataTypeSystem, s has the same
   * type name as x.
   */
  public static final SqlReturnTypeInference AGG_SUM = opBinding -> {
    final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
    final RelDataType type = typeFactory.getTypeSystem()
        .deriveSumType(typeFactory, opBinding.getOperandType(0));
    if (opBinding.getGroupCount() == 0 || opBinding.hasFilter()) {
      return typeFactory.createTypeWithNullability(type, true);
    } else {
      return type;
    }
  };

  /**
   * Type-inference strategy for $SUM0 aggregate function inferred from the
   * operand type. By default the inferred type is identical to the operand
   * type. E.g. in "select $sum0(x) as s from empty", s has the same type as
   * x.
   */
  public static final SqlReturnTypeInference AGG_SUM_EMPTY_IS_ZERO =
      opBinding -> {
        final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
        final RelDataType sumType = typeFactory.getTypeSystem()
            .deriveSumType(typeFactory, opBinding.getOperandType(0));
        // SUM0 should not return null.
        return typeFactory.createTypeWithNullability(sumType, false);
      };

  /**
   * Type-inference strategy for the {@code CUME_DIST} and {@code PERCENT_RANK}
   * aggregate functions.
   */
  public static final SqlReturnTypeInference FRACTIONAL_RANK = opBinding -> {
    final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
    return typeFactory.getTypeSystem().deriveFractionalRankType(typeFactory);
  };

  /**
   * Type-inference strategy for the {@code NTILE}, {@code RANK},
   * {@code DENSE_RANK}, and {@code ROW_NUMBER} aggregate functions.
   */
  public static final SqlReturnTypeInference RANK = opBinding -> {
    final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
    return typeFactory.getTypeSystem().deriveRankType(typeFactory);
  };

  public static final SqlReturnTypeInference AVG_AGG_FUNCTION = opBinding -> {
    final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
    final RelDataType relDataType =
        typeFactory.getTypeSystem().deriveAvgAggType(typeFactory,
            opBinding.getOperandType(0));
    if (opBinding.getGroupCount() == 0 || opBinding.hasFilter()) {
      return typeFactory.createTypeWithNullability(relDataType, true);
    } else {
      return relDataType;
    }
  };

  public static final SqlReturnTypeInference COVAR_REGR_FUNCTION = opBinding -> {
    final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
    final RelDataType relDataType =
        typeFactory.getTypeSystem().deriveCovarType(typeFactory,
            opBinding.getOperandType(0), opBinding.getOperandType(1));
    if (opBinding.getGroupCount() == 0 || opBinding.hasFilter()) {
      return typeFactory.createTypeWithNullability(relDataType, true);
    } else {
      return relDataType;
    }
  };
}
