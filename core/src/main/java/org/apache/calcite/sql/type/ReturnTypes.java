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
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.util.Glossary;

import com.google.common.base.Preconditions;

import java.util.AbstractList;
import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * A collection of return-type inference strategies.
 */
public abstract class ReturnTypes {
  private ReturnTypes() {
  }

  public static SqlReturnTypeInferenceChain chain(
      SqlReturnTypeInference... rules) {
    return new SqlReturnTypeInferenceChain(rules);
  }

  /** Creates a return-type inference that applies a rule then a sequence of
   * transforms. */
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
      cascade(ARG0, SqlTypeTransforms.TO_NULLABLE,
          SqlTypeTransforms.TO_VARYING);

  /**
   * Type-inference strategy whereby the result type of a call is the type of
   * the operand #0 (0-based). If any of the other operands are nullable the
   * returned type will also be nullable.
   */
  public static final SqlReturnTypeInference ARG0_NULLABLE =
      cascade(ARG0, SqlTypeTransforms.TO_NULLABLE);

  /**
   * Type-inference strategy whereby the result type of a call is the type of
   * the operand #0 (0-based), with nulls always allowed.
   */
  public static final SqlReturnTypeInference ARG0_FORCE_NULLABLE =
      cascade(ARG0, SqlTypeTransforms.FORCE_NULLABLE);

  public static final SqlReturnTypeInference ARG0_INTERVAL =
      new MatchReturnTypeInference(0,
          SqlTypeFamily.DATETIME_INTERVAL.getTypeNames());

  public static final SqlReturnTypeInference ARG0_INTERVAL_NULLABLE =
      cascade(ARG0_INTERVAL, SqlTypeTransforms.TO_NULLABLE);

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
      cascade(ARG1, SqlTypeTransforms.TO_NULLABLE);
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
      cascade(ARG2, SqlTypeTransforms.TO_NULLABLE);
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
      cascade(BOOLEAN, SqlTypeTransforms.TO_NULLABLE);

  /**
   * Type-inference strategy with similar effect to {@link #BOOLEAN_NULLABLE},
   * which is more efficient, but can only be used if all arguments are
   * BOOLEAN.
   */
  public static final SqlReturnTypeInference BOOLEAN_NULLABLE_OPTIMIZED =
      new SqlReturnTypeInference() {
        // Equivalent to
        //   cascade(ARG0, SqlTypeTransforms.TO_NULLABLE);
        // but implemented by hand because used in AND, which is a very common
        // operator.
        public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
          final int n = opBinding.getOperandCount();
          RelDataType type1 = null;
          for (int i = 0; i < n; i++) {
            type1 = opBinding.getOperandType(i);
            if (type1.isNullable()) {
              break;
            }
          }
          return type1;
        }
      };

  /**
   * Type-inference strategy whereby the result type of a call is Boolean
   * not null.
   */
  public static final SqlReturnTypeInference BOOLEAN_NOT_NULL =
      cascade(BOOLEAN, SqlTypeTransforms.TO_NOT_NULLABLE);
  /**
   * Type-inference strategy whereby the result type of a call is Date.
   */
  public static final SqlReturnTypeInference DATE =
      explicit(SqlTypeName.DATE);
  /**
   * Type-inference strategy whereby the result type of a call is Time(0).
   */
  public static final SqlReturnTypeInference TIME =
      explicit(SqlTypeName.TIME, 0);
  /**
   * Type-inference strategy whereby the result type of a call is nullable
   * Time(0).
   */
  public static final SqlReturnTypeInference TIME_NULLABLE =
      cascade(TIME, SqlTypeTransforms.TO_NULLABLE);
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
      cascade(DOUBLE, SqlTypeTransforms.TO_NULLABLE);

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
      cascade(INTEGER, SqlTypeTransforms.TO_NULLABLE);

  /**
   * Type-inference strategy whereby the result type of a call is a Bigint
   */
  public static final SqlReturnTypeInference BIGINT =
      explicit(SqlTypeName.BIGINT);
  /**
   * Type-inference strategy whereby the result type of a call is a nullable
   * Bigint
   */
  public static final SqlReturnTypeInference BIGINT_FORCE_NULLABLE =
      cascade(BIGINT, SqlTypeTransforms.FORCE_NULLABLE);
  /**
   * Type-inference strategy whereby the result type of a call is an Bigint
   * with nulls allowed if any of the operands allow nulls.
   */
  public static final SqlReturnTypeInference BIGINT_NULLABLE =
      cascade(BIGINT, SqlTypeTransforms.TO_NULLABLE);

  /**
   * Type-inference strategy that always returns "VARCHAR(2000)".
   */
  public static final SqlReturnTypeInference VARCHAR_2000 =
      explicit(SqlTypeName.VARCHAR, 2000);
  /**
   * Type-inference strategy for Histogram agg support
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
      new SqlReturnTypeInference() {
        public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
          return opBinding.getTypeFactory().leastRestrictive(
              opBinding.collectOperandTypes());
        }
      };
  /**
   * Returns the same type as the multiset carries. The multiset type returned
   * is the least restrictive of the call's multiset operands
   */
  public static final SqlReturnTypeInference MULTISET =
      new SqlReturnTypeInference() {
        public RelDataType inferReturnType(
            final SqlOperatorBinding opBinding) {
          ExplicitOperatorBinding newBinding =
              new ExplicitOperatorBinding(
                  opBinding,
                  new AbstractList<RelDataType>() {
                    public RelDataType get(int index) {
                      RelDataType type =
                          opBinding.getOperandType(index)
                              .getComponentType();
                      assert type != null;
                      return type;
                    }

                    public int size() {
                      return opBinding.getOperandCount();
                    }
                    // CHECKSTYLE: IGNORE 1
                  });
          RelDataType biggestElementType =
              LEAST_RESTRICTIVE.inferReturnType(newBinding);
          return opBinding.getTypeFactory().createMultisetType(
              biggestElementType,
              -1);
        }
      };

  /**
   * Returns a multiset type.
   *
   * <p>For example, given <code>INTEGER</code>, returns
   * <code>INTEGER MULTISET</code>.
   */
  public static final SqlReturnTypeInference TO_MULTISET =
      cascade(ARG0, SqlTypeTransforms.TO_MULTISET);

  /**
   * Returns the element type of a multiset
   */
  public static final SqlReturnTypeInference MULTISET_ELEMENT_NULLABLE =
      cascade(MULTISET, SqlTypeTransforms.TO_MULTISET_ELEMENT_TYPE);

  /**
   * Same as {@link #MULTISET} but returns with nullability if any of the
   * operands is nullable.
   */
  public static final SqlReturnTypeInference MULTISET_NULLABLE =
      cascade(MULTISET, SqlTypeTransforms.TO_NULLABLE);

  /**
   * Returns the type of the only column of a multiset.
   *
   * <p>For example, given <code>RECORD(x INTEGER) MULTISET</code>, returns
   * <code>INTEGER MULTISET</code>.
   */
  public static final SqlReturnTypeInference MULTISET_PROJECT_ONLY =
      cascade(MULTISET, SqlTypeTransforms.ONLY_COLUMN);

  /**
   * Type-inference strategy whereby the result type of a call is
   * {@link #ARG0_INTERVAL_NULLABLE} and {@link #LEAST_RESTRICTIVE}. These rules
   * are used for integer division.
   */
  public static final SqlReturnTypeInference INTEGER_QUOTIENT_NULLABLE =
      chain(ARG0_INTERVAL_NULLABLE, LEAST_RESTRICTIVE);

  /**
   * Type-inference strategy for a call where the first argument is a decimal.
   * The result type of a call is a decimal with a scale of 0, and the same
   * precision and nullability as the first argument.
   */
  public static final SqlReturnTypeInference DECIMAL_SCALE0 =
      new SqlReturnTypeInference() {
        public RelDataType inferReturnType(
            SqlOperatorBinding opBinding) {
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
        }
      };
  /**
   * Type-inference strategy whereby the result type of a call is
   * {@link #DECIMAL_SCALE0} with a fallback to {@link #ARG0} This rule
   * is used for floor, ceiling.
   */
  public static final SqlReturnTypeInference ARG0_OR_EXACT_NO_SCALE =
      chain(DECIMAL_SCALE0, ARG0);

  /**
   * Type-inference strategy whereby the result type of a call is the decimal
   * product of two exact numeric operands where at least one of the operands
   * is a decimal.
   */
  public static final SqlReturnTypeInference DECIMAL_PRODUCT =
      new SqlReturnTypeInference() {
        public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
          RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
          RelDataType type1 = opBinding.getOperandType(0);
          RelDataType type2 = opBinding.getOperandType(1);
          return typeFactory.createDecimalProduct(type1, type2);
        }
      };
  /**
   * Same as {@link #DECIMAL_PRODUCT} but returns with nullability if any of
   * the operands is nullable by using
   * {@link org.apache.calcite.sql.type.SqlTypeTransforms#TO_NULLABLE}
   */
  public static final SqlReturnTypeInference DECIMAL_PRODUCT_NULLABLE =
      cascade(DECIMAL_PRODUCT, SqlTypeTransforms.TO_NULLABLE);

  /**
   * Type-inference strategy whereby the result type of a call is
   * {@link #DECIMAL_PRODUCT_NULLABLE} with a fallback to
   * {@link #ARG0_INTERVAL_NULLABLE}
   * and {@link #LEAST_RESTRICTIVE}.
   * These rules are used for multiplication.
   */
  public static final SqlReturnTypeInference PRODUCT_NULLABLE =
      chain(DECIMAL_PRODUCT_NULLABLE, ARG0_INTERVAL_NULLABLE,
          LEAST_RESTRICTIVE);

  /**
   * Type-inference strategy whereby the result type of a call is the decimal
   * product of two exact numeric operands where at least one of the operands
   * is a decimal.
   */
  public static final SqlReturnTypeInference DECIMAL_QUOTIENT =
      new SqlReturnTypeInference() {
        public RelDataType inferReturnType(
            SqlOperatorBinding opBinding) {
          RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
          RelDataType type1 = opBinding.getOperandType(0);
          RelDataType type2 = opBinding.getOperandType(1);
          return typeFactory.createDecimalQuotient(type1, type2);
        }
      };
  /**
   * Same as {@link #DECIMAL_QUOTIENT} but returns with nullability if any of
   * the operands is nullable by using
   * {@link org.apache.calcite.sql.type.SqlTypeTransforms#TO_NULLABLE}
   */
  public static final SqlReturnTypeInference DECIMAL_QUOTIENT_NULLABLE =
      cascade(DECIMAL_QUOTIENT, SqlTypeTransforms.TO_NULLABLE);

  /**
   * Type-inference strategy whereby the result type of a call is
   * {@link #DECIMAL_QUOTIENT_NULLABLE} with a fallback to
   * {@link #ARG0_INTERVAL_NULLABLE} and {@link #LEAST_RESTRICTIVE} These rules
   * are used for division.
   */
  public static final SqlReturnTypeInference QUOTIENT_NULLABLE =
      chain(
          DECIMAL_QUOTIENT_NULLABLE, ARG0_INTERVAL_NULLABLE, LEAST_RESTRICTIVE);
  /**
   * Type-inference strategy whereby the result type of a call is the decimal
   * sum of two exact numeric operands where at least one of the operands is a
   * decimal. Let p1, s1 be the precision and scale of the first operand Let
   * p2, s2 be the precision and scale of the second operand Let p, s be the
   * precision and scale of the result, Then the result type is a decimal
   * with:
   *
   * <ul>
   * <li>s = max(s1, s2)</li>
   * <li>p = max(p1 - s1, p2 - s2) + s + 1</li>
   * </ul>
   *
   * <p>p and s are capped at their maximum values
   *
   * @see Glossary#SQL2003 SQL:2003 Part 2 Section 6.26
   */
  public static final SqlReturnTypeInference DECIMAL_SUM =
      new SqlReturnTypeInference() {
        public RelDataType inferReturnType(
            SqlOperatorBinding opBinding) {
          RelDataType type1 = opBinding.getOperandType(0);
          RelDataType type2 = opBinding.getOperandType(1);
          if (SqlTypeUtil.isExactNumeric(type1)
              && SqlTypeUtil.isExactNumeric(type2)) {
            if (SqlTypeUtil.isDecimal(type1)
                || SqlTypeUtil.isDecimal(type2)) {
              int p1 = type1.getPrecision();
              int p2 = type2.getPrecision();
              int s1 = type1.getScale();
              int s2 = type2.getScale();

              final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
              int scale = Math.max(s1, s2);
              final RelDataTypeSystem typeSystem = typeFactory.getTypeSystem();
              assert scale <= typeSystem.getMaxNumericScale();
              int precision = Math.max(p1 - s1, p2 - s2) + scale + 1;
              precision =
                  Math.min(
                      precision,
                      typeSystem.getMaxNumericPrecision());
              assert precision > 0;

              return typeFactory.createSqlType(
                  SqlTypeName.DECIMAL,
                  precision,
                  scale);
            }
          }

          return null;
        }
      };
  /**
   * Same as {@link #DECIMAL_SUM} but returns with nullability if any
   * of the operands is nullable by using
   * {@link org.apache.calcite.sql.type.SqlTypeTransforms#TO_NULLABLE}.
   */
  public static final SqlReturnTypeInference DECIMAL_SUM_NULLABLE =
      cascade(DECIMAL_SUM, SqlTypeTransforms.TO_NULLABLE);

  /**
   * Type-inference strategy whereby the result type of a call is
   * {@link #DECIMAL_SUM_NULLABLE} with a fallback to {@link #LEAST_RESTRICTIVE}
   * These rules are used for addition and subtraction.
   */
  public static final SqlReturnTypeInference NULLABLE_SUM =
      new SqlReturnTypeInferenceChain(DECIMAL_SUM_NULLABLE, LEAST_RESTRICTIVE);

  /**
   * Type-inference strategy whereby the result type of a call is
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
      new SqlReturnTypeInference() {
        public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
          final RelDataType argType0 = opBinding.getOperandType(0);
          final RelDataType argType1 = opBinding.getOperandType(1);

          final boolean containsAnyType =
              (argType0.getSqlTypeName() == SqlTypeName.ANY)
                  || (argType1.getSqlTypeName() == SqlTypeName.ANY);

          if (!containsAnyType
              && !(SqlTypeUtil.inCharOrBinaryFamilies(argType0)
                  && SqlTypeUtil.inCharOrBinaryFamilies(argType1))) {
            Preconditions.checkArgument(
                SqlTypeUtil.sameNamedType(argType0, argType1));
          }
          SqlCollation pickedCollation = null;
          if (!containsAnyType
              && SqlTypeUtil.inCharFamily(argType0)) {
            if (!SqlTypeUtil.isCharTypeComparable(
                opBinding.collectOperandTypes().subList(0, 2))) {
              throw opBinding.newError(
                  RESOURCE.typeNotComparable(
                      argType0.getFullTypeString(),
                      argType1.getFullTypeString()));
            }

            pickedCollation =
                SqlCollation.getCoercibilityDyadicOperator(
                    argType0.getCollation(), argType1.getCollation());
            assert null != pickedCollation;
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
            if (argType0.getCollation().equals(pickedCollation)) {
              pickedType = argType0;
            } else if (argType1.getCollation().equals(pickedCollation)) {
              pickedType = argType1;
            } else {
              throw new AssertionError("should never come here");
            }
            ret =
                typeFactory.createTypeWithCharsetAndCollation(ret,
                    pickedType.getCharset(), pickedType.getCollation());
          }
          return ret;
        }
      };

  /**
   * Same as {@link #DYADIC_STRING_SUM_PRECISION} and using
   * {@link org.apache.calcite.sql.type.SqlTypeTransforms#TO_NULLABLE},
   * {@link org.apache.calcite.sql.type.SqlTypeTransforms#TO_VARYING}.
   */
  public static final SqlReturnTypeInference
  DYADIC_STRING_SUM_PRECISION_NULLABLE_VARYING =
      cascade(DYADIC_STRING_SUM_PRECISION, SqlTypeTransforms.TO_NULLABLE,
          SqlTypeTransforms.TO_VARYING);

  /**
   * Same as {@link #DYADIC_STRING_SUM_PRECISION} and using
   * {@link org.apache.calcite.sql.type.SqlTypeTransforms#TO_NULLABLE}
   */
  public static final SqlReturnTypeInference
  DYADIC_STRING_SUM_PRECISION_NULLABLE =
      cascade(DYADIC_STRING_SUM_PRECISION, SqlTypeTransforms.TO_NULLABLE);

  /**
   * Type-inference strategy where the expression is assumed to be registered
   * as a {@link org.apache.calcite.sql.validate.SqlValidatorNamespace}, and
   * therefore the result type of the call is the type of that namespace.
   */
  public static final SqlReturnTypeInference SCOPE =
      new SqlReturnTypeInference() {
        public RelDataType inferReturnType(
            SqlOperatorBinding opBinding) {
          SqlCallBinding callBinding = (SqlCallBinding) opBinding;
          return callBinding.getValidator().getNamespace(
              callBinding.getCall()).getRowType();
        }
      };

  /**
   * Returns a multiset of column #0 of a multiset. For example, given
   * <code>RECORD(x INTEGER, y DATE) MULTISET</code>, returns <code>INTEGER
   * MULTISET</code>.
   */
  public static final SqlReturnTypeInference MULTISET_PROJECT0 =
      new SqlReturnTypeInference() {
        public RelDataType inferReturnType(
            SqlOperatorBinding opBinding) {
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
        }
      };
  /**
   * Returns a multiset of the first column of a multiset. For example, given
   * <code>INTEGER MULTISET</code>, returns <code>RECORD(x INTEGER)
   * MULTISET</code>.
   */
  public static final SqlReturnTypeInference MULTISET_RECORD =
      new SqlReturnTypeInference() {
        public RelDataType inferReturnType(
            SqlOperatorBinding opBinding) {
          assert opBinding.getOperandCount() == 1;
          final RelDataType multisetType = opBinding.getOperandType(0);
          RelDataType componentType = multisetType.getComponentType();
          assert componentType != null : "expected a multiset type: "
              + multisetType;
          final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
          final RelDataType type = typeFactory.builder()
              .add(SqlUtil.deriveAliasFromOrdinal(0), componentType).build();
          return typeFactory.createMultisetType(type, -1);
        }
      };
  /**
   * Returns the field type of a structured type which has only one field. For
   * example, given {@code RECORD(x INTEGER)} returns {@code INTEGER}.
   */
  public static final SqlReturnTypeInference RECORD_TO_SCALAR =
      new SqlReturnTypeInference() {
        public RelDataType inferReturnType(
            SqlOperatorBinding opBinding) {
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
        }
      };

  /**
   * Type-inference strategy for SUM aggregate function inferred from the
   * operand type, and nullable if the call occurs within a "GROUP BY ()"
   * query. E.g. in "select sum(x) as s from empty", s may be null. Also,
   * with the default implementation of RelDataTypeSystem, s has the same
   * type name as x.
   */
  public static final SqlReturnTypeInference AGG_SUM =
      new SqlReturnTypeInference() {
        @Override public RelDataType
        inferReturnType(SqlOperatorBinding opBinding) {
          final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
          final RelDataType type = typeFactory.getTypeSystem()
              .deriveSumType(typeFactory, opBinding.getOperandType(0));
          if (opBinding.getGroupCount() == 0 || opBinding.hasFilter()) {
            return typeFactory.createTypeWithNullability(type, true);
          } else {
            return type;
          }
        }
      };

  /**
   * Type-inference strategy for $SUM0 aggregate function inferred from the
   * operand type. By default the inferred type is identical to the operand
   * type. E.g. in "select $sum0(x) as s from empty", s has the same type as
   * x.
   */
  public static final SqlReturnTypeInference AGG_SUM_EMPTY_IS_ZERO =
      new SqlReturnTypeInference() {
        @Override public RelDataType
        inferReturnType(SqlOperatorBinding opBinding) {
          final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
          return typeFactory.getTypeSystem()
              .deriveSumType(typeFactory, opBinding.getOperandType(0));
        }
      };

  /**
   * Type-inference strategy for the {@code CUME_DIST} and {@code PERCENT_RANK}
   * aggregate functions.
   */
  public static final SqlReturnTypeInference FRACTIONAL_RANK =
      new SqlReturnTypeInference() {
        @Override public RelDataType
        inferReturnType(SqlOperatorBinding opBinding) {
          final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
          return typeFactory.getTypeSystem().deriveFractionalRankType(typeFactory);
        }
      };

  /**
   * Type-inference strategy for the {@code NTILE}, {@code RANK},
   * {@code DENSE_RANK}, and {@code ROW_NUMBER} aggregate functions.
   */
  public static final SqlReturnTypeInference RANK =
      new SqlReturnTypeInference() {
        @Override public RelDataType
        inferReturnType(SqlOperatorBinding opBinding) {
          final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
          return typeFactory.getTypeSystem().deriveRankType(typeFactory);
        }
      };
}

// End ReturnTypes.java
