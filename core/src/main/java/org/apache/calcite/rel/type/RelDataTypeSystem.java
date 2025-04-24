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
package org.apache.calcite.rel.type;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.Glossary;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.RoundingMode;

/**
 * Type system.
 *
 * <p>Provides behaviors concerning type limits and behaviors. For example,
 * in the default system, a DECIMAL can have maximum precision 19, but Hive
 * overrides to 38.
 *
 * <p>The default implementation is {@link #DEFAULT}.
 */
public interface RelDataTypeSystem {
  /** Default type system. */
  RelDataTypeSystem DEFAULT = new RelDataTypeSystemImpl() { };

  /**
   * Returns the maximum scale allowed for this type, or
   * {@link RelDataType#SCALE_NOT_SPECIFIED}
   * if scale is not applicable for this type.
   *
   * @return Maximum allowed scale
   */
  int getMaxScale(SqlTypeName typeName);

  /**
   * Returns the minimum scale allowed for this type, or
   * {@link RelDataType#SCALE_NOT_SPECIFIED}
   * if scale are not applicable for this type.
   *
   * @return Minimum allowed scale
   */
  int getMinScale(SqlTypeName typeName);

  /**
   * Returns default precision for this type if supported, otherwise
   * {@link RelDataType#PRECISION_NOT_SPECIFIED}
   * if precision is either unsupported or must be specified explicitly.
   *
   * @return Default precision
   */
  int getDefaultPrecision(SqlTypeName typeName);

  /**
   * Returns default scale for this type if supported, otherwise
   * {@link RelDataType#SCALE_NOT_SPECIFIED}
   * if scale is either unsupported or must be specified explicitly.
   *
   * @return Default scale
   */
  int getDefaultScale(SqlTypeName typeName);

  /**
   * Returns the maximum precision (or length) allowed for this type, or
   * {@link RelDataType#PRECISION_NOT_SPECIFIED}
   * if precision/length are not applicable for this type.
   *
   * @return Maximum allowed precision
   */
  int getMaxPrecision(SqlTypeName typeName);

  /**
   * Returns the minimum precision (or length) allowed for this type, or
   * {@link RelDataType#PRECISION_NOT_SPECIFIED}
   * if precision/length are not applicable for this type.
   *
   * @return Minimum allowed precision
   */
  int getMinPrecision(SqlTypeName typeName);

  /** Returns the maximum scale of a NUMERIC or DECIMAL type.
   * Default value is 19.
   *
   * @deprecated Replaced by {@link #getMaxScale}(DECIMAL).
   *
   * <p>From Calcite release 1.38 onwards, instead of calling this method, you
   * should call {@code getMaxScale(DECIMAL)}.
   *
   * <p>In Calcite release 1.38, if you wish to change the maximum
   * scale of {@link SqlTypeName#DECIMAL} values, you should do two things:
   *
   * <ul>
   * <li>Override the {@link #getMaxScale(SqlTypeName)} method,
   *     changing its behavior for {@code DECIMAL};
   * <li>Make sure that the implementation of your
   *     {@code #getMaxNumericScale} method calls
   *     {@code getMaxScale(DECIMAL)}.
   * </ul>
   *
   * <p>In Calcite release 1.39, Calcite will cease calling this method,
   * and will remove the override of the method in
   * {@link RelDataTypeSystemImpl}. You should remove all calls to
   * and overrides of this method. */
  @Deprecated // calcite will cease calling in 1.39, and removed before 2.0
  default int getMaxNumericScale() {
    return 19;
  }

  /** Returns the maximum precision of a NUMERIC or DECIMAL type.
   * Default value is 19.
   *
   * @deprecated Replaced by {@link #getMaxScale}(DECIMAL).
   *
   * <p>From Calcite release 1.38 onwards, instead of calling this method, you
   * should call {@code getMaxPrecision(DECIMAL)}.
   *
   * <p>In Calcite release 1.38, if you wish to change the maximum
   * precision of {@link SqlTypeName#DECIMAL} values, you should do two things:
   *
   * <ul>
   * <li>Override the {@link #getMaxPrecision(SqlTypeName)} method,
   *     changing its behavior for {@code DECIMAL};
   * <li>Make sure that the implementation of your
   *     {@code #getMaxNumericPrecision} method calls
   *     {@code getMaxPrecision(DECIMAL)}.
   * </ul>
   *
   * <p>In Calcite release 1.39, Calcite will cease calling this method,
   * and will remove the override of the method in
   * {@link RelDataTypeSystemImpl}. You should remove all calls to
   * and overrides of this method. */
  @Deprecated // calcite will cease calling in 1.39, and removed before 2.0
  default int getMaxNumericPrecision() {
    return getMaxPrecision(SqlTypeName.DECIMAL);
  }

  /** Returns the rounding behavior for numerical operations capable of discarding precision. */
  RoundingMode roundingMode();

  /** Returns whether a MAP type is allowed to have nullable keys. */
  default boolean mapKeysCanBeNullable() {
    return false;
  }

  /** Returns the LITERAL string for the type, either PREFIX/SUFFIX. */
  @Nullable String getLiteral(SqlTypeName typeName, boolean isPrefix);

  /** Returns whether the type is case sensitive. */
  boolean isCaseSensitive(SqlTypeName typeName);

  /** Returns whether the type can be auto increment. */
  boolean isAutoincrement(SqlTypeName typeName);

  /** Returns the numeric type radix, typically 2 or 10.
   * 0 means "not applicable". */
  int getNumTypeRadix(SqlTypeName typeName);

  /** Returns the return type of a call to the {@code SUM} aggregate function,
   * inferred from its argument type. */
  RelDataType deriveSumType(RelDataTypeFactory typeFactory,
      RelDataType argumentType);

  /** Returns the return type of a call to the {@code AVG}, {@code STDDEV} or
   * {@code VAR} aggregate functions, inferred from its argument type.
   */
  RelDataType deriveAvgAggType(RelDataTypeFactory typeFactory,
      RelDataType argumentType);

  /** Returns the return type of a call to the {@code COVAR} aggregate function,
   * inferred from its argument types. */
  RelDataType deriveCovarType(RelDataTypeFactory typeFactory,
      RelDataType arg0Type, RelDataType arg1Type);

  /** Returns the return type of the {@code CUME_DIST} and {@code PERCENT_RANK}
   * aggregate functions. */
  RelDataType deriveFractionalRankType(RelDataTypeFactory typeFactory);

  /** Returns the return type of the {@code NTILE}, {@code RANK},
   * {@code DENSE_RANK}, and {@code ROW_NUMBER} aggregate functions. */
  RelDataType deriveRankType(RelDataTypeFactory typeFactory);

  /** Whether two record types are considered distinct if their field names
   * are the same but in different cases. */
  boolean isSchemaCaseSensitive();

  /** Whether the least restrictive type of a number of CHAR types of different
   * lengths should be a VARCHAR type. And similarly BINARY to VARBINARY. */
  boolean shouldConvertRaggedUnionTypesToVarying();

  /**
   * Returns whether a decimal multiplication should be implemented by casting
   * arguments to double values.
   *
   * <p>Pre-condition: <code>createDecimalProduct(type1, type2) != null</code>
   */
  default boolean shouldUseDoubleMultiplication(RelDataTypeFactory typeFactory,
      RelDataType type1, RelDataType type2) {
    assert deriveDecimalMultiplyType(typeFactory, type1, type2) != null;
    return false;
  }

  /**
   * Infers the return type of a decimal addition. Decimal addition involves
   * at least one decimal operand and requires both operands to have exact
   * numeric types.
   *
   * <p>Rules:
   *
   * <ul>
   * <li>Let p1, s1 be the precision and scale of the first operand</li>
   * <li>Let p2, s2 be the precision and scale of the second operand</li>
   * <li>Let p, s be the precision and scale of the result</li>
   * <li>Let d be the number of whole digits in the result</li>
   * <li>Then the result type is a decimal with:
   *   <ul>
   *   <li>s = max(s1, s2)</li>
   *   <li>p = max(p1 - s1, p2 - s2) + s + 1</li>
   *   </ul>
   * </li>
   * <li>p and s are capped at their maximum values</li>
   * </ul>
   *
   * @see Glossary#SQL2003 SQL:2003 Part 2 Section 6.26
   *
   * @param typeFactory TypeFactory used to create output type
   * @param type1       Type of the first operand
   * @param type2       Type of the second operand
   * @return Result type for a decimal addition
   */
  default @Nullable RelDataType deriveDecimalPlusType(RelDataTypeFactory typeFactory,
      RelDataType type1, RelDataType type2) {
    if (SqlTypeUtil.isExactNumeric(type1)
        && SqlTypeUtil.isExactNumeric(type2)) {
      if (SqlTypeUtil.isDecimal(type1)
          || SqlTypeUtil.isDecimal(type2)) {
        // Java numeric will always have invalid precision/scale,
        // use its default decimal precision/scale instead.
        type1 = RelDataTypeFactoryImpl.isJavaType(type1)
            ? typeFactory.decimalOf(type1)
            : type1;
        type2 = RelDataTypeFactoryImpl.isJavaType(type2)
            ? typeFactory.decimalOf(type2)
            : type2;
        int p1 = type1.getPrecision();
        int p2 = type2.getPrecision();
        int s1 = type1.getScale();
        int s2 = type2.getScale();
        int scale = Math.max(s1, s2);
        assert scale <= getMaxNumericScale();
        int precision = Math.max(p1 - s1, p2 - s2) + scale + 1;
        precision = Math.min(precision, getMaxNumericPrecision());
        assert precision > 0;

        return typeFactory.createSqlType(SqlTypeName.DECIMAL, precision, scale);
      }
    }
    return null;
  }

  /**
   * Infers the return type of a decimal multiplication. Decimal
   * multiplication involves at least one decimal operand and requires both
   * operands to have exact numeric types.
   *
   * <p>The default implementation is SQL:2003 compliant.
   *
   * <p>Rules:
   *
   * <ul>
   * <li>Let p1, s1 be the precision and scale of the first operand</li>
   * <li>Let p2, s2 be the precision and scale of the second operand</li>
   * <li>Let p, s be the precision and scale of the result</li>
   * <li>Let d be the number of whole digits in the result</li>
   * <li>Then the result type is a decimal with:
   *   <ul>
   *   <li>p = p1 + p2</li>
   *   <li>s = s1 + s2</li>
   *   </ul>
   * </li>
   * <li>p and s are capped at their maximum values</li>
   * </ul>
   *
   * <p>p and s are capped at their maximum values
   *
   * @see Glossary#SQL2003 SQL:2003 Part 2 Section 6.26
   *
   * @param typeFactory TypeFactory used to create output type
   * @param type1       Type of the first operand
   * @param type2       Type of the second operand
   * @return Result type for a decimal multiplication, or null if decimal
   * multiplication should not be applied to the operands
   */
  default @Nullable RelDataType deriveDecimalMultiplyType(RelDataTypeFactory typeFactory,
      RelDataType type1, RelDataType type2) {
    if (SqlTypeUtil.isExactNumeric(type1)
        && SqlTypeUtil.isExactNumeric(type2)) {
      if (SqlTypeUtil.isDecimal(type1)
          || SqlTypeUtil.isDecimal(type2)) {
        // Java numeric will always have invalid precision/scale,
        // use its default decimal precision/scale instead.
        type1 = RelDataTypeFactoryImpl.isJavaType(type1)
            ? typeFactory.decimalOf(type1)
            : type1;
        type2 = RelDataTypeFactoryImpl.isJavaType(type2)
            ? typeFactory.decimalOf(type2)
            : type2;
        int p1 = type1.getPrecision();
        int p2 = type2.getPrecision();
        int s1 = type1.getScale();
        int s2 = type2.getScale();

        int scale = s1 + s2;
        scale = Math.min(scale, getMaxNumericScale());
        int precision = p1 + p2;
        precision = Math.min(precision, getMaxNumericPrecision());

        RelDataType ret;
        ret = typeFactory.createSqlType(SqlTypeName.DECIMAL, precision, scale);

        return ret;
      }
    }

    return null;
  }

  /**
   * Infers the return type of a decimal division. Decimal division involves
   * at least one decimal operand and requires both operands to have exact
   * numeric types.
   *
   * <p>The default implementation is SQL:2003 compliant.
   *
   * <p>Rules:
   *
   * <ul>
   * <li>Let p1, s1 be the precision and scale of the first operand</li>
   * <li>Let p2, s2 be the precision and scale of the second operand</li>
   * <li>Let p, s be the precision and scale of the result</li>
   * <li>Let d be the number of whole digits in the result</li>
   * <li>Then the result type is a decimal with:
   *   <ul>
   *   <li>d = p1 - s1 + s2</li>
   *   <li>s = max(6, s1 + p2 + 1)</li>
   *   <li>p = d + s</li>
   *   </ul>
   * </li>
   * <li>p and s are capped at their maximum values</li>
   * </ul>
   *
   * @see Glossary#SQL2003 SQL:2003 Part 2 Section 6.26
   *
   * @param typeFactory TypeFactory used to create output type
   * @param type1       Type of the first operand
   * @param type2       Type of the second operand
   * @return Result type for a decimal division, or null if decimal
   * division should not be applied to the operands
   */
  default @Nullable RelDataType deriveDecimalDivideType(RelDataTypeFactory typeFactory,
      RelDataType type1, RelDataType type2) {

    if (SqlTypeUtil.isExactNumeric(type1)
        && SqlTypeUtil.isExactNumeric(type2)) {
      if (SqlTypeUtil.isDecimal(type1)
          || SqlTypeUtil.isDecimal(type2)) {
        // Java numeric will always have invalid precision/scale,
        // use its default decimal precision/scale instead.
        type1 = RelDataTypeFactoryImpl.isJavaType(type1)
            ? typeFactory.decimalOf(type1)
            : type1;
        type2 = RelDataTypeFactoryImpl.isJavaType(type2)
            ? typeFactory.decimalOf(type2)
            : type2;
        int p1 = type1.getPrecision();
        int p2 = type2.getPrecision();
        int s1 = type1.getScale();
        int s2 = type2.getScale();

        final int maxScale = getMaxNumericScale();
        int six = Math.min(6, maxScale);
        int d = p1 - s1 + s2;
        int scale = Math.max(six, s1 + p2 + 1);
        scale = Math.min(scale, maxScale);
        int precision = d + scale;

  // Rules from
  // https://learn.microsoft.com/en-us/sql/t-sql/data-types/precision-scale-and-length-transact-sql
        // Rules reproduced here in case the web page goes away.
        // Note that in SQL server getMaxNumericPrecision() == 38 and
        // getMaxNumericScale() > 6.
        // In multiplication and division operations, we need precision - scale places to store
        // the integral part of the result. The scale might be reduced using the following rules:
        //
        // - The resulting scale is reduced to min(scale, 38 - (precision-scale))
        //   if the integral part is less than 32, because it can't be greater than
        //   38 - (precision-scale). The result might be rounded in this case.
        // - The scale isn't changed if it's less than 6 and if the integral part
        //   is greater than 32. In this case, an overflow error might be raised if
        //   it can't fit into decimal(38, scale).
        // - The scale is set to 6 if it's greater than 6 and if the integral part
        //   is greater than 32. In this case, both the integral part and scale would be
        //   reduced and resulting type is decimal(38, 6). The result might be rounded to
        //   7 decimal places, or the overflow error is thrown if the integral part
        //   can't fit into 32 digits.
        final int maxPrecision = getMaxNumericPrecision();
        int bound = maxPrecision - six;  // This was '32' in the MS documentation
        if (precision <= bound) {
          scale = Math.min(scale, maxPrecision - (precision - scale));
        } else {
          // precision > bound
          scale = Math.min(six, scale);
        }

        precision = Math.min(precision, maxPrecision);
        assert precision > 0;
        assert scale <= maxScale;

        RelDataType ret;
        ret = typeFactory.
            createSqlType(
            SqlTypeName.DECIMAL,
            precision,
            scale);

        return ret;
      }
    }

    return null;
  }

  /**
   * Infers the return type of a decimal modulus operation. Decimal modulus
   * involves at least one decimal operand.
   *
   * <p>The default implementation is SQL:2003 compliant: the declared type of
   * the result is the declared type of the second operand (expression divisor).
   *
   * @see Glossary#SQL2003 SQL:2003 Part 2 Section 6.27
   *
   * <p>Rules:
   *
   * <ul>
   * <li>Let p1, s1 be the precision and scale of the first operand</li>
   * <li>Let p2, s2 be the precision and scale of the second operand</li>
   * <li>Let p, s be the precision and scale of the result</li>
   * <li>Let d be the number of whole digits in the result</li>
   * <li>Then the result type is a decimal with:
   *   <ul>
   *   <li>s = max(s1, s2)</li>
   *   <li>p = min(p1 - s1, p2 - s2) + max(s1, s2)</li>
   *   </ul>
   * </li>
   * <li>p and s are capped at their maximum values</li>
   * </ul>
   *
   * @param typeFactory TypeFactory used to create output type
   * @param type1       Type of the first operand
   * @param type2       Type of the second operand
   * @return Result type for a decimal modulus, or null if decimal
   * modulus should not be applied to the operands
   */
  default @Nullable RelDataType deriveDecimalModType(RelDataTypeFactory typeFactory,
      RelDataType type1, RelDataType type2) {
    if (SqlTypeUtil.isExactNumeric(type1)
        && SqlTypeUtil.isExactNumeric(type2)) {
      if (SqlTypeUtil.isDecimal(type1)
          || SqlTypeUtil.isDecimal(type2)) {
        // Java numeric will always have invalid precision/scale,
        // use its default decimal precision/scale instead.
        type1 = RelDataTypeFactoryImpl.isJavaType(type1)
            ? typeFactory.decimalOf(type1)
            : type1;
        type2 = RelDataTypeFactoryImpl.isJavaType(type2)
            ? typeFactory.decimalOf(type2)
            : type2;
        int p1 = type1.getPrecision();
        int p2 = type2.getPrecision();
        int s1 = type1.getScale();
        int s2 = type2.getScale();
        // Keep consistency with SQL standard.
        if (s1 == 0 && s2 == 0) {
          return type2;
        }

        final int maxScale = getMaxNumericScale();
        final int maxPrecision = getMaxNumericPrecision();

        final int scale = Math.min(Math.max(s1, s2), maxScale);
        int precision = Math.min(p1 - s1, p2 - s2) + Math.max(s1, s2);
        precision = Math.min(precision, maxPrecision);
        assert precision > 0;

        return typeFactory.createSqlType(SqlTypeName.DECIMAL,
            precision, scale);
      }
    }
    return null;
  }

  /** Returns a list of supported time frames.
   *
   * <p>The validator calls this method with {@link TimeFrames#CORE} as an
   * argument, and the default implementation of this method returns its input,
   * and therefore {@link TimeFrames#CORE CORE} is the default time frame set.
   *
   * <p>If you wish to use a custom time frame set, create an instance of
   * {@code RelDataTypeSystem} that overrides this method. Your method should
   * call {@link TimeFrameSet#builder()}, will probably add all or most of the
   * time frames in the {@code frameSet} argument, and then call
   * {@link TimeFrameSet.Builder#build()}.
   *
   * @param frameSet Set of built-in time frames
   */
  default TimeFrameSet deriveTimeFrameSet(TimeFrameSet frameSet) {
    return frameSet;
  }
}
