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

  /** Returns the maximum scale of a given type. */
  int getMaxScale(SqlTypeName typeName);

  /**
   * Returns default precision for this type if supported, otherwise -1 if
   * precision is either unsupported or must be specified explicitly.
   *
   * @return Default precision
   */
  int getDefaultPrecision(SqlTypeName typeName);

  /**
   * Returns the maximum precision (or length) allowed for this type, or -1 if
   * precision/length are not applicable for this type.
   *
   * @return Maximum allowed precision
   */
  int getMaxPrecision(SqlTypeName typeName);

  /** Returns the maximum scale of a NUMERIC or DECIMAL type. */
  int getMaxNumericScale();

  /** Returns the maximum precision of a NUMERIC or DECIMAL type. */
  int getMaxNumericPrecision();

  /** Returns the LITERAL string for the type, either PREFIX/SUFFIX. */
  String getLiteral(SqlTypeName typeName, boolean isPrefix);

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
   * @param typeFactory typeFactory used to create output type
   * @param type1 type of the first operand
   * @param type2 type of the second operand
   * @return the result type for a decimal addition.
   */
  default RelDataType deriveDecimalPlusType(RelDataTypeFactory typeFactory,
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
        precision =
                Math.min(
                        precision,
                        getMaxNumericPrecision());
        assert precision > 0;

        return typeFactory.createSqlType(
                SqlTypeName.DECIMAL,
                precision,
                scale);
      }
    }
    return null;
  }

  /**
   * Returns whether a decimal multiplication should be implemented by casting
   * arguments to double values.
   *
   * <p>Pre-condition: <code>createDecimalProduct(type1, type2) != null</code>
   */
  default boolean shouldUseDoubleMultiplication(
      RelDataTypeFactory typeFactory,
      RelDataType type1,
      RelDataType type2) {
    assert deriveDecimalMultiplyType(typeFactory, type1, type2) != null;
    return false;
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
   *   <li>p = p1 + p2)</li>
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
   * @param typeFactory typeFactory used to create output type
   * @param type1 type of the first operand
   * @param type2 type of the second operand
   * @return the result type for a decimal multiplication, or null if decimal
   * multiplication should not be applied to the operands.
   */
  default RelDataType deriveDecimalMultiplyType(RelDataTypeFactory typeFactory,
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
        precision =
                Math.min(
                        precision,
                        getMaxNumericPrecision());

        RelDataType ret;
        ret = typeFactory.createSqlType(
                        SqlTypeName.DECIMAL,
                        precision,
                        scale);

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
   *   <li>s &lt; max(6, s1 + p2 + 1)</li>
   *   <li>p = d + s</li>
   *   </ul>
   * </li>
   * <li>p and s are capped at their maximum values</li>
   * </ul>
   *
   * @see Glossary#SQL2003 SQL:2003 Part 2 Section 6.26
   *
   * @param typeFactory typeFactory used to create output type
   * @param type1 type of the first operand
   * @param type2 type of the second operand
   * @return the result type for a decimal division, or null if decimal
   * division should not be applied to the operands.
   */
  default RelDataType deriveDecimalDivideType(RelDataTypeFactory typeFactory,
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

        final int maxNumericPrecision = getMaxNumericPrecision();
        int dout =
                Math.min(
                        p1 - s1 + s2,
                        maxNumericPrecision);

        int scale = Math.max(6, s1 + p2 + 1);
        scale =
                Math.min(
                        scale,
                        maxNumericPrecision - dout);
        scale = Math.min(scale, getMaxNumericScale());

        int precision = dout + scale;
        assert precision <= maxNumericPrecision;
        assert precision > 0;

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
   * involves at least one decimal operand and requires both operands to have
   * exact numeric types.
   *
   * <p>The default implementation is SQL:2003 compliant: the declared type of
   * the result is the declared type of the second operand (expression divisor).
   *
   * @see Glossary#SQL2003 SQL:2003 Part 2 Section 6.27
   *
   * @param typeFactory typeFactory used to create output type
   * @param type1 type of the first operand
   * @param type2 type of the second operand
   * @return the result type for a decimal modulus, or null if decimal
   * modulus should not be applied to the operands.
   */
  default RelDataType deriveDecimalModType(RelDataTypeFactory typeFactory,
      RelDataType type1, RelDataType type2) {
    if (SqlTypeUtil.isExactNumeric(type1)
            && SqlTypeUtil.isExactNumeric(type2)) {
      if (SqlTypeUtil.isDecimal(type1)
              || SqlTypeUtil.isDecimal(type2)) {
        return type2;
      }
    }
    return null;
  }

}

// End RelDataTypeSystem.java
