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

import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;

/** Default implementation of
 * {@link org.apache.calcite.rel.type.RelDataTypeSystem},
 * providing parameters from the SQL standard.
 *
 * <p>To implement other type systems, create a derived class and override
 * values as needed.
 *
 * <table border='1'>
 *   <caption>Parameter values</caption>
 *   <tr><th>Parameter</th>         <th>Value</th></tr>
 *   <tr><td>MAX_NUMERIC_SCALE</td> <td>19</td></tr>
 * </table>
 */
public abstract class RelDataTypeSystemImpl implements RelDataTypeSystem {
  public int getMaxScale(SqlTypeName typeName) {
    switch (typeName) {
    case DECIMAL:
      return getMaxNumericScale();
    case INTERVAL_YEAR:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_MONTH:
    case INTERVAL_DAY:
    case INTERVAL_DAY_HOUR:
    case INTERVAL_DAY_MINUTE:
    case INTERVAL_DAY_SECOND:
    case INTERVAL_HOUR:
    case INTERVAL_HOUR_MINUTE:
    case INTERVAL_HOUR_SECOND:
    case INTERVAL_MINUTE:
    case INTERVAL_MINUTE_SECOND:
    case INTERVAL_SECOND:
      return SqlTypeName.MAX_INTERVAL_FRACTIONAL_SECOND_PRECISION;
    default:
      return -1;
    }
  }

  @Override public int getDefaultPrecision(SqlTypeName typeName) {
    // Following BasicSqlType precision as the default
    switch (typeName) {
    case CHAR:
    case BINARY:
      return 1;
    case VARCHAR:
    case VARBINARY:
      return RelDataType.PRECISION_NOT_SPECIFIED;
    case DECIMAL:
      return getMaxNumericPrecision();
    case INTERVAL_YEAR:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_MONTH:
    case INTERVAL_DAY:
    case INTERVAL_DAY_HOUR:
    case INTERVAL_DAY_MINUTE:
    case INTERVAL_DAY_SECOND:
    case INTERVAL_HOUR:
    case INTERVAL_HOUR_MINUTE:
    case INTERVAL_HOUR_SECOND:
    case INTERVAL_MINUTE:
    case INTERVAL_MINUTE_SECOND:
    case INTERVAL_SECOND:
      return SqlTypeName.DEFAULT_INTERVAL_START_PRECISION;
    case BOOLEAN:
      return 1;
    case TINYINT:
      return 3;
    case SMALLINT:
      return 5;
    case INTEGER:
      return 10;
    case BIGINT:
      return 19;
    case REAL:
      return 7;
    case FLOAT:
    case DOUBLE:
      return 15;
    case TIME:
    case TIME_WITH_LOCAL_TIME_ZONE:
    case DATE:
      return 0; // SQL99 part 2 section 6.1 syntax rule 30
    case TIMESTAMP:
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      // farrago supports only 0 (see
      // SqlTypeName.getDefaultPrecision), but it should be 6
      // (microseconds) per SQL99 part 2 section 6.1 syntax rule 30.
      return 0;
    default:
      return -1;
    }
  }

  @Override public int getMaxPrecision(SqlTypeName typeName) {
    switch (typeName) {
    case DECIMAL:
      return getMaxNumericPrecision();
    case VARCHAR:
    case CHAR:
      return 65536;
    case VARBINARY:
    case BINARY:
      return 65536;
    case TIME:
    case TIME_WITH_LOCAL_TIME_ZONE:
    case TIMESTAMP:
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      return SqlTypeName.MAX_DATETIME_PRECISION;
    case INTERVAL_YEAR:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_MONTH:
    case INTERVAL_DAY:
    case INTERVAL_DAY_HOUR:
    case INTERVAL_DAY_MINUTE:
    case INTERVAL_DAY_SECOND:
    case INTERVAL_HOUR:
    case INTERVAL_HOUR_MINUTE:
    case INTERVAL_HOUR_SECOND:
    case INTERVAL_MINUTE:
    case INTERVAL_MINUTE_SECOND:
    case INTERVAL_SECOND:
      return SqlTypeName.MAX_INTERVAL_START_PRECISION;
    default:
      return getDefaultPrecision(typeName);
    }
  }

  @Override public int getMaxNumericScale() {
    return 19;
  }

  @Override public int getMaxNumericPrecision() {
    return 19;
  }

  @Override public String getLiteral(SqlTypeName typeName, boolean isPrefix) {
    switch (typeName) {
    case VARBINARY:
    case VARCHAR:
    case CHAR:
      return "'";
    case BINARY:
      return isPrefix ? "x'" : "'";
    case TIMESTAMP:
      return isPrefix ? "TIMESTAMP '" : "'";
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      return isPrefix ? "TIMESTAMP WITH LOCAL TIME ZONE '" : "'";
    case INTERVAL_DAY:
    case INTERVAL_DAY_HOUR:
    case INTERVAL_DAY_MINUTE:
    case INTERVAL_DAY_SECOND:
    case INTERVAL_HOUR:
    case INTERVAL_HOUR_MINUTE:
    case INTERVAL_HOUR_SECOND:
    case INTERVAL_MINUTE:
    case INTERVAL_MINUTE_SECOND:
    case INTERVAL_SECOND:
      return isPrefix ? "INTERVAL '" : "' DAY";
    case INTERVAL_YEAR:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_MONTH:
      return isPrefix ? "INTERVAL '" : "' YEAR TO MONTH";
    case TIME:
      return isPrefix ? "TIME '" : "'";
    case TIME_WITH_LOCAL_TIME_ZONE:
      return isPrefix ? "TIME WITH LOCAL TIME ZONE '" : "'";
    case DATE:
      return isPrefix ? "DATE '" : "'";
    case ARRAY:
      return isPrefix ? "(" : ")";
    default:
      return null;
    }
  }

  @Override public boolean isCaseSensitive(SqlTypeName typeName) {
    switch (typeName) {
    case CHAR:
    case VARCHAR:
      return true;
    default:
      return false;
    }
  }

  @Override public boolean isAutoincrement(SqlTypeName typeName) {
    return false;
  }

  @Override public int getNumTypeRadix(SqlTypeName typeName) {
    if (typeName.getFamily() == SqlTypeFamily.NUMERIC
        && getDefaultPrecision(typeName) != -1) {
      return 10;
    }
    return 0;
  }

  @Override public RelDataType deriveSumType(RelDataTypeFactory typeFactory,
      RelDataType argumentType) {
    return argumentType;
  }

  @Override public RelDataType deriveAvgAggType(RelDataTypeFactory typeFactory,
      RelDataType argumentType) {
    return argumentType;
  }

  @Override public RelDataType deriveCovarType(RelDataTypeFactory typeFactory,
      RelDataType arg0Type, RelDataType arg1Type) {
    return arg0Type;
  }

  @Override public RelDataType deriveFractionalRankType(RelDataTypeFactory typeFactory) {
    return typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(SqlTypeName.DOUBLE), false);
  }

  @Override public RelDataType deriveRankType(RelDataTypeFactory typeFactory) {
    return typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(SqlTypeName.BIGINT), false);
  }

  public boolean isSchemaCaseSensitive() {
    return true;
  }

  public boolean shouldConvertRaggedUnionTypesToVarying() {
    return false;
  }

  public boolean allowExtendedTrim() {
    return false;
  }

  /**
   * Infers the return type of a decimal modulus operation. Decimal modulus
   * involves at least one decimal operand.
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
   * @param typeFactory typeFactory used to create output type
   * @param type1 type of the first operand
   * @param type2 type of the second operand
   * @return the result type for a decimal modulus, or null if decimal
   * modulus should not be applied to the operands.
   */
  @Override public RelDataType deriveDecimalModType(RelDataTypeFactory typeFactory,
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
        // keep consistency with SQL standard
        if (s1 == 0 && s2 == 0) {
          return type2;
        }

        int scale = Math.max(s1, s2);
        assert scale <= getMaxNumericScale();

        int precision = Math.min(p1 - s1, p2 - s2) + Math.max(s1, s2);
        precision = Math.min(precision, getMaxNumericPrecision());
        assert precision > 0;

        return typeFactory.createSqlType(SqlTypeName.DECIMAL,
                precision, scale);
      }
    }
    return null;
  }

}

// End RelDataTypeSystemImpl.java
