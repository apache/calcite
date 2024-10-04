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

import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.RoundingMode;

import static org.apache.calcite.sql.type.SqlTypeName.DEFAULT_INTERVAL_FRACTIONAL_SECOND_PRECISION;
import static org.apache.calcite.sql.type.SqlTypeName.MIN_INTERVAL_START_PRECISION;

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
 *   <tr><td>MIN_NUMERIC_SCALE</td> <td>0</td></tr>
 *   <tr><td>MAX_NUMERIC_PRECISION</td> <td>19</td></tr>
 * </table>
 */
public abstract class RelDataTypeSystemImpl implements RelDataTypeSystem {
  @Override public int getMaxScale(SqlTypeName typeName) {
    switch (typeName) {
    case DECIMAL:
      // from 1.39, this will be 'return 19;'
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
      return RelDataType.SCALE_NOT_SPECIFIED;
    }
  }

  /**
   * Returns the minimum scale (or fractional second precision in the case of
   * intervals) allowed for this type, or {@link RelDataType#SCALE_NOT_SPECIFIED}
   * if precision/length are not applicable for this type.
   *
   * @return Minimum allowed scale
   */
  @Override public int getMinScale(SqlTypeName typeName) {
    switch (typeName) {
    case DECIMAL:
      return 0;
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
      return 0; // MIN_INTERVAL_FRACTIONAL_SECOND_PRECISION;
    default:
      return RelDataType.SCALE_NOT_SPECIFIED;
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
      // from 1.39, this will be 'return getMaxPrecision(typeName);'
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
    case TIME_TZ:
    case DATE:
      return 0; // SQL99 part 2 section 6.1 syntax rule 30
    case TIMESTAMP:
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
    case TIMESTAMP_TZ:
      // farrago supports only 0 (see
      // SqlTypeName.getDefaultPrecision), but it should be 6
      // (microseconds) per SQL99 part 2 section 6.1 syntax rule 30.
      return 0;
    default:
      return RelDataType.PRECISION_NOT_SPECIFIED;
    }
  }

  /** Returns the default scale for this type if supported, otherwise {@link RelDataType#SCALE_NOT_SPECIFIED}
   *  if scale is either unsupported or must be specified explicitly. */
  @Override public int getDefaultScale(SqlTypeName typeName) {
    switch (typeName) {
    case DECIMAL:
      return 0;
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
      return DEFAULT_INTERVAL_FRACTIONAL_SECOND_PRECISION;
    default:
      return RelDataType.SCALE_NOT_SPECIFIED;
    }
  }

  @Override public int getMaxPrecision(SqlTypeName typeName) {
    switch (typeName) {
    case DECIMAL:
      // from 1.39, this will be 'return 19;'
      return getMaxNumericPrecision();
    case VARCHAR:
    case CHAR:
      return 65536;
    case VARBINARY:
    case BINARY:
      return 65536;
    case TIME:
    case TIME_WITH_LOCAL_TIME_ZONE:
    case TIME_TZ:
    case TIMESTAMP:
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
    case TIMESTAMP_TZ:
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

  /**
   * Returns the minimum precision (or length) allowed for this type,
   * or {@link RelDataType#PRECISION_NOT_SPECIFIED}
   * if precision/length are not applicable for this type.
   *
   * @return Minimum allowed precision
   */
  @Override public int getMinPrecision(SqlTypeName typeName) {
    switch (typeName) {
    case DECIMAL:
    case VARCHAR:
    case CHAR:
    case VARBINARY:
    case BINARY:
    case TIME:
    case TIME_WITH_LOCAL_TIME_ZONE:
    case TIME_TZ:
    case TIMESTAMP:
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
    case TIMESTAMP_TZ:
      return 1;
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
      return MIN_INTERVAL_START_PRECISION;
    default:
      return RelDataType.PRECISION_NOT_SPECIFIED;
    }
  }

  @SuppressWarnings("deprecation")
  @Override public int getMaxNumericScale() {
    return 19;
  }

  @SuppressWarnings("deprecation")
  @Override public int getMaxNumericPrecision() {
    return 19;
  }

  @Override public RoundingMode roundingMode() {
    return RoundingMode.DOWN;
  }

  @Override public @Nullable String getLiteral(SqlTypeName typeName, boolean isPrefix) {
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
    case TIMESTAMP_TZ:
      return isPrefix ? "TIMESTAMP WITH TIME ZONE '" : "'";
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
    case TIME_TZ:
      return isPrefix ? "TIME WITH TIME ZONE '" : "'";
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
        && getDefaultPrecision(typeName) != RelDataType.PRECISION_NOT_SPECIFIED) {
      return 10;
    }
    return 0;
  }

  @Override public RelDataType deriveSumType(RelDataTypeFactory typeFactory,
      RelDataType argumentType) {
    if (argumentType instanceof BasicSqlType) {
      SqlTypeName typeName = argumentType.getSqlTypeName();
      if (typeName.allowsPrec()
          && argumentType.getPrecision() != RelDataType.PRECISION_NOT_SPECIFIED) {
        int precision = typeFactory.getTypeSystem().getMaxPrecision(typeName);
        if (typeName.allowsScale()) {
          argumentType =
              typeFactory.createTypeWithNullability(
                  typeFactory.createSqlType(typeName, precision,
                      argumentType.getScale()),
                  argumentType.isNullable());
        } else {
          argumentType =
              typeFactory.createTypeWithNullability(
                  typeFactory.createSqlType(typeName, precision),
                  argumentType.isNullable());
        }
      }
    }
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

  @Override public boolean isSchemaCaseSensitive() {
    return true;
  }

  @Override public boolean shouldConvertRaggedUnionTypesToVarying() {
    return false;
  }

}
