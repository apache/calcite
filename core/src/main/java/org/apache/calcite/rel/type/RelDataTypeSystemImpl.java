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
    case DATE:
      return 0; // SQL99 part 2 section 6.1 syntax rule 30
    case TIMESTAMP:
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
    case TIMESTAMP:
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

  @Override public RelDataType deriveSumType(
      RelDataTypeFactory typeFactory, RelDataType argumentType) {
    return argumentType;
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

}

// End RelDataTypeSystemImpl.java
