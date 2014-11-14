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
package org.eigenbase.reltype;

import org.eigenbase.sql.type.SqlTypeName;

/** Default implementation of {@link org.eigenbase.reltype.RelDataTypeSystem},
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
    case INTERVAL_DAY_TIME:
    case INTERVAL_YEAR_MONTH:
      return SqlTypeName.MAX_INTERVAL_FRACTIONAL_SECOND_PRECISION;
    default:
      return -1;
    }
  }

  public int getDefaultPrecision(SqlTypeName typeName) {
    switch (typeName) {
    case CHAR:
    case BINARY:
    case VARCHAR:
    case VARBINARY:
      return 1;
    case TIME:
      return 0;
    case TIMESTAMP:
      // TODO jvs 26-July-2004:  should be 6 for microseconds,
      // but we can't support that yet
      return 0;
    case DECIMAL:
      return getMaxNumericPrecision();
    case INTERVAL_DAY_TIME:
    case INTERVAL_YEAR_MONTH:
      return SqlTypeName.DEFAULT_INTERVAL_START_PRECISION;
    default:
      return -1;
    }
  }

  public int getMaxPrecision(SqlTypeName typeName) {
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
    case INTERVAL_DAY_TIME:
    case INTERVAL_YEAR_MONTH:
      return SqlTypeName.MAX_INTERVAL_START_PRECISION;
    default:
      return -1;
    }
  }

  public int getMaxNumericScale() {
    return 19;
  }

  public int getMaxNumericPrecision() {
    return 19;
  }
}

// End RelDataTypeSystemImpl.java
