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
package org.apache.calcite.sql.fun;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * The <code>TIMESTAMPADD</code> function, which adds an interval to a
 * datetime (TIMESTAMP, TIME or DATE).
 *
 * <p>The SQL syntax is
 *
 * <blockquote>
 * <code>TIMESTAMPADD(<i>timestamp interval</i>, <i>quantity</i>,
 * <i>datetime</i>)</code>
 * </blockquote>
 *
 * <p>The interval time unit can one of the following literals:<ul>
 * <li>NANOSECOND (and synonym SQL_TSI_FRAC_SECOND)
 * <li>MICROSECOND (and synonyms SQL_TSI_MICROSECOND, FRAC_SECOND)
 * <li>SECOND (and synonym SQL_TSI_SECOND)
 * <li>MINUTE (and synonym  SQL_TSI_MINUTE)
 * <li>HOUR (and synonym  SQL_TSI_HOUR)
 * <li>DAY (and synonym SQL_TSI_DAY)
 * <li>WEEK (and synonym  SQL_TSI_WEEK)
 * <li>MONTH (and synonym SQL_TSI_MONTH)
 * <li>QUARTER (and synonym SQL_TSI_QUARTER)
 * <li>YEAR (and synonym  SQL_TSI_YEAR)
 * </ul>
 *
 * <p>Returns modified datetime.
 */
public class SqlTimestampAddFunction extends SqlFunction {

  private static final int MILLISECOND_PRECISION = 3;
  private static final int MICROSECOND_PRECISION = 6;

  private static final SqlReturnTypeInference RETURN_TYPE_INFERENCE =
      opBinding -> {
        final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
        return deduceType(typeFactory,
            opBinding.getOperandLiteralValue(0, TimeUnit.class),
            opBinding.getOperandType(1), opBinding.getOperandType(2));
      };

  public static RelDataType deduceType(RelDataTypeFactory typeFactory,
      TimeUnit timeUnit, RelDataType operandType1, RelDataType operandType2) {
    final RelDataType type;
    switch (timeUnit) {
    case HOUR:
    case MINUTE:
    case SECOND:
    case MILLISECOND:
    case MICROSECOND:
      switch (timeUnit) {
      case MILLISECOND:
        type = typeFactory.createSqlType(SqlTypeName.TIMESTAMP,
            MILLISECOND_PRECISION);
        break;
      case MICROSECOND:
        type = typeFactory.createSqlType(SqlTypeName.TIMESTAMP,
            MICROSECOND_PRECISION);
        break;
      default:
        if (operandType2.getSqlTypeName() == SqlTypeName.TIME) {
          type = typeFactory.createSqlType(SqlTypeName.TIME);
        } else {
          type = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
        }
      }
      break;
    default:
      type = operandType2;
    }
    return typeFactory.createTypeWithNullability(type,
        operandType1.isNullable()
            || operandType2.isNullable());
  }

  /** Creates a SqlTimestampAddFunction. */
  SqlTimestampAddFunction() {
    super("TIMESTAMPADD", SqlKind.TIMESTAMP_ADD, RETURN_TYPE_INFERENCE, null,
        OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.INTEGER,
            SqlTypeFamily.DATETIME),
        SqlFunctionCategory.TIMEDATE);
  }
}

// End SqlTimestampAddFunction.java
