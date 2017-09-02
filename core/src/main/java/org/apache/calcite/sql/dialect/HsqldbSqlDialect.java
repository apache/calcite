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
package org.apache.calcite.sql.dialect;

import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlFloorFunction;

import java.sql.DatabaseMetaData;

/**
 * A <code>SqlDialect</code> implementation for the Hsqldb database.
 */
public class HsqldbSqlDialect extends SqlDialect {
  public static final SqlDialect DEFAULT = new HsqldbSqlDialect();

  public HsqldbSqlDialect(DatabaseMetaData databaseMetaData) {
    super(
        DatabaseProduct.HSQLDB,
        databaseMetaData,
        resolveSequenceSupport(HsqldbSequenceSupport.INSTANCE, databaseMetaData)
    );
  }

  private HsqldbSqlDialect() {
    super(
        DatabaseProduct.HSQLDB,
        null,
        NullCollation.HIGH,
        resolveSequenceSupport(HsqldbSequenceSupport.INSTANCE, null)
    );
  }

  @Override public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    switch (call.getKind()) {
    case FLOOR:
      if (call.operandCount() != 2) {
        super.unparseCall(writer, call, leftPrec, rightPrec);
        return;
      }

      final SqlLiteral timeUnitNode = call.operand(1);
      final TimeUnitRange timeUnit = timeUnitNode.getValueAs(TimeUnitRange.class);

      final String translatedLit = convertTimeUnit(timeUnit);
      SqlCall call2 = SqlFloorFunction.replaceTimeUnitOperand(call, translatedLit,
          timeUnitNode.getParserPosition());
      SqlFloorFunction.unparseDatetimeFunction(writer, call2, "TRUNC", true);
      break;

    default:
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

  private static String convertTimeUnit(TimeUnitRange unit) {
    switch (unit) {
    case YEAR:
      return "YYYY";
    case MONTH:
      return "MM";
    case DAY:
      return "DD";
    case WEEK:
      return "WW";
    case HOUR:
      return "HH24";
    case MINUTE:
      return "MI";
    case SECOND:
      return "SS";
    default:
      throw new AssertionError("could not convert time unit to HSQLDB equivalent: "
          + unit);
    }
  }
}

// End HsqldbSqlDialect.java
