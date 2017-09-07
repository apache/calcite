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

import java.sql.DatabaseMetaData;

/**
 * A <code>SqlDialect</code> implementation for the Mysql database.
 */
public class MysqlSqlDialect extends SqlDialect {
  public static final SqlDialect DEFAULT = new MysqlSqlDialect();

  public MysqlSqlDialect(DatabaseMetaData databaseMetaData) {
    super(DatabaseProduct.MYSQL, databaseMetaData, null);
  }

  private MysqlSqlDialect() {
    super(DatabaseProduct.MYSQL, "`", NullCollation.HIGH, null);
  }

  @Override public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    switch (call.getKind()) {
    case FLOOR:
      if (call.operandCount() != 2) {
        super.unparseCall(writer, call, leftPrec, rightPrec);
        return;
      }

      unparseFloor(writer, call);
      break;

    default:
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

  /**
   * Unparses datetime floor for MySQL. There is no TRUNC function, so simulate
   * this using calls to DATE_FORMAT.
   *
   * @param writer Writer
   * @param call Call
   */
  private void unparseFloor(SqlWriter writer, SqlCall call) {
    SqlLiteral node = call.operand(1);
    TimeUnitRange unit = (TimeUnitRange) node.getValue();

    if (unit == TimeUnitRange.WEEK) {
      writer.print("STR_TO_DATE");
      SqlWriter.Frame frame = writer.startList("(", ")");

      writer.print("DATE_FORMAT(");
      call.operand(0).unparse(writer, 0, 0);
      writer.print(", '%x%v-1'), '%x%v-%w'");
      writer.endList(frame);
      return;
    }

    String format;
    switch (unit) {
    case YEAR:
      format = "%Y-01-01";
      break;
    case MONTH:
      format = "%Y-%m-01";
      break;
    case DAY:
      format = "%Y-%m-%d";
      break;
    case HOUR:
      format = "%Y-%m-%d %k:00:00";
      break;
    case MINUTE:
      format = "%Y-%m-%d %k:%i:00";
      break;
    case SECOND:
      format = "%Y-%m-%d %k:%i:%s";
      break;
    default:
      throw new AssertionError("MYSQL does not support FLOOR for time unit: "
          + unit);
    }

    writer.print("DATE_FORMAT");
    SqlWriter.Frame frame = writer.startList("(", ")");
    call.operand(0).unparse(writer, 0, 0);
    writer.sep(",", true);
    writer.print("'" + format + "'");
    writer.endList(frame);
  }
}

// End MysqlSqlDialect.java
