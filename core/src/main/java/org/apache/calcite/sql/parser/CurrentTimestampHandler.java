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
package org.apache.calcite.sql.parser;

import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Locale;

import static org.apache.calcite.sql.fun.SqlLibraryOperators.DATE_FORMAT;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.FORMAT_TIMESTAMP;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CAST;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CURRENT_TIMESTAMP;

/**
 * This class is specific to Hive, Spark and bigQuery to unparse CURRENT_TIMESTAMP function
 */
public class CurrentTimestampHandler {

  private SqlDialect sqlDialect;
  public static final String DEFAULT_DATE_FORMAT_FOR_HIVE = "yyyy-MM-dd HH:mm:ss";
  public static final String DEFAULT_DATE_FORMAT_FOR_BIGQUERY = "%F %H:%M:%E";
  public CurrentTimestampHandler(SqlDialect sqlDialect) {
    this.sqlDialect = sqlDialect;
  }

  public SqlCall makeDateFormatCall(SqlCall call) {
    SqlCharStringLiteral formatNode = makeSqlNodeForDateFormat(call);
    SqlNode timestampCall = new SqlBasicCall(CURRENT_TIMESTAMP, SqlNode.EMPTY_ARRAY,
            SqlParserPos.ZERO);
    SqlNode[] formatTimestampOperands = new SqlNode[]{timestampCall, formatNode};
    return new SqlBasicCall(DATE_FORMAT, formatTimestampOperands,
        SqlParserPos.ZERO);
  }

  private SqlCharStringLiteral makeSqlNodeForDateFormat(SqlCall call) {
    Integer precision = Integer.parseInt(((SqlLiteral) call.operand(0)).getValue().toString());
    StringBuilder fractionPart = new StringBuilder();
    for (int i = 0; i < precision; i++) {
      fractionPart.append('s');
    }
    return SqlLiteral.createCharString
            (buildDatetimeFormat(precision, fractionPart.toString()), SqlParserPos.ZERO);
  }

  private String buildDatetimeFormat(Integer precision, String fractionPart) {
    return precision > 0
            ? DEFAULT_DATE_FORMAT_FOR_HIVE + "." + fractionPart : DEFAULT_DATE_FORMAT_FOR_HIVE;
  }

  public SqlCall makeFormatTimestampCall(SqlCall call) {
    SqlCharStringLiteral formatNode = makeSqlNodeForFormatTimestamp(call);
    SqlNode timestampCall = new SqlBasicCall(CURRENT_TIMESTAMP, SqlNode.EMPTY_ARRAY,
            SqlParserPos.ZERO);
    SqlNode[] formatTimestampOperands = new SqlNode[]{formatNode, timestampCall};
    return new SqlBasicCall(FORMAT_TIMESTAMP, formatTimestampOperands, SqlParserPos.ZERO);
  }

  private SqlCharStringLiteral makeSqlNodeForFormatTimestamp(SqlCall call) {
    String precision = ((SqlLiteral) call.operand(0)).getValue().toString();
    String dateFormat = String.format
            (Locale.ROOT, "%s%s%s", DEFAULT_DATE_FORMAT_FOR_BIGQUERY, precision, "S");
    return SqlLiteral.createCharString(dateFormat, SqlParserPos.ZERO);
  }

  public SqlCall makeCastCall(SqlCall call) {
    SqlNode sqlTypeNode = sqlDialect.getCastSpec(
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.TIMESTAMP));
    SqlNode[] castOperands = new SqlNode[]{call, sqlTypeNode};
    return new SqlBasicCall(CAST, castOperands, SqlParserPos.ZERO);
  }
}
