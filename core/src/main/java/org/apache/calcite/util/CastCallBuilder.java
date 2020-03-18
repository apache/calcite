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
package org.apache.calcite.util;

import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Locale;

import static org.apache.calcite.sql.fun.SqlLibraryOperators.FORMAT_TIMESTAMP;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.STRING_SPLIT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CAST;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ITEM;

/**
 * Used to build cast call based cast type
 */

public class CastCallBuilder {

  private SqlDialect dialect;
  private static final SqlParserPos POS = SqlParserPos.ZERO;

  public CastCallBuilder(SqlDialect dialect) {
    this.dialect = dialect;
  }

  public SqlNode makCastCallForTimestampWithPrecision(SqlNode operandToCast, int precision) {
    SqlNode timestampWithoutPrecision =
        dialect.getCastSpec(new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.TIMESTAMP));
    SqlCall castedTimestampNode = CAST.createCall(POS, operandToCast,
        timestampWithoutPrecision);
    SqlCharStringLiteral timestampFormat = SqlLiteral.createCharString(String.format
        (Locale.ROOT, "%s%s%s", "YYYY-MM-DDBHH24:MI:SS.S(", precision, ")"), POS);
    SqlCall formattedCall = FORMAT_TIMESTAMP.createCall(POS, timestampFormat,
        castedTimestampNode);
    return CAST.createCall(POS, formattedCall, timestampWithoutPrecision);
  }

  public SqlNode makCastCallForTimeWithPrecision(SqlNode operandToCast, int precision) {
    SqlCharStringLiteral timestampFormat;
    if (precision == 0) {
      timestampFormat = SqlLiteral.createCharString("YYYY-MM-DDBHH24:MI:SS", POS);
    } else {
      timestampFormat = SqlLiteral.createCharString(String.format
          (Locale.ROOT, "%s%s%s", "YYYY-MM-DDBHH24:MI:SS.S(", precision, ")"), POS);
    }
    SqlCall formattedCall = FORMAT_TIMESTAMP.createCall(POS, timestampFormat, operandToCast);
    SqlCall splitFunctionCall = STRING_SPLIT.createCall(POS, formattedCall,
        SqlLiteral.createCharString(" ", POS));
    return ITEM.createCall(POS, splitFunctionCall, SqlLiteral.createExactNumeric("1", POS));
  }

}

// End CastCallBuilder.java
