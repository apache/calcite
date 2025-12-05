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
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * A <code>SqlDialect</code> implementation for the DuckDB database.
 */
public class DuckDBSqlDialect extends SqlDialect {
  public static final RelDataTypeSystem TYPE_SYSTEM =
      new RelDataTypeSystemImpl() {

        // We can refer to document of DuckDB 1.2.x:
        // https://duckdb.org/docs/stable/sql/data_types/numeric#fixed-point-decimals
        @Override public int getMaxPrecision(SqlTypeName typeName) {
          switch (typeName) {
          case DECIMAL:
            return 38;
          default:
            return super.getMaxPrecision(typeName);
          }
        }

        @Override public int getMaxScale(SqlTypeName typeName) {
          switch (typeName) {
          case DECIMAL:
            return 38;
          default:
            return super.getMaxScale(typeName);
          }
        }

        @Override public int getMaxNumericScale() {
          return getMaxScale(SqlTypeName.DECIMAL);
        }
      };

  public static final SqlDialect.Context DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT
      .withDatabaseProduct(SqlDialect.DatabaseProduct.DUCKDB)
      .withIdentifierQuoteString("\"")
      // Refer to document: https://duckdb.org/docs/stable/sql/query_syntax/orderby.html
      .withNullCollation(NullCollation.LAST)
      .withDataTypeSystem(TYPE_SYSTEM);

  public static final SqlDialect DEFAULT = new DuckDBSqlDialect(DEFAULT_CONTEXT);

  /** Creates a DuckDBSqlDialect. */
  public DuckDBSqlDialect(SqlDialect.Context context) {
    super(context);
  }

  @Override public boolean supportsApproxCountDistinct() {
    return true;
  }

  @Override public boolean supportsAliasedValues() {
    return false;
  }

  @Override public void unparseCall(SqlWriter writer, SqlCall call,
      int leftPrec, int rightPrec) {
    switch (call.getKind()) {
    case MAP_VALUE_CONSTRUCTOR:
      writer.keyword(call.getOperator().getName());
      final SqlWriter.Frame mapFrame = writer.startList("{", "}");
      for (int i = 0; i < call.operandCount(); i++) {
        String sep = i % 2 == 0 ? "," : ":";
        writer.sep(sep);
        call.operand(i).unparse(writer, leftPrec, rightPrec);
      }
      writer.endList(mapFrame);
      break;
    case FLOOR:
      if (call.operandCount() != 2) {
        super.unparseCall(writer, call, leftPrec, rightPrec);
        return;
      }
      unparseFloor(writer, call);
      break;
    case CHAR_LENGTH:
      SqlCall lengthCall = SqlLibraryOperators.LENGTH
          .createCall(SqlParserPos.ZERO, call.getOperandList());
      super.unparseCall(writer, lengthCall, leftPrec, rightPrec);
      break;
    default:
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

  private static void unparseFloor(SqlWriter writer, SqlCall call) {
    SqlLiteral node = call.operand(1);
    TimeUnitRange unit = node.getValueAs(TimeUnitRange.class);

    String format;
    switch (unit) {
    case YEAR:
      format = "year";
      break;
    case MONTH:
      format = "month";
      break;
    case WEEK:
      format = "week";
      break;
    case DAY:
      format = "day";
      break;
    case HOUR:
      format = "hour";
      break;
    case MINUTE:
      format = "minute";
      break;
    case SECOND:
      format = "second";
      break;
    case MILLISECOND:
      format = "milliseconds";
      break;
    case MICROSECOND:
      format = "microseconds";
      break;
    default:
      throw new AssertionError("DUCKDB does not support FLOOR for time unit: "
          + unit);
    }

    // Refer to document: https://duckdb.org/docs/stable/sql/functions/date#date_truncpart-date
    writer.print("DATETRUNC");
    SqlWriter.Frame frame = writer.startList("(", ")");
    writer.print("'" + format + "'");
    writer.sep(",", true);
    call.operand(0).unparse(writer, 0, 0);
    writer.endList(frame);
  }

}
