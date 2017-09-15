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
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

import java.sql.DatabaseMetaData;

/**
 * A <code>SqlDialect</code> implementation for the Mysql database.
 */
public class MysqlSqlDialect extends SqlDialect {
  public static final SqlDialect DEFAULT = new MysqlSqlDialect();

  /** MySQL specific function. */
  public static final SqlFunction ISNULL_FUNCTION =
      new SqlFunction("ISNULL", SqlKind.OTHER_FUNCTION,
          ReturnTypes.BOOLEAN, InferTypes.FIRST_KNOWN,
          OperandTypes.ANY, SqlFunctionCategory.SYSTEM);

  public MysqlSqlDialect(DatabaseMetaData databaseMetaData) {
    super(DatabaseProduct.MYSQL, databaseMetaData);
  }

  private MysqlSqlDialect() {
    super(DatabaseProduct.MYSQL, "`", NullCollation.HIGH);
  }

  @Override public boolean supportsCharSet() {
    return false;
  }

  @Override public boolean supportsOffsetFetch() {
    return false;
  }

  @Override public SqlNode emulateNullDirection(SqlImplementor.Context context,
      RelFieldCollation field) {
    return ISNULL_FUNCTION.createCall(SqlParserPos.ZERO, context.field(field.getFieldIndex()));
  }

  @Override public boolean supportsAggregateFunction(SqlKind kind) {
    switch (kind) {
    case COUNT:
    case SUM:
    case SUM0:
    case MIN:
    case MAX:
    case SINGLE_VALUE:
      return true;
    }
    return false;
  }

  @Override public CalendarPolicy getCalendarPolicy() {
    return CalendarPolicy.SHIFT;
  }

  @Override public SqlNode getCastSpec(RelDataType type) {
    switch (type.getSqlTypeName()) {
    case VARCHAR:
      // MySQL doesn't have a VARCHAR type, only CHAR.
      return new SqlDataTypeSpec(new SqlIdentifier("CHAR", SqlParserPos.ZERO),
          type.getPrecision(), -1, null, null, SqlParserPos.ZERO);
    case INTEGER:
      return new SqlDataTypeSpec(new SqlIdentifier("_UNSIGNED", SqlParserPos.ZERO),
          type.getPrecision(), -1, null, null, SqlParserPos.ZERO);
    }
    return super.getCastSpec(type);
  }

  @Override public SqlNode rewriteSingleValueExpr(SqlNode aggCall) {
    final SqlNode operand = ((SqlBasicCall) aggCall).operand(0);
    final SqlLiteral nullLiteral = SqlLiteral.createNull(SqlParserPos.ZERO);
    final SqlNode unionOperand = new SqlSelect(SqlParserPos.ZERO, SqlNodeList.EMPTY,
        SqlNodeList.of(nullLiteral), null, null, null, null, SqlNodeList.EMPTY, null, null, null);
    // For MySQL, generate
    //   CASE COUNT(*)
    //   WHEN 0 THEN NULL
    //   WHEN 1 THEN <result>
    //   ELSE (SELECT NULL UNION ALL SELECT NULL)
    //   END
    final SqlNode caseExpr =
        new SqlCase(SqlParserPos.ZERO,
            SqlStdOperatorTable.COUNT.createCall(SqlParserPos.ZERO, operand),
            SqlNodeList.of(
                SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO),
                SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO)
            ),
            SqlNodeList.of(
                nullLiteral,
                operand
            ),
            SqlStdOperatorTable.SCALAR_QUERY.createCall(SqlParserPos.ZERO,
                SqlStdOperatorTable.UNION_ALL
                    .createCall(SqlParserPos.ZERO, unionOperand, unionOperand)));

    LOGGER.debug("SINGLE_VALUE rewritten into [{}]", caseExpr);

    return caseExpr;
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
