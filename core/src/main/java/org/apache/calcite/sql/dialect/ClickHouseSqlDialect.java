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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAbstractDateTimeLiteral;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDateLiteral;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlTimeLiteral;
import org.apache.calcite.sql.SqlTimestampLiteral;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.google.common.base.Preconditions;

/**
 * A <code>SqlDialect</code> implementation for the ClickHouse database.
 */
public class ClickHouseSqlDialect extends SqlDialect {
  public static final SqlDialect DEFAULT =
      new ClickHouseSqlDialect(EMPTY_CONTEXT
          .withDatabaseProduct(DatabaseProduct.CLICKHOUSE)
          .withIdentifierQuoteString("`")
          .withNullCollation(NullCollation.LOW));

  private static final SqlSpecialOperator CLICKHOUSE_SUBSTRING =
      new SqlSpecialOperator("substring", SqlKind.OTHER_FUNCTION) {
        public void unparse(
            SqlWriter writer,
            SqlCall call,
            int leftPrec,
            int rightPrec) {
          writer.print(getName());
          final SqlWriter.Frame frame =
              writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
          for (SqlNode operand : call.getOperandList()) {
            writer.sep(",");
            operand.unparse(writer, 0, 0);
          }
          writer.endList(frame);
        }
      };

  /** Creates a ClickHouseSqlDialect. */
  public ClickHouseSqlDialect(Context context) {
    super(context);
  }

  @Override public boolean supportsCharSet() {
    return false;
  }

  @Override public boolean supportsNestedAggregations() {
    return false;
  }

  @Override public SqlNode getCastSpec(RelDataType type) {
    String castSpec;
    switch (type.getSqlTypeName()) {
    case VARCHAR:
      castSpec = "String";
      break;
    case TINYINT:
      castSpec = "Int8";
      break;
    case SMALLINT:
      castSpec = "Int16";
      break;
    case INTEGER:
      castSpec = "Int32";
      break;
    case BIGINT:
      castSpec = "Int64";
      break;
    case FLOAT:
      castSpec = "Float32";
      break;
    case DOUBLE:
      castSpec = "Float64";
      break;
    default:
      return super.getCastSpec(type);
    }

    return new SqlDataTypeSpec(new SqlIdentifier(castSpec, SqlParserPos.ZERO),
        type.getPrecision(), -1, null, null, SqlParserPos.ZERO);
  }

  @Override public void unparseDateTimeLiteral(SqlWriter writer,
      SqlAbstractDateTimeLiteral literal, int leftPrec, int rightPrec) {
    String toFunc;
    if (literal instanceof SqlDateLiteral) {
      toFunc = "toDate";
    } else if (literal instanceof SqlTimestampLiteral) {
      toFunc = "toDateTime";
    } else if (literal instanceof SqlTimeLiteral) {
      toFunc = "toTime";
    } else {
      throw new AssertionError("ClickHouse does not support DateTime literal: "
          + literal);
    }

    writer.literal(toFunc + "('" + literal.toFormattedString() + "')");
  }

  @Override public void unparseOffsetFetch(SqlWriter writer, SqlNode offset,
      SqlNode fetch) {
    Preconditions.checkArgument(fetch != null);

    writer.newlineAndIndent();
    final SqlWriter.Frame frame =
        writer.startList(SqlWriter.FrameTypeEnum.FETCH);
    writer.keyword("LIMIT");

    if (offset != null) {
      offset.unparse(writer, -1, -1);
      writer.sep(",", true);
    }

    fetch.unparse(writer, -1, -1);
    writer.endList(frame);
  }

  @Override public void unparseCall(SqlWriter writer, SqlCall call,
      int leftPrec, int rightPrec) {
    if (call.getOperator() == SqlStdOperatorTable.SUBSTRING) {
      CLICKHOUSE_SUBSTRING.unparse(writer, call, 0, 0);
    } else {
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
  }

  /**
   * Unparses datetime floor for ClickHouse.
   *
   * @param writer Writer
   * @param call Call
   */
  private void unparseFloor(SqlWriter writer, SqlCall call) {
    final SqlLiteral timeUnitNode = call.operand(1);
    TimeUnitRange unit = (TimeUnitRange) timeUnitNode.getValue();

    String funName;
    switch (unit) {
    case YEAR:
      funName = "toStartOfYear";
      break;
    case MONTH:
      funName = "toStartOfMonth";
      break;
    case WEEK:
      funName = "toMonday";
      break;
    case DAY:
      funName = "toDate";
      break;
    case HOUR:
      funName = "toStartOfHour";
      break;
    case MINUTE:
      funName = "toStartOfMinute";
      break;
    default:
      throw new AssertionError("ClickHouse does not support FLOOR for time unit: "
          + unit);
    }

    writer.print(funName);
    SqlWriter.Frame frame = writer.startList("(", ")");
    call.operand(0).unparse(writer, 0, 0);
    writer.endList(frame);
  }
}

// End ClickHouseSqlDialect.java
