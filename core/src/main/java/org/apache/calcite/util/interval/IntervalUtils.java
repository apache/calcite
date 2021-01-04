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

package org.apache.calcite.util.interval;

import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.BigQuerySqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * util for resolving interval type operands
 */
public class IntervalUtils {

  //returns interval operand if present in a sqlCall
  public SqlIntervalLiteral getIntervalFromCall(SqlBasicCall call) {
    if (call.operandCount() == 1) {
      return call.operand(0) instanceof SqlIntervalLiteral ? call.operand(0) : null;
    }
    if (call.operand(1).getKind() == SqlKind.IDENTIFIER
        || (call.operand(1) instanceof SqlNumericLiteral)) {
      return call.operand(0);
    }
    return call.operand(1);
  }

  //returns the non interval operand from a sqlCall
  private SqlNode getNonIntervalOperand(SqlBasicCall intervalOperand) {
    if (intervalOperand.operandCount() == 1) {
      return null;
    }
    if (intervalOperand.operand(1).getKind() == SqlKind.IDENTIFIER
        || (intervalOperand.operand(1) instanceof SqlNumericLiteral)) {
      return intervalOperand.operand(1);
    }
    return intervalOperand.operand(0);
  }

  //return interval value
  //Ex for INTERVAL '2' MONTH returns 2
  public String getIntervalValue(SqlIntervalLiteral sqlIntervalLiteral) {
    try {
      if (sqlIntervalLiteral.getTypeName() == SqlTypeName.INTERVAL_HOUR_SECOND) {
        SqlIntervalLiteral.IntervalValue interval =
            (SqlIntervalLiteral.IntervalValue) sqlIntervalLiteral.getValue();
        long equivalentSecondValue = SqlParserUtil.intervalToMillis(interval.getIntervalLiteral(),
              interval.getIntervalQualifier()) / 1000;
        return Long.toString(equivalentSecondValue);
      }

      return sqlIntervalLiteral.getValueAs(Integer.class).toString();
    } catch (Throwable e) {
      return ((SqlIntervalLiteral.IntervalValue) sqlIntervalLiteral.getValue()).getSign() == -1
        ? "-" + sqlIntervalLiteral.getValue().toString() : sqlIntervalLiteral.getValue().toString();
    }
  }

  //builds a SqlCall with operand and literal string
  public SqlNode buildCallwithStringVal(String val, SqlBasicCall call, SqlNode operand) {
    if ((call.getKind() == SqlKind.TIMES
        || call.getOperator().getName().equals("TIMESTAMPINTMUL"))
        && val.trim().equals("1")) {
      return operand;
    }
    if (call.getOperator().getName().equals("TIMESTAMPINTMUL")) {
      return SqlStdOperatorTable.MULTIPLY.createCall(SqlParserPos.ZERO,
        operand, SqlLiteral.createExactNumeric(val, SqlParserPos.ZERO));
    }
    return call.getOperator().createCall(SqlParserPos.ZERO,
      operand, SqlLiteral.createExactNumeric(val, SqlParserPos.ZERO));
  }

  //resolves given internal expr to return suitable interval value
  public String buildInterval(SqlNode node, SqlDialect dialect) {
    String intervalLiteral = "";
    boolean isBq = dialect instanceof BigQuerySqlDialect;
    if (node instanceof SqlIntervalLiteral) {
      SqlIntervalLiteral literal = (SqlIntervalLiteral) node;
      intervalLiteral = getIntervalValue(literal);
      if (isBq) {
        TimeUnitRange tr = ((SqlIntervalLiteral.IntervalValue) literal.getValue())
            .getIntervalQualifier().timeUnitRange;
        String timeUnit = tr == TimeUnitRange.HOUR_TO_SECOND
            ? TimeUnitRange.SECOND.toString() : tr.toString();
        intervalLiteral = createInterval(intervalLiteral,
          timeUnit);
      }
    } else if (node instanceof SqlNumericLiteral) {
      Long intervalValue = ((SqlLiteral) node).getValueAs(Long.class);
      intervalLiteral = Long.toString(Math.abs(intervalValue));
      if (isBq) {
        intervalLiteral = createInterval(intervalLiteral, "MONTH");
      }
    } else {
      throw new UnsupportedOperationException("operand of type"
        + node.getClass().toString() + "not supported !");
    }

    return intervalLiteral;
  }

  public void unparse(SqlWriter writer, SqlCall call, int leftPrec,
                      int rightPrec, SqlDialect dialect) {
    boolean isBq = dialect instanceof BigQuerySqlDialect;
    SqlWriter.Frame frame = writer.startFunCall(call.getOperator().getName());
    if (call.getOperator().getName().equals("DATETIME_ADD")
        || call.getOperator().getName().equals("DATETIME_SUB")) {
      SqlWriter.Frame castFrame = writer.startFunCall("CAST");
      call.operand(0).unparse(writer, leftPrec, rightPrec);
      writer.sep("AS", true);
      writer.literal("DATETIME");
      writer.endFunCall(castFrame);
    } else {
      call.operand(0).unparse(writer, leftPrec, rightPrec);
    }
    writer.sep(",", true);
    String val;
    if (call.operand(1) instanceof SqlBasicCall) {
      if (isBq) {
        writer.print("INTERVAL ");
      }
      SqlBasicCall node = call.operand(1);
      SqlIntervalLiteral sqlIntervalLiteral = getIntervalFromCall(node);
      SqlNode identifier = getNonIntervalOperand(node);
      String interval = getIntervalValue(sqlIntervalLiteral);
      SqlNode opCall = buildCallwithStringVal(interval, node, identifier);
      opCall.unparse(writer, leftPrec, rightPrec);
      if (isBq) {
        writer.literal(((SqlIntervalLiteral.IntervalValue) sqlIntervalLiteral.getValue())
            .getIntervalQualifier().timeUnitRange.toString());
      }
    } else {
      val = buildInterval(call.operand(1), dialect);
      writer.print(val);
    }
    writer.endFunCall(frame);
  }

  private String createInterval(String ip, String intervalType) {
    return  "INTERVAL " + ip + " " + intervalType;
  }
}

// End IntervalUtils.java
