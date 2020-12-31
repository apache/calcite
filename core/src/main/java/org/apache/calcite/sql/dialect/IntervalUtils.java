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

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

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
      return sqlIntervalLiteral.getValueAs(Integer.class).toString();
    } catch (Throwable e) {
      return ((SqlIntervalLiteral.IntervalValue) sqlIntervalLiteral.getValue()).getSign() == -1
        ? "-" + sqlIntervalLiteral.getValue().toString() : sqlIntervalLiteral.getValue().toString();
    }
  }

  //builds a SqlCall with operand and literal string
  public SqlCall buildCallwithStringVal(String val, SqlBasicCall call, SqlNode operand) {
    return call.getOperator().createCall(SqlParserPos.ZERO,
      SqlLiteral.createExactNumeric(val, SqlParserPos.ZERO), operand);
  }


  public SqlIntervalLiteral createInterval(TimeUnit timeUnit, String interval) {
    return SqlLiteral.createInterval(1, interval,
      new SqlIntervalQualifier(timeUnit, -1, TimeUnit.MONTH, -1,
        SqlParserPos.ZERO), SqlParserPos.ZERO);
  }

  public SqlIntervalLiteral createInterval(SqlIntervalQualifier qualifier, String interval) {
    return SqlLiteral.createInterval(1, interval, qualifier, SqlParserPos.ZERO);
  }

  //resolves given node to return suitable interval
  public SqlIntervalLiteral buildInterval(SqlNode node, SqlDialect dialect) {
    SqlIntervalLiteral intervalLiteral;

    if (node instanceof SqlIntervalLiteral) {
      intervalLiteral = (SqlIntervalLiteral) node;
    } else if (node instanceof SqlNumericLiteral) {
      Long intervalValue = ((SqlLiteral) node).getValueAs(Long.class);
      String interval = Long.toString(Math.abs(intervalValue));
      intervalLiteral = createInterval(TimeUnit.MONTH, interval);
    } else if (node instanceof SqlBasicCall) {
      SqlIntervalLiteral sqlIntervalLiteral = getIntervalFromCall((SqlBasicCall) node);
      SqlNode identifier = getNonIntervalOperand((SqlBasicCall) node);
      String interval = getIntervalValue(sqlIntervalLiteral);
      SqlCall opCall = buildCallwithStringVal(interval, (SqlBasicCall) node, identifier);
      intervalLiteral = createInterval(
        ((SqlIntervalLiteral.IntervalValue) sqlIntervalLiteral.getValue()).getIntervalQualifier(),
        opCall.toSqlString(dialect).getSql());
    } else {
      throw new UnsupportedOperationException("operand of type"
        + node.getClass().toString() + "not supported !");
    }

    return intervalLiteral;
  }

  public void unparseAddMonths(SqlWriter writer, SqlCall call, int leftPrec,
                               int rightPrec, SqlDialect dialect) {
    SqlWriter.Frame frame = writer.startFunCall("ADD_MONTHS");
    call.operand(0).unparse(writer, leftPrec, rightPrec);
    writer.sep(",", true);
    SqlIntervalLiteral intervalLiteral = buildInterval(call.operand(1), dialect);
    String val = getIntervalValue(intervalLiteral);
    writer.print(val);
    writer.endFunCall(frame);
  }
}

// End IntervalUtils.java
