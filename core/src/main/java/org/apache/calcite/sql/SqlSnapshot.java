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
package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;
import java.util.Objects;

/**
 * Parse tree node for "{@code FOR SYSTEM_TIME AS OF}" temporal clause.
 */
public class SqlSnapshot extends SqlCall {
  private static final int OPERAND_TABLE_REF = 0;
  private static final int OPERAND_PERIOD = 1;

  //~ Instance fields -------------------------------------------

  private SqlNode tableRef;
  private SqlNode period;

  /** Creates a SqlSnapshot. */
  public SqlSnapshot(SqlParserPos pos, SqlNode tableRef, SqlNode period) {
    super(pos);
    this.tableRef = Objects.requireNonNull(tableRef);
    this.period = Objects.requireNonNull(period);
  }

  // ~ Methods

  @Override public SqlOperator getOperator() {
    return SqlSnapshotOperator.INSTANCE;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(tableRef, period);
  }

  public SqlNode getTableRef() {
    return tableRef;
  }

  public SqlNode getPeriod() {
    return period;
  }

  @Override public void setOperand(int i, SqlNode operand) {
    switch (i) {
    case OPERAND_TABLE_REF:
      tableRef = Objects.requireNonNull(operand);
      break;
    case OPERAND_PERIOD:
      period = Objects.requireNonNull(operand);
      break;
    default:
      throw new AssertionError(i);
    }
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    getOperator().unparse(writer, this, 0, 0);
  }

  /**
   * An operator describing a FOR SYSTEM_TIME specification.
   */
  public static class SqlSnapshotOperator extends SqlOperator {

    public static final SqlSnapshotOperator INSTANCE = new SqlSnapshotOperator();

    private SqlSnapshotOperator() {
      super("SNAPSHOT", SqlKind.SNAPSHOT, 2, true, null, null, null);
    }

    @Override public SqlSyntax getSyntax() {
      return SqlSyntax.SPECIAL;
    }

    @Override public SqlCall createCall(
        SqlLiteral functionQualifier,
        SqlParserPos pos,
        SqlNode... operands) {
      assert functionQualifier == null;
      assert operands.length == 2;
      return new SqlSnapshot(pos, operands[0], operands[1]);
    }

    @Override public <R> void acceptCall(
        SqlVisitor<R> visitor,
        SqlCall call,
        boolean onlyExpressions,
        SqlBasicVisitor.ArgHandler<R> argHandler) {
      if (onlyExpressions) {
        List<SqlNode> operands = call.getOperandList();
        // skip the first operand
        for (int i = 1; i < operands.size(); i++) {
          argHandler.visitChild(visitor, call, i, operands.get(i));
        }
      } else {
        super.acceptCall(visitor, call, false, argHandler);
      }
    }

    @Override public void unparse(
        SqlWriter writer,
        SqlCall call,
        int leftPrec,
        int rightPrec) {
      final SqlSnapshot snapshot = (SqlSnapshot) call;

      snapshot.tableRef.unparse(writer, 0, 0);
      writer.keyword("FOR SYSTEM_TIME AS OF");
      snapshot.period.unparse(writer, 0, 0);
    }
  }
}

// End SqlSnapshot.java
