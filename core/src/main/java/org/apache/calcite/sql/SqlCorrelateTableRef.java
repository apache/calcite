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

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * A <code>SqlCorrelateTableRef</code> is a node of a parse tree which represents
 * a lateral table with hints.
 */
public class SqlCorrelateTableRef extends SqlCall {

  //~ Instance fields --------------------------------------------------------
  private final SqlNode correlateTable;
  private final SqlNodeList hints;

  //~ Static fields/initializers ---------------------------------------------
  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("CORRELATE_TABLE_REF", SqlKind.LATERAL) {
        @Override public SqlCall createCall(
            @Nullable SqlLiteral functionQualifier,
            SqlParserPos pos, @Nullable SqlNode... operands) {
          return new SqlCorrelateTableRef(pos,
              requireNonNull(operands[0], "correlateTable"),
              (SqlNodeList) requireNonNull(operands[1], "hints"));
        }
      };

  //~ Constructors -----------------------------------------------------------

  public SqlCorrelateTableRef(SqlParserPos pos, SqlNode correlateTable, SqlNodeList hints) {
    super(pos);
    this.correlateTable = correlateTable;
    this.hints = hints;
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableList.of(correlateTable, hints);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    correlateTable.unparse(writer, leftPrec, rightPrec);
    if (this.hints != null && this.hints.size() > 0) {
      writer.getDialect().unparseTableScanHints(writer, this.hints, leftPrec, rightPrec);
    }
  }
}
