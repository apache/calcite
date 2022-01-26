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
 *  A <code>SqlTableRef</code> is a node of a parse tree which represents
 *  a table reference.
 *
 *  <p>It can be attached with a sql hint statement, see {@link SqlHint} for details.
 */
public class SqlTableRef extends SqlCall {

  //~ Instance fields --------------------------------------------------------

  private final SqlNode tableRef;
  private final SqlNodeList hints;
  private final boolean isLateralTable;

  //~ Static fields/initializers ---------------------------------------------

  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("TABLE_REF", SqlKind.TABLE_REF) {
        @Override public SqlCall createCall(
            @Nullable SqlLiteral functionQualifier,
            SqlParserPos pos, @Nullable SqlNode... operands) {
          return new SqlTableRef(pos,
              requireNonNull(operands[0], "tableRef"),
              (SqlNodeList) requireNonNull(operands[1], "hints"));
        }
      };

  private static final SqlOperator LATERAL_OPERATOR =
      new SqlSpecialOperator("LATERAL_TABLE_REF", SqlKind.LATERAL_TABLE_REF) {
        @Override public SqlCall createCall(
            @Nullable SqlLiteral functionQualifier,
            SqlParserPos pos, @Nullable SqlNode... operands) {
          return new SqlTableRef(pos,
              requireNonNull(operands[0], "tableRef"),
              (SqlNodeList) requireNonNull(operands[1], "hints"),
              true);
        }
      };

  //~ Constructors -----------------------------------------------------------

  public SqlTableRef(SqlParserPos pos, SqlNode tableRef, SqlNodeList hints) {
    this(pos, tableRef, hints, false);
  }

  public SqlTableRef(
      SqlParserPos pos,
      SqlNode tableRef,
      SqlNodeList hints,
      boolean isLateralTable) {
    super(pos);
    this.tableRef = tableRef;
    this.hints = hints;
    this.isLateralTable = isLateralTable;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlOperator getOperator() {
    if (isLateralTable) {
      return LATERAL_OPERATOR;
    }
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableList.of(tableRef, hints);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    tableRef.unparse(writer, leftPrec, rightPrec);
    if (this.hints != null && this.hints.size() > 0) {
      writer.getDialect().unparseTableScanHints(writer, this.hints, leftPrec, rightPrec);
    }
  }
}
