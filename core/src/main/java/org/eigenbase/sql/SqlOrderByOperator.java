/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.eigenbase.sql;

/**
 * SqlOrderByOperator is used to represent an ORDER BY on a query other than a
 * SELECT (e.g. VALUES or UNION). It is a purely syntactic operator, and is
 * eliminated by SqlValidator.performUnconditionalRewrites and replaced with the
 * ORDER_OPERAND of SqlSelect.
 */
public class SqlOrderByOperator extends SqlSpecialOperator {
  //~ Static fields/initializers ---------------------------------------------

  // constants representing operand positions
  public static final int QUERY_OPERAND = 0;
  public static final int ORDER_OPERAND = 1;
  public static final int OFFSET_OPERAND = 2;
  public static final int FETCH_OPERAND = 3;

  //~ Constructors -----------------------------------------------------------

  public SqlOrderByOperator() {
    // NOTE:  make precedence lower then SELECT to avoid extra parens
    super("ORDER BY", SqlKind.ORDER_BY, 0);
  }

  //~ Methods ----------------------------------------------------------------

  public SqlSyntax getSyntax() {
    return SqlSyntax.POSTFIX;
  }

  public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    assert call.operandCount() == 4;
    final SqlWriter.Frame frame =
        writer.startList(SqlWriter.FrameTypeEnum.ORDER_BY);
    call.operand(QUERY_OPERAND).unparse(writer, getLeftPrec(), getRightPrec());
    if (call.operand(ORDER_OPERAND) != SqlNodeList.EMPTY) {
      writer.sep(getName());
      final SqlWriter.Frame listFrame =
          writer.startList(SqlWriter.FrameTypeEnum.ORDER_BY_LIST);
      unparseListClause(writer, call.operand(ORDER_OPERAND));
      writer.endList(listFrame);
    }
    if (call.operand(OFFSET_OPERAND) != null) {
      final SqlWriter.Frame frame2 =
          writer.startList(SqlWriter.FrameTypeEnum.OFFSET);
      writer.newlineAndIndent();
      writer.keyword("OFFSET");
      call.operand(OFFSET_OPERAND).unparse(writer, -1, -1);
      writer.keyword("ROWS");
      writer.endList(frame2);
    }
    if (call.operand(FETCH_OPERAND) != null) {
      final SqlWriter.Frame frame3 =
          writer.startList(SqlWriter.FrameTypeEnum.FETCH);
      writer.newlineAndIndent();
      writer.keyword("FETCH");
      writer.keyword("NEXT");
      call.operand(FETCH_OPERAND).unparse(writer, -1, -1);
      writer.keyword("ROWS");
      writer.keyword("ONLY");
      writer.endList(frame3);
    }
    writer.endList(frame);
  }
}

// End SqlOrderByOperator.java
