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

import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.validate.*;

/**
 * A <code>SqlSelect</code> is a node of a parse tree which represents a select
 * statement. It warrants its own node type just because we have a lot of
 * methods to put somewhere.
 */
public class SqlSelect extends SqlCall {
  //~ Static fields/initializers ---------------------------------------------

  // constants representing operand positions
  public static final int KEYWORDS_OPERAND = 0;
  public static final int SELECT_OPERAND = 1;
  public static final int FROM_OPERAND = 2;
  public static final int WHERE_OPERAND = 3;
  public static final int GROUP_OPERAND = 4;
  public static final int HAVING_OPERAND = 5;
  public static final int WINDOW_OPERAND = 6;
  public static final int ORDER_OPERAND = 7;
  public static final int OFFSET_OPERAND = 8;
  public static final int FETCH_OPERAND = 9;
  public static final int OPERAND_COUNT = 10;

  //~ Constructors -----------------------------------------------------------

  SqlSelect(
      SqlSelectOperator operator,
      SqlNode[] operands,
      SqlParserPos pos) {
    super(operator, operands, pos);
    assert operands.length == OPERAND_COUNT;
    assert operands[KEYWORDS_OPERAND] instanceof SqlNodeList;
    assert operands[WINDOW_OPERAND] instanceof SqlNodeList;
    assert pos != null;
  }

  //~ Methods ----------------------------------------------------------------

  public final boolean isDistinct() {
    return getModifierNode(SqlSelectKeyword.Distinct) != null;
  }

  public final SqlNode getModifierNode(SqlSelectKeyword modifier) {
    final SqlNodeList keywords =
        (SqlNodeList) operands[SqlSelect.KEYWORDS_OPERAND];
    for (int i = 0; i < keywords.size(); i++) {
      SqlSelectKeyword keyword =
          (SqlSelectKeyword) SqlLiteral.symbolValue(keywords.get(i));
      if (keyword == modifier) {
        return keywords.get(i);
      }
    }
    return null;
  }

  public final SqlNode getFrom() {
    return operands[SqlSelect.FROM_OPERAND];
  }

  public final SqlNodeList getGroup() {
    return (SqlNodeList) operands[SqlSelect.GROUP_OPERAND];
  }

  public final SqlNode getHaving() {
    return operands[SqlSelect.HAVING_OPERAND];
  }

  public final SqlNodeList getSelectList() {
    return (SqlNodeList) operands[SqlSelect.SELECT_OPERAND];
  }

  public final SqlNode getWhere() {
    return operands[SqlSelect.WHERE_OPERAND];
  }

  public final SqlNodeList getWindowList() {
    return (SqlNodeList) operands[SqlSelect.WINDOW_OPERAND];
  }

  public final SqlNodeList getOrderList() {
    return (SqlNodeList) operands[SqlSelect.ORDER_OPERAND];
  }

  public final SqlNode getOffset() {
    return operands[SqlSelect.OFFSET_OPERAND];
  }

  public final SqlNode getFetch() {
    return operands[SqlSelect.FETCH_OPERAND];
  }

  public void addFrom(SqlIdentifier tableId) {
    SqlNode fromClause = getFrom();
    if (fromClause == null) {
      fromClause = tableId;
    } else {
      fromClause =
          SqlStdOperatorTable.joinOperator.createCall(
              null,
              fromClause,
              tableId);
    }
    operands[FROM_OPERAND] = fromClause;
  }

  public void addWhere(SqlNode condition) {
    assert operands[SELECT_OPERAND] == null
        : "cannot add a filter if there is already a select list";
    operands[WHERE_OPERAND] =
        SqlUtil.andExpressions(operands[WHERE_OPERAND], condition);
  }

  public void validate(SqlValidator validator, SqlValidatorScope scope) {
    validator.validateQuery(this, scope);
  }

  // Override SqlCall, to introduce a subquery frame.
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    if (!writer.inQuery()) {
      // If this SELECT is the topmost item in a subquery, introduce a new
      // frame. (The topmost item in the subquery might be a UNION or
      // ORDER. In this case, we don't need a wrapper frame.)
      final SqlWriter.Frame frame =
          writer.startList(SqlWriter.FrameTypeEnum.Subquery, "(", ")");
      getOperator().unparse(writer, operands, 0, 0);
      writer.endList(frame);
    } else {
      getOperator().unparse(writer, operands, leftPrec, rightPrec);
    }
  }

  public boolean hasOrderBy() {
    SqlNodeList orderList = getOrderList();
    return ((null != orderList) && (0 != orderList.size()));
  }

  public boolean hasWhere() {
    return null != getWhere();
  }

  public boolean isKeywordPresent(SqlSelectKeyword targetKeyWord) {
    final SqlNodeList keywordList =
        (SqlNodeList) operands[SqlSelect.KEYWORDS_OPERAND];
    for (int i = 0; i < keywordList.size(); i++) {
      final SqlSelectKeyword keyWord =
          (SqlSelectKeyword) SqlLiteral.symbolValue(keywordList.get(i));
      if (keyWord == targetKeyWord) {
        return true;
      }
    }
    return false;
  }
}

// End SqlSelect.java
