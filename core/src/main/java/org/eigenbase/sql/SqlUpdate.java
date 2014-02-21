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

import java.util.*;

import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.validate.*;

/**
 * A <code>SqlUpdate</code> is a node of a parse tree which represents an UPDATE
 * statement.
 */
public class SqlUpdate extends SqlCall {
  //~ Static fields/initializers ---------------------------------------------

  // constants representing operand positions
  public static final int TARGET_TABLE_OPERAND = 0;
  public static final int SOURCE_EXPRESSION_LIST_OPERAND = 1;
  public static final int TARGET_COLUMN_LIST_OPERAND = 2;
  public static final int CONDITION_OPERAND = 3;
  public static final int SOURCE_SELECT_OPERAND = 4;
  public static final int ALIAS_OPERAND = 5;
  public static final int OPERAND_COUNT = 6;

  //~ Constructors -----------------------------------------------------------

  public SqlUpdate(
      SqlSpecialOperator operator,
      SqlIdentifier targetTable,
      SqlNodeList targetColumnList,
      SqlNodeList sourceExpressionList,
      SqlNode condition,
      SqlIdentifier alias,
      SqlParserPos pos) {
    super(
        operator,
        new SqlNode[OPERAND_COUNT],
        pos);
    operands[TARGET_TABLE_OPERAND] = targetTable;
    operands[SOURCE_EXPRESSION_LIST_OPERAND] = sourceExpressionList;
    operands[TARGET_COLUMN_LIST_OPERAND] = targetColumnList;
    operands[CONDITION_OPERAND] = condition;
    operands[ALIAS_OPERAND] = alias;
    assert sourceExpressionList.size() == targetColumnList.size();
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * @return the identifier for the target table of the update
   */
  public SqlIdentifier getTargetTable() {
    return (SqlIdentifier) operands[TARGET_TABLE_OPERAND];
  }

  /**
   * @return the alias for the target table of the update
   */
  public SqlIdentifier getAlias() {
    return (SqlIdentifier) operands[ALIAS_OPERAND];
  }

  /**
   * @return the list of target column names
   */
  public SqlNodeList getTargetColumnList() {
    return (SqlNodeList) operands[TARGET_COLUMN_LIST_OPERAND];
  }

  /**
   * @return the list of source expressions
   */
  public SqlNodeList getSourceExpressionList() {
    return (SqlNodeList) operands[SOURCE_EXPRESSION_LIST_OPERAND];
  }

  /**
   * Gets the filter condition for rows to be updated.
   *
   * @return the condition expression for the data to be updated, or null for
   * all rows in the table
   */
  public SqlNode getCondition() {
    return operands[CONDITION_OPERAND];
  }

  /**
   * Gets the source SELECT expression for the data to be updated. Returns
   * null before the statement has been expanded by
   * SqlValidator.performUnconditionalRewrites.
   *
   * @return the source SELECT for the data to be updated
   */
  public SqlSelect getSourceSelect() {
    return (SqlSelect) operands[SOURCE_SELECT_OPERAND];
  }

  // implement SqlNode
  public void unparse(
      SqlWriter writer,
      int leftPrec,
      int rightPrec) {
    final SqlWriter.Frame frame =
        writer.startList(SqlWriter.FrameTypeEnum.SELECT, "UPDATE", "");
    getTargetTable().unparse(
        writer,
        getOperator().getLeftPrec(),
        getOperator().getRightPrec());
    if (getTargetColumnList() != null) {
      getTargetColumnList().unparse(
          writer,
          getOperator().getLeftPrec(),
          getOperator().getRightPrec());
    }
    if (getAlias() != null) {
      writer.keyword("AS");
      getAlias().unparse(
          writer,
          getOperator().getLeftPrec(),
          getOperator().getRightPrec());
    }
    final SqlWriter.Frame setFrame =
        writer.startList(SqlWriter.FrameTypeEnum.UPDATE_SET_LIST, "SET", "");
    Iterator targetColumnIter = getTargetColumnList().getList().iterator();
    Iterator sourceExpressionIter =
        getSourceExpressionList().getList().iterator();
    while (targetColumnIter.hasNext()) {
      writer.sep(",");
      SqlIdentifier id = (SqlIdentifier) targetColumnIter.next();
      id.unparse(
          writer,
          getOperator().getLeftPrec(),
          getOperator().getRightPrec());
      writer.keyword("=");
      SqlNode sourceExp = (SqlNode) sourceExpressionIter.next();
      sourceExp.unparse(
          writer,
          getOperator().getLeftPrec(),
          getOperator().getRightPrec());
    }
    writer.endList(setFrame);
    if (getCondition() != null) {
      writer.sep("WHERE");
      getCondition().unparse(
          writer,
          getOperator().getLeftPrec(),
          getOperator().getRightPrec());
    }
    writer.endList(frame);
  }

  public void validate(SqlValidator validator, SqlValidatorScope scope) {
    validator.validateUpdate(this);
  }
}

// End SqlUpdate.java
