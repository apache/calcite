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

import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.validate.*;

/**
 * A <code>SqlDelete</code> is a node of a parse tree which represents a DELETE
 * statement.
 */
public class SqlDelete extends SqlCall {
  //~ Static fields/initializers ---------------------------------------------

  // constants representing operand positions
  public static final int TARGET_TABLE_OPERAND = 0;
  public static final int CONDITION_OPERAND = 1;
  public static final int SOURCE_SELECT_OPERAND = 2;
  public static final int ALIAS_OPERAND = 3;
  public static final int OPERAND_COUNT = 4;

  //~ Constructors -----------------------------------------------------------

  public SqlDelete(
      SqlSpecialOperator operator,
      SqlIdentifier targetTable,
      SqlNode condition,
      SqlIdentifier alias,
      SqlParserPos pos) {
    super(
        operator,
        new SqlNode[OPERAND_COUNT],
        pos);
    operands[TARGET_TABLE_OPERAND] = targetTable;
    operands[CONDITION_OPERAND] = condition;
    operands[ALIAS_OPERAND] = alias;
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * @return the identifier for the target table of the deletion
   */
  public SqlIdentifier getTargetTable() {
    return (SqlIdentifier) operands[TARGET_TABLE_OPERAND];
  }

  /**
   * @return the alias for the target table of the deletion
   */
  public SqlIdentifier getAlias() {
    return (SqlIdentifier) operands[ALIAS_OPERAND];
  }

  /**
   * Gets the filter condition for rows to be deleted.
   *
   * @return the condition expression for the data to be deleted, or null for
   * all rows in the table
   */
  public SqlNode getCondition() {
    return operands[CONDITION_OPERAND];
  }

  /**
   * Gets the source SELECT expression for the data to be deleted. This
   * returns null before the condition has been expanded by
   * SqlValidator.performUnconditionRewrites.
   *
   * @return the source SELECT for the data to be inserted
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
        writer.startList(SqlWriter.FrameTypeEnum.Select, "DELETE FROM", "");
    getTargetTable().unparse(
        writer,
        getOperator().getLeftPrec(),
        getOperator().getRightPrec());
    if (getAlias() != null) {
      writer.keyword("AS");
      getAlias().unparse(
          writer,
          getOperator().getLeftPrec(),
          getOperator().getRightPrec());
    }
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
    validator.validateDelete(this);
  }
}

// End SqlDelete.java
