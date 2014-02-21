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
 * A <code>SqlMerge</code> is a node of a parse tree which represents a MERGE
 * statement.
 */
public class SqlMerge extends SqlCall {
  //~ Static fields/initializers ---------------------------------------------

  // constants representing operand positions
  public static final int TARGET_TABLE_OPERAND = 0;
  public static final int SOURCE_TABLEREF_OPERAND = 1;
  public static final int CONDITION_OPERAND = 2;
  public static final int UPDATE_OPERAND = 3;
  public static final int INSERT_OPERAND = 4;
  public static final int SOURCE_SELECT_OPERAND = 5;
  public static final int ALIAS_OPERAND = 6;
  public static final int OPERAND_COUNT = 7;

  //~ Constructors -----------------------------------------------------------

  public SqlMerge(
      SqlSpecialOperator operator,
      SqlIdentifier targetTable,
      SqlNode condition,
      SqlNode source,
      SqlNode updateCall,
      SqlNode insertCall,
      SqlIdentifier alias,
      SqlParserPos pos) {
    super(
        operator,
        new SqlNode[OPERAND_COUNT],
        pos);
    operands[TARGET_TABLE_OPERAND] = targetTable;
    operands[CONDITION_OPERAND] = condition;
    operands[SOURCE_TABLEREF_OPERAND] = source;
    operands[UPDATE_OPERAND] = updateCall;
    operands[INSERT_OPERAND] = insertCall;
    operands[ALIAS_OPERAND] = alias;
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * @return the identifier for the target table of the merge
   */
  public SqlIdentifier getTargetTable() {
    return (SqlIdentifier) operands[TARGET_TABLE_OPERAND];
  }

  /**
   * @return the alias for the target table of the merge
   */
  public SqlIdentifier getAlias() {
    return (SqlIdentifier) operands[ALIAS_OPERAND];
  }

  /**
   * @return the source for the merge
   */
  public SqlNode getSourceTableRef() {
    return (SqlNode) operands[SOURCE_TABLEREF_OPERAND];
  }

  public void setSourceTableRef(SqlNode tableRef) {
    operands[SOURCE_TABLEREF_OPERAND] = tableRef;
  }

  /**
   * @return the update statement for the merge
   */
  public SqlUpdate getUpdateCall() {
    return (SqlUpdate) operands[UPDATE_OPERAND];
  }

  /**
   * @return the insert statement for the merge
   */
  public SqlInsert getInsertCall() {
    return (SqlInsert) operands[INSERT_OPERAND];
  }

  /**
   * @return the condition expression to determine whether to update or insert
   */
  public SqlNode getCondition() {
    return operands[CONDITION_OPERAND];
  }

  /**
   * Gets the source SELECT expression for the data to be updated/inserted.
   * Returns null before the statement has been expanded by
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
        writer.startList(SqlWriter.FrameTypeEnum.SELECT, "MERGE INTO", "");
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

    writer.newlineAndIndent();
    writer.keyword("USING");
    getSourceTableRef().unparse(
        writer,
        getOperator().getLeftPrec(),
        getOperator().getRightPrec());

    writer.newlineAndIndent();
    writer.keyword("ON");
    getCondition().unparse(
        writer,
        getOperator().getLeftPrec(),
        getOperator().getRightPrec());

    SqlUpdate updateCall = (SqlUpdate) getUpdateCall();
    if (updateCall != null) {
      writer.newlineAndIndent();
      writer.keyword("WHEN MATCHED THEN UPDATE");
      final SqlWriter.Frame setFrame =
          writer.startList(
              SqlWriter.FrameTypeEnum.UPDATE_SET_LIST,
              "SET",
              "");

      Iterator targetColumnIter =
          updateCall.getTargetColumnList().getList().iterator();
      Iterator sourceExpressionIter =
          updateCall.getSourceExpressionList().getList().iterator();
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
    }

    SqlInsert insertCall = (SqlInsert) getInsertCall();
    if (insertCall != null) {
      writer.newlineAndIndent();
      writer.keyword("WHEN NOT MATCHED THEN INSERT");
      if (insertCall.getTargetColumnList() != null) {
        insertCall.getTargetColumnList().unparse(
            writer,
            getOperator().getLeftPrec(),
            getOperator().getRightPrec());
      }
      insertCall.getSource().unparse(
          writer,
          getOperator().getLeftPrec(),
          getOperator().getRightPrec());

      writer.endList(frame);
    }
  }

  public void validate(SqlValidator validator, SqlValidatorScope scope) {
    validator.validateMerge(this);
  }
}

// End SqlMerge.java
