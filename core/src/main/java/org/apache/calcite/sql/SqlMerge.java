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
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Pair;

import java.util.List;

/**
 * A <code>SqlMerge</code> is a node of a parse tree which represents a MERGE
 * statement.
 */
public class SqlMerge extends SqlCall {
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("MERGE", SqlKind.MERGE);

  SqlNode targetTable;
  SqlNode condition;
  SqlNode source;
  SqlUpdate updateCall;
  SqlInsert insertCall;
  SqlSelect sourceSelect;
  SqlIdentifier alias;

  //~ Constructors -----------------------------------------------------------

  public SqlMerge(SqlParserPos pos,
      SqlNode targetTable,
      SqlNode condition,
      SqlNode source,
      SqlUpdate updateCall,
      SqlInsert insertCall,
      SqlSelect sourceSelect,
      SqlIdentifier alias) {
    super(pos);
    this.targetTable = targetTable;
    this.condition = condition;
    this.source = source;
    this.updateCall = updateCall;
    this.insertCall = insertCall;
    this.sourceSelect = sourceSelect;
    this.alias = alias;
  }

  //~ Methods ----------------------------------------------------------------

  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public SqlKind getKind() {
    return SqlKind.MERGE;
  }

  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(targetTable, condition, source, updateCall,
        insertCall, sourceSelect, alias);
  }

  @Override public void setOperand(int i, SqlNode operand) {
    switch (i) {
    case 0:
      assert operand instanceof SqlIdentifier;
      targetTable = operand;
      break;
    case 1:
      condition = operand;
      break;
    case 2:
      source = operand;
      break;
    case 3:
      updateCall = (SqlUpdate) operand;
      break;
    case 4:
      insertCall = (SqlInsert) operand;
      break;
    case 5:
      sourceSelect = (SqlSelect) operand;
      break;
    case 6:
      alias = (SqlIdentifier) operand;
      break;
    default:
      throw new AssertionError(i);
    }
  }

  /**
   * @return the identifier for the target table of the merge
   */
  public SqlNode getTargetTable() {
    return targetTable;
  }

  /**
   * @return the alias for the target table of the merge
   */
  public SqlIdentifier getAlias() {
    return alias;
  }

  /**
   * @return the source for the merge
   */
  public SqlNode getSourceTableRef() {
    return source;
  }

  public void setSourceTableRef(SqlNode tableRef) {
    this.source = tableRef;
  }

  /**
   * @return the update statement for the merge
   */
  public SqlUpdate getUpdateCall() {
    return updateCall;
  }

  /**
   * @return the insert statement for the merge
   */
  public SqlInsert getInsertCall() {
    return insertCall;
  }

  /**
   * @return the condition expression to determine whether to update or insert
   */
  public SqlNode getCondition() {
    return condition;
  }

  /**
   * Gets the source SELECT expression for the data to be updated/inserted.
   * Returns null before the statement has been expanded by
   * {@link SqlValidatorImpl#performUnconditionalRewrites(SqlNode, boolean)}.
   *
   * @return the source SELECT for the data to be updated
   */
  public SqlSelect getSourceSelect() {
    return sourceSelect;
  }

  public void setSourceSelect(SqlSelect sourceSelect) {
    this.sourceSelect = sourceSelect;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    final SqlWriter.Frame frame =
        writer.startList(SqlWriter.FrameTypeEnum.SELECT, "MERGE INTO", "");
    final int opLeft = getOperator().getLeftPrec();
    final int opRight = getOperator().getRightPrec();
    targetTable.unparse(writer, opLeft, opRight);
    if (alias != null) {
      writer.keyword("AS");
      alias.unparse(writer, opLeft, opRight);
    }

    writer.newlineAndIndent();
    writer.keyword("USING");
    source.unparse(writer, opLeft, opRight);

    writer.newlineAndIndent();
    writer.keyword("ON");
    condition.unparse(writer, opLeft, opRight);

    if (updateCall != null) {
      writer.newlineAndIndent();
      writer.keyword("WHEN MATCHED THEN UPDATE");
      final SqlWriter.Frame setFrame =
          writer.startList(
              SqlWriter.FrameTypeEnum.UPDATE_SET_LIST,
              "SET",
              "");

      for (Pair<SqlNode, SqlNode> pair : Pair.zip(
          updateCall.targetColumnList, updateCall.sourceExpressionList)) {
        writer.sep(",");
        SqlIdentifier id = (SqlIdentifier) pair.left;
        id.unparse(writer, opLeft, opRight);
        writer.keyword("=");
        SqlNode sourceExp = pair.right;
        sourceExp.unparse(writer, opLeft, opRight);
      }
      writer.endList(setFrame);
    }

    if (insertCall != null) {
      writer.newlineAndIndent();
      writer.keyword("WHEN NOT MATCHED THEN INSERT");
      if (insertCall.getTargetColumnList() != null) {
        insertCall.getTargetColumnList().unparse(writer, opLeft, opRight);
      }
      insertCall.getSource().unparse(writer, opLeft, opRight);

      writer.endList(frame);
    }
  }

  public void validate(SqlValidator validator, SqlValidatorScope scope) {
    validator.validateMerge(this);
  }
}

// End SqlMerge.java
