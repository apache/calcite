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

import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

import java.util.LinkedList;
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
  @Nullable SqlUpdate updateCall;
  @Nullable SqlInsert insertCall;
  @Nullable SqlDelete deleteCall;
  @Nullable SqlSelect sourceSelect;
  @Nullable SqlIdentifier alias;

  List<SqlCall> callOrderList = new LinkedList<>();

  //~ Constructors -----------------------------------------------------------

  public SqlMerge(SqlParserPos pos,
      SqlNode targetTable,
      SqlNode condition,
      SqlNode source,
      @Nullable SqlUpdate updateCall,
      @Nullable SqlInsert insertCall,
      @Nullable SqlSelect sourceSelect,
      @Nullable SqlIdentifier alias) {
    super(pos);
    this.targetTable = targetTable;
    this.condition = condition;
    this.source = source;
    this.updateCall = updateCall;
    this.insertCall = insertCall;
    this.sourceSelect = sourceSelect;
    this.alias = alias;
  }

  public SqlMerge(SqlParserPos pos,
      SqlNode targetTable,
      SqlNode condition,
      SqlNode source,
      @Nullable SqlUpdate updateCall,
      @Nullable SqlDelete deleteCall,
      @Nullable SqlMergeInsert insertCall,
      @Nullable SqlSelect sourceSelect,
      @Nullable SqlIdentifier alias) {
    super(pos);
    this.targetTable = targetTable;
    this.condition = condition;
    this.source = source;
    this.updateCall = updateCall;
    this.deleteCall = deleteCall;
    this.insertCall = insertCall;
    this.sourceSelect = sourceSelect;
    this.alias = alias;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public SqlKind getKind() {
    return SqlKind.MERGE;
  }

  @SuppressWarnings("nullness")
  @Override public List<@Nullable SqlNode> getOperandList() {
    return ImmutableNullableList.of(targetTable, condition, source, updateCall, deleteCall,
        insertCall, sourceSelect, alias);
  }

  @SuppressWarnings("assignment.type.incompatible")
  @Override public void setOperand(int i, @Nullable SqlNode operand) {
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
      updateCall = (@Nullable SqlUpdate) operand;
      break;
    case 4:
      deleteCall = (@Nullable SqlDelete) operand;
      break;
    case 5:
      insertCall = (@Nullable SqlInsert) operand;
      break;
    case 6:
      sourceSelect = (@Nullable SqlSelect) operand;
      break;
    case 7:
      alias = (SqlIdentifier) operand;
      break;
    default:
      throw new AssertionError(i);
    }
  }

  /** Return the identifier for the target table of this MERGE. */
  public SqlNode getTargetTable() {
    return targetTable;
  }

  /** Returns the alias for the target table of this MERGE. */
  @Pure
  public @Nullable SqlIdentifier getAlias() {
    return alias;
  }

  /** Returns the source query of this MERGE. */
  public SqlNode getSourceTableRef() {
    return source;
  }

  public void setSourceTableRef(SqlNode tableRef) {
    this.source = tableRef;
  }

  /** Returns the UPDATE statement for this MERGE. */
  public @Nullable SqlUpdate getUpdateCall() {
    return updateCall;
  }

  /** Returns the INSERT statement for this MERGE. */
  public @Nullable SqlInsert getInsertCall() {
    return insertCall;
  }

  /** Returns the condition expression to determine whether to UPDATE or
   * INSERT. */
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
  public @Nullable SqlSelect getSourceSelect() {
    return sourceSelect;
  }

  public void setSourceSelect(SqlSelect sourceSelect) {
    this.sourceSelect = sourceSelect;
  }

  public void setCallOrderList(List<SqlCall> callOrderList) {
    this.callOrderList = callOrderList;
  }

  /** Maintaining an call order list. If not mentioned then we follow
   * the default order list of [ UDPATE, DELETE, INSERT ]
   * @return List of callOrderList
   */
  public List<SqlCall> getCallOrderList() {
    if (this.callOrderList.isEmpty()) {
      List<SqlCall> callOrderList = new LinkedList<>();
      callOrderList.add(this.updateCall);
      callOrderList.add(this.deleteCall);
      callOrderList.add(this.insertCall);
      setCallOrderList(callOrderList);
      return callOrderList;
    }

    return this.callOrderList;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    final SqlWriter.Frame frame =
        writer.startList(SqlWriter.FrameTypeEnum.SELECT, "MERGE INTO", "");
    final int opLeft = getOperator().getLeftPrec();
    final int opRight = getOperator().getRightPrec();
    targetTable.unparse(writer, opLeft, opRight);
    SqlIdentifier alias = this.alias;
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

    List<SqlCall> callOrderList = this.getCallOrderList();
    for (SqlCall call: callOrderList) {
      if (call instanceof SqlUpdate) {
        SqlUpdate updateCall = this.updateCall;
        if (updateCall != null) {
          unparseUpdateCall(writer, opLeft, opRight);
        }
      } else if (call instanceof SqlDelete) {
        SqlDelete deleteCall = this.deleteCall;
        if (deleteCall != null) {
          unparseDeleteCall(writer, opLeft, opRight);
        }
      } else if (call instanceof SqlInsert) {
        SqlInsert insertCall = this.insertCall;
        if (insertCall != null) {
          writer.newlineAndIndent();
          writer.keyword("WHEN NOT MATCHED");
          if (this.insertCall instanceof SqlMergeInsert
              && ((SqlMergeInsert) this.insertCall).condition != null) {
            writer.keyword("AND");
            ((SqlMergeInsert) this.insertCall).condition.unparse(writer, opLeft, opRight);
          }
          writer.keyword("THEN INSERT");
          SqlNodeList targetColumnList = insertCall.getTargetColumnList();
          if (targetColumnList != null) {
            targetColumnList.unparse(writer, opLeft, opRight);
          }
          insertCall.getSource().unparse(writer, opLeft, opRight);
          writer.endList(frame);
        }
      }
    }
  }

  private void unparseUpdateCall(SqlWriter writer, int opLeft, int opRight) {
    writer.newlineAndIndent();
    writer.keyword("WHEN MATCHED");
    if (this.updateCall.condition != null) {
      writer.keyword("AND");
      this.updateCall.condition.unparse(writer, opLeft, opRight);
    }
    writer.keyword("THEN UPDATE");
    final SqlWriter.Frame setFrame =
        writer.startList(
            SqlWriter.FrameTypeEnum.UPDATE_SET_LIST,
            "SET",
            "");

    for (Pair<SqlNode, SqlNode> pair : Pair.zip(
        updateCall.targetColumnList, updateCall.sourceExpressionList)) {
      writer.sep(",");
      SqlIdentifier id = (SqlIdentifier) pair.left;
        assert id != null;
      id.unparse(writer, opLeft, opRight);
      writer.keyword("=");
      SqlNode sourceExp = pair.right;
        assert sourceExp != null;
      sourceExp.unparse(writer, opLeft, opRight);
    }
    writer.endList(setFrame);
  }

  private void unparseDeleteCall(SqlWriter writer, int opLeft, int opRight) {
    writer.newlineAndIndent();
    writer.keyword("WHEN MATCHED");
    if (this.deleteCall.condition != null) {
      writer.keyword("AND");
      this.deleteCall.condition.unparse(writer, opLeft, opRight);
    }
    writer.keyword("THEN");
    writer.newlineAndIndent();
    writer.keyword("DELETE");
  }

  @Override public void validate(SqlValidator validator, SqlValidatorScope scope) {
    validator.validateMerge(this);
  }
}
