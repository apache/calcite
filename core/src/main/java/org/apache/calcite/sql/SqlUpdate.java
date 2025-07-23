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

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * A <code>SqlUpdate</code> is a node of a parse tree which represents an UPDATE
 * statement.
 */
public class SqlUpdate extends SqlCall {
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("UPDATE", SqlKind.UPDATE) {
        @SuppressWarnings("argument.type.incompatible")
        @Override public SqlCall createCall(@Nullable SqlLiteral functionQualifier,
            SqlParserPos pos,
            @Nullable SqlNode... operands) {
          return new SqlUpdate(
              pos,
              operands[0],
              (SqlNodeList) operands[1],
              (SqlNodeList) operands[2],
              operands[3],
              null,
              (SqlIdentifier) operands[4]);
        }
      };

  SqlNode targetTable;
  SqlNodeList targetColumnList;
  SqlNodeList sourceExpressionList;
  @Nullable SqlNode condition;
  @Nullable SqlSelect sourceSelect;
  @Nullable SqlIdentifier alias;

  //~ Constructors -----------------------------------------------------------

  public SqlUpdate(SqlParserPos pos,
      SqlNode targetTable,
      SqlNodeList targetColumnList,
      SqlNodeList sourceExpressionList,
      @Nullable SqlNode condition,
      @Nullable SqlSelect sourceSelect,
      @Nullable SqlIdentifier alias) {
    super(pos);
    this.targetTable = requireNonNull(targetTable, "targetTable");
    this.targetColumnList =
        requireNonNull(targetColumnList, "targetColumnList");
    this.sourceExpressionList =
        requireNonNull(sourceExpressionList, "sourceExpressionList");
    this.condition = condition;
    this.sourceSelect = sourceSelect;
    assert sourceExpressionList.size() == targetColumnList.size();
    this.alias = alias;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlKind getKind() {
    return SqlKind.UPDATE;
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @SuppressWarnings("nullness")
  @Override public List<@Nullable SqlNode> getOperandList() {
    return ImmutableNullableList.of(targetTable, targetColumnList,
        sourceExpressionList, condition, alias);
  }

  @SuppressWarnings("assignment.type.incompatible")
  @Override public void setOperand(int i, @Nullable SqlNode operand) {
    switch (i) {
    case 0:
      assert operand instanceof SqlIdentifier;
      targetTable = operand;
      break;
    case 1:
      targetColumnList = requireNonNull((SqlNodeList) operand);
      break;
    case 2:
      sourceExpressionList = requireNonNull((SqlNodeList) operand);
      break;
    case 3:
      condition = operand;
      break;
    case 4:
      sourceExpressionList = requireNonNull((SqlNodeList) operand);
      break;
    case 5:
      alias = (SqlIdentifier) operand;
      break;
    default:
      throw new AssertionError(i);
    }
  }

  /** Returns the identifier for the target table of this UPDATE. */
  public SqlNode getTargetTable() {
    return targetTable;
  }

  /** Returns the alias for the target table of this UPDATE. */
  @Pure
  public @Nullable SqlIdentifier getAlias() {
    return alias;
  }

  public void setAlias(SqlIdentifier alias) {
    this.alias = alias;
  }

  /** Returns the list of target column names. */
  public SqlNodeList getTargetColumnList() {
    return targetColumnList;
  }

  /** Returns the list of source expressions. */
  public SqlNodeList getSourceExpressionList() {
    return sourceExpressionList;
  }

  /**
   * Gets the filter condition for rows to be updated.
   *
   * @return the condition expression for the data to be updated, or null for
   * all rows in the table
   */
  public @Nullable SqlNode getCondition() {
    return condition;
  }

  /**
   * Gets the source SELECT expression for the data to be updated. Returns
   * null before the statement has been expanded by
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

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    final SqlWriter.Frame frame =
        writer.startList(SqlWriter.FrameTypeEnum.SELECT, "UPDATE", "");
    final int opLeft = getOperator().getLeftPrec();
    final int opRight = getOperator().getRightPrec();
    targetTable.unparse(writer, opLeft, opRight);
    SqlIdentifier alias = this.alias;
    if (alias != null) {
      writer.keyword("AS");
      alias.unparse(writer, opLeft, opRight);
    }
    final SqlWriter.Frame setFrame =
        writer.startList(SqlWriter.FrameTypeEnum.UPDATE_SET_LIST, "SET", "");
    for (Pair<SqlNode, SqlNode> pair
        : Pair.zip(getTargetColumnList(), getSourceExpressionList())) {
      writer.sep(",");
      SqlIdentifier id = (SqlIdentifier) pair.left;
      id.unparse(writer, opLeft, opRight);
      writer.keyword("=");
      SqlNode sourceExp = pair.right;
      sourceExp.unparse(writer, opLeft, opRight);
    }
    writer.endList(setFrame);
    SqlNode condition = this.condition;
    if (condition != null) {
      writer.sep("WHERE");
      condition.unparse(writer, opLeft, opRight);
    }
    writer.endList(frame);
  }

  @Override public void validate(SqlValidator validator, SqlValidatorScope scope) {
    validator.validateUpdate(this);
  }
}
