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

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * A <code>SqlDelete</code> is a node of a parse tree which represents a DELETE
 * statement.
 */
public class SqlDelete extends SqlCall {
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("DELETE", SqlKind.DELETE) {
        @SuppressWarnings("argument.type.incompatible")
        @Override public SqlCall createCall(
            @Nullable SqlLiteral functionQualifier,
            SqlParserPos pos,
            @Nullable SqlNode... operands) {
          return new SqlDelete(
              pos,
              operands[0],
              operands[1],
              (SqlSelect) operands[2],
              (SqlIdentifier) operands[3]);
        }
      };

  SqlNode targetTable;
  @Nullable SqlNode condition;
  @Nullable SqlSelect sourceSelect;
  @Nullable SqlIdentifier alias;

  //~ Constructors -----------------------------------------------------------

  public SqlDelete(
      SqlParserPos pos,
      SqlNode targetTable,
      @Nullable SqlNode condition,
      @Nullable SqlSelect sourceSelect,
      @Nullable SqlIdentifier alias) {
    super(pos);
    this.targetTable = targetTable;
    this.condition = condition;
    this.sourceSelect = sourceSelect;
    this.alias = alias;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlKind getKind() {
    return SqlKind.DELETE;
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @SuppressWarnings("nullness")
  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(targetTable, condition, sourceSelect, alias);
  }

  @SuppressWarnings("assignment.type.incompatible")
  @Override public void setOperand(int i, @Nullable SqlNode operand) {
    switch (i) {
    case 0:
      targetTable = operand;
      break;
    case 1:
      condition = operand;
      break;
    case 2:
      sourceSelect = (SqlSelect) operand;
      break;
    case 3:
      alias = (SqlIdentifier) operand;
      break;
    default:
      throw new AssertionError(i);
    }
  }

  /**
   * Returns the identifier for the target table of the deletion.
   */
  public SqlNode getTargetTable() {
    return targetTable;
  }

  /**
   * Returns the alias for the target table of the deletion.
   */
  public @Nullable SqlIdentifier getAlias() {
    return alias;
  }

  /**
   * Gets the filter condition for rows to be deleted.
   *
   * @return the condition expression for the data to be deleted, or null for
   * all rows in the table
   */
  public @Nullable SqlNode getCondition() {
    return condition;
  }

  /**
   * Gets the source SELECT expression for the data to be deleted. This
   * returns null before the condition has been expanded by
   * {@link SqlValidatorImpl#performUnconditionalRewrites(SqlNode, boolean)}.
   *
   * @return the source SELECT for the data to be inserted
   */
  public @Nullable SqlSelect getSourceSelect() {
    return sourceSelect;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    final SqlWriter.Frame frame =
        writer.startList(SqlWriter.FrameTypeEnum.SELECT, "DELETE FROM", "");
    final int opLeft = getOperator().getLeftPrec();
    final int opRight = getOperator().getRightPrec();
    targetTable.unparse(writer, opLeft, opRight);
    SqlIdentifier alias = this.alias;
    if (alias != null) {
      writer.keyword("AS");
      alias.unparse(writer, opLeft, opRight);
    }
    SqlNode condition = this.condition;
    if (condition != null) {
      writer.sep("WHERE");
      condition.unparse(writer, opLeft, opRight);
    }
    writer.endList(frame);
  }

  @Override public void validate(SqlValidator validator, SqlValidatorScope scope) {
    validator.validateDelete(this);
  }

  public void setSourceSelect(SqlSelect sourceSelect) {
    this.sourceSelect = sourceSelect;
  }
}
