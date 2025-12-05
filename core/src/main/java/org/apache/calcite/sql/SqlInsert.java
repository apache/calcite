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
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * A <code>SqlInsert</code> is a node of a parse tree which represents an INSERT
 * statement.
 */
public class SqlInsert extends SqlCall {
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("INSERT", SqlKind.INSERT) {
        @SuppressWarnings("argument.type.incompatible")
        @Override public SqlCall createCall(@Nullable SqlLiteral functionQualifier,
            SqlParserPos pos,
            @Nullable SqlNode... operands) {
          return new SqlInsert(
              pos,
              (SqlNodeList) operands[0],
              operands[1],
              operands[2],
              (SqlNodeList) operands[3]);
        }
      };

  SqlNodeList keywords;
  SqlNode targetTable;
  SqlNode source;
  @Nullable SqlNodeList columnList;

  //~ Constructors -----------------------------------------------------------

  public SqlInsert(SqlParserPos pos,
      SqlNodeList keywords,
      SqlNode targetTable,
      SqlNode source,
      @Nullable SqlNodeList columnList) {
    super(pos);
    this.keywords = requireNonNull(keywords, "keywords");
    this.targetTable = targetTable;
    this.source = source;
    this.columnList = columnList;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlKind getKind() {
    return SqlKind.INSERT;
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @SuppressWarnings("nullness")
  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(keywords, targetTable, source, columnList);
  }

  /** Returns whether this is an UPSERT statement.
   *
   * <p>In SQL, this is represented using the {@code UPSERT} keyword rather than
   * {@code INSERT}; in the abstract syntax tree, an UPSERT is indicated by the
   * presence of a {@link SqlInsertKeyword#UPSERT} keyword. */
  public final boolean isUpsert() {
    return getModifierNode(SqlInsertKeyword.UPSERT) != null;
  }

  @SuppressWarnings("assignment.type.incompatible")
  @Override public void setOperand(int i, @Nullable SqlNode operand) {
    switch (i) {
    case 0:
      keywords = (SqlNodeList) operand;
      break;
    case 1:
      assert operand instanceof SqlIdentifier;
      targetTable = operand;
      break;
    case 2:
      source = operand;
      break;
    case 3:
      columnList = (SqlNodeList) operand;
      break;
    default:
      throw new AssertionError(i);
    }
  }

  /**
   * Return the identifier for the target table of the insertion.
   */
  public SqlNode getTargetTable() {
    return targetTable;
  }

  /**
   * Returns the source expression for the data to be inserted.
   */
  public SqlNode getSource() {
    return source;
  }

  public void setSource(SqlSelect source) {
    this.source = source;
  }

  /**
   * Returns the list of target column names, or null for all columns in the
   * target table.
   */
  @Pure
  public @Nullable SqlNodeList getTargetColumnList() {
    return columnList;
  }

  public final @Nullable SqlNode getModifierNode(SqlInsertKeyword modifier) {
    for (SqlNode keyword : keywords) {
      SqlInsertKeyword keyword2 =
          ((SqlLiteral) keyword).symbolValue(SqlInsertKeyword.class);
      if (keyword2 == modifier) {
        return keyword;
      }
    }
    return null;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SELECT);
    writer.sep(isUpsert() ? "UPSERT INTO" : "INSERT INTO");
    final int opLeft = getOperator().getLeftPrec();
    final int opRight = getOperator().getRightPrec();
    targetTable.unparse(writer, opLeft, opRight);
    if (columnList != null) {
      columnList.unparse(writer, opLeft, opRight);
    }
    writer.newlineAndIndent();
    source.unparse(writer, 0, 0);
    writer.endList(frame);
  }

  @Override public void validate(SqlValidator validator, SqlValidatorScope scope) {
    validator.validateInsert(this);
  }
}
