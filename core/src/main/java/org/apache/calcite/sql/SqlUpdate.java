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
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

import java.util.ArrayList;
import java.util.List;

/**
 * A <code>SqlUpdate</code> is a node of a parse tree which represents an UPDATE
 * statement.
 */
public class SqlUpdate extends SqlCall {
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("UPDATE", SqlKind.UPDATE);

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
    this.targetTable = targetTable;
    this.targetColumnList = targetColumnList;
    this.sourceExpressionList = sourceExpressionList;
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
      targetColumnList = (SqlNodeList) operand;
      break;
    case 2:
      sourceExpressionList = (SqlNodeList) operand;
      break;
    case 3:
      condition = operand;
      break;
    case 4:
      sourceExpressionList = (SqlNodeList) operand;
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

  public void setTargetTable(SqlNode targetTable) {
    this.targetTable = targetTable;
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

    List<SqlNode> sources = new ArrayList<>();

    if (sourceSelect != null && sourceSelect.from != null) {
      SqlJoin join = getJoinFromSourceSelect();
      if (join != null) {
        setTargetAndSources(join, sources);
      }
    }

    targetTable.unparse(writer, opLeft, opRight);

    SqlIdentifier alias = this.alias;
    if (alias != null) {
      writer.keyword("AS");
      alias.unparse(writer, opLeft, opRight);
    }

    unparseSet(writer, opLeft, opRight);
    if (!sources.isEmpty()) {
      unparseUpdateSources(sources, writer, opLeft, opRight);
    }
    unparseUpdateCondition(writer, opLeft, opRight);

    writer.endList(frame);
  }

  private SqlJoin getJoinFromSourceSelect() {
    SqlJoin join = null;
    if (sourceSelect.from instanceof SqlBasicCall && sourceSelect.from.getKind() == SqlKind.AS
        && ((SqlBasicCall) sourceSelect.from).operands[0] instanceof SqlJoin) {
      join = (SqlJoin) ((SqlBasicCall) sourceSelect.from).operands[0];
    } else if (sourceSelect.from instanceof SqlJoin) {
      join = (SqlJoin) sourceSelect.from;
    }
    return join;
  }

  private void setTargetAndSources(SqlNode node, List<SqlNode> sources) {
    switch (node.getKind()) {
    case JOIN:
      setTargetAndSources((SqlJoin) node, sources);
      break;
    case IDENTIFIER:
      setTargetAndSources((SqlIdentifier) node, sources);
      break;
    case AS:
      setTargetAndSources((SqlBasicCall) node, sources);
      break;
    case WITH:
      setTargetAndSources((SqlWith) node, sources);
      break;
    }
  }

  private void setTargetAndSources(SqlJoin node, List<SqlNode> sources) {
    setTargetAndSources(node.right, sources);
    setTargetAndSources(node.left, sources);
  }

  private void setTargetAndSources(SqlIdentifier node, List<SqlNode> sources) {
    if (!isTargetTable(node)) {
      sources.add(node);
    }
  }

  private void setTargetAndSources(SqlBasicCall node, List<SqlNode> sources) {
    if (isTargetTable(node)) {
      setTargetTable(node);
    } else {
      sources.add(node);
    }
  }

  /**
   * This method will @return true when:
   * 1. If the targetTable and the @param node is exactly same
   * 2. If @param node is aliased and its first operand and targetTable are same
   * 3. If targetTable is aliased and its first operand and @param node are same.
   */
  private boolean isTargetTable(SqlNode node) {
    if (node.equalsDeep(targetTable, Litmus.IGNORE)) {
      return true;
    } else if (node instanceof SqlBasicCall) {
      return ((SqlBasicCall) node).operands[0].equalsDeep(targetTable, Litmus.IGNORE);
    }
    return targetTable instanceof SqlBasicCall && targetTable.getKind() == SqlKind.AS
        && ((SqlBasicCall) targetTable).operands[0].equalsDeep(node, Litmus.IGNORE);
  }

  private void setTargetAndSources(SqlWith node, List<SqlNode> sources) {
    sources.add(node);
  }

  private void unparseSet(SqlWriter writer, int opLeft, int opRight) {
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
  }

  private void unparseUpdateSources(
      List<SqlNode> sources, SqlWriter writer, int opLeft, int opRight) {
    writer.keyword("FROM");
    for (int index = 0; index < sources.size(); index++) {
      SqlNode source = sources.get(index);
      source.unparse(writer, opLeft, opRight);
      if (sources.size() - 1 != index) {
        writer.keyword(",");
      }
    }

    if (sources.size() == 1) {
      SqlIdentifier fromAlias = getAliasForFromClause();
      if (fromAlias != null) {
        writer.keyword("AS");
        fromAlias.unparse(writer, opLeft, opRight);
      }
    }
  }

  private SqlIdentifier getAliasForFromClause() {
    if (sourceSelect != null && sourceSelect.from != null) {
      if (sourceSelect.from instanceof SqlBasicCall && sourceSelect.from.getKind() == SqlKind.AS) {
        return (SqlIdentifier) ((SqlBasicCall) sourceSelect.from).operands[1];
      }
    }
    return null;
  }

  private void unparseUpdateCondition(SqlWriter writer, int opLeft, int opRight) {
    SqlNode condition = this.condition;
    if (condition != null) {
      writer.sep("WHERE");
      condition.unparse(writer, opLeft, opRight);
    }
  }

  @Override public void validate(SqlValidator validator, SqlValidatorScope scope) {
    validator.validateUpdate(this);
  }
}
