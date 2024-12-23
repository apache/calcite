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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
    init();
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
    final int operatorLeftPrec = getOperator().getLeftPrec();
    final int operatorRightPrec = getOperator().getRightPrec();

    List<SqlNode> sources = getSources();
    targetTable.unparse(writer, operatorLeftPrec, operatorRightPrec);
    unparseTargetAlias(writer, operatorLeftPrec, operatorRightPrec);
    unparseSetClause(writer, operatorLeftPrec, operatorRightPrec);
    unparseSources(writer, operatorLeftPrec, operatorRightPrec, sources);
    unparseCondition(writer, operatorLeftPrec, operatorRightPrec);

    writer.endList(frame);
  }

  /**
   * This @return single or multiple sources used by update statement
   * This update target table also.
   */
  private List<SqlNode> getSources() {
    List<SqlNode> sources = new ArrayList<>();
    if (sourceSelect != null && sourceSelect.from != null) {
      Optional<SqlJoin> join = getJoinFromSourceSelect();
      if (join.isPresent()) {
        sources = sqlKindSourceCollectorMap.get(join.get().getKind()).collectSources(join.get());
      }
    }
    return sources;
  }

  private Optional<SqlJoin> getJoinFromSourceSelect() {
    if (sourceSelect.from.getKind() == SqlKind.AS
        && ((SqlBasicCall) sourceSelect.from).getOperandList().get(0) instanceof SqlJoin) {
      return Optional.of((SqlJoin) ((SqlBasicCall) sourceSelect.from).getOperandList().get(0));
    }
    return sourceSelect.from instanceof SqlJoin
        ? Optional.of((SqlJoin) sourceSelect.from) : Optional.empty();
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
    } else if (node.getKind() == SqlKind.AS) {
      return ((SqlBasicCall) node).getOperandList().get(0).equalsDeep(targetTable, Litmus.IGNORE);
    }
    return targetTable instanceof SqlBasicCall && targetTable.getKind() == SqlKind.AS
        && ((SqlBasicCall) targetTable).getOperandList().get(0).equalsDeep(node, Litmus.IGNORE);
  }

  private void unparseTargetAlias(SqlWriter writer, int operatorLeftPrec, int operatorRightPrec) {
    if (alias != null) {
      writer.keyword("AS");
      alias.unparse(writer, operatorLeftPrec, operatorRightPrec);
    }
  }

  private void unparseSetClause(SqlWriter writer, int opLeft, int opRight) {
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

  private void unparseSources(SqlWriter writer, int opLeft, int opRight, List<SqlNode> sources) {
    if (!sources.isEmpty()) {
      writer.keyword("FROM");
      final SqlWriter.Frame sourcesFrame = writer.startList("", "");
      for (SqlNode source: sources) {
        writer.sep(",");
        source.unparse(writer, opLeft, opRight);
      }
      writer.endList(sourcesFrame);
      unparseSourceAlias(sources, writer, opLeft, opRight);
    }
  }

  private void unparseSourceAlias(
      List<SqlNode> sources, SqlWriter writer, int opLeft, int opRight) {
    if (sources.size() == 1) {
      Optional<SqlIdentifier> aliasForFromClause = getAliasForFromClause();
      if (aliasForFromClause.isPresent()) {
        writer.keyword("AS");
        aliasForFromClause.get().unparse(writer, opLeft, opRight);
      }
    }
  }

  private Optional<SqlIdentifier> getAliasForFromClause() {
    if (sourceSelect != null && sourceSelect.from != null) {
      if (sourceSelect.from instanceof SqlBasicCall && sourceSelect.from.getKind() == SqlKind.AS) {
        return Optional.of((SqlIdentifier) ((SqlBasicCall) sourceSelect.from).
                getOperandList().get(1));
      }
    }
    return Optional.empty();
  }

  private void unparseCondition(SqlWriter writer, int opLeft, int opRight) {
    if (condition != null) {
      writer.sep("WHERE");
      condition.unparse(writer, opLeft, opRight);
    }
  }

  /**
   * Collect Sources for Update Statement.
   *
   * Examples:
   *
   * Example 1: When there is only one source.
   *
   * A. When source is CTE (SqlWith)
   *
   * sourceSelect: SELECT *
   * FROM ((WITH `CTE1` () AS (SELECT *
   * FROM `foodmart`.`empDeptBoolTableDup`) (SELECT `CTE10`.`ID`, `CTE10`.`DEPT_ID`,
   * `CTE10`.`NAME`, `CTE10`.`BOOL_DATA`
   * FROM `CTE1` AS `CTE10`
   * INNER JOIN `foodmart`.`trimmed_employee` AS `trimmed_employee0` ON `CTE10`.`ID` =
   * `trimmed_employee0`.`EMPLOYEE_ID`
   * WHERE NVL(`trimmed_employee0`.`DEPARTMENT_ID`, CAST('' AS NUMERIC)) <> NVL(`CTE10`.`DEPT_ID`,
   * CAST('' AS NUMERIC)))) INNER JOIN `foodmart`.`empDeptBoolTable` ON
   * TRUE) AS `t0`
   * WHERE `empDeptBoolTable`.`ID` = `t0`.`ID`
   *
   * Here source is :
   * WITH `CTE1` () AS (SELECT *
   * FROM `foodmart`.`empDeptBoolTableDup`) (SELECT `CTE10`.`ID`, `CTE10`.`DEPT_ID`,
   * `CTE10`.`NAME`, `CTE10`.`BOOL_DATA`
   * FROM `CTE1` AS `CTE10`
   * INNER JOIN `foodmart`.`trimmed_employee` AS `trimmed_employee0` ON `CTE10`.`ID` =
   * `trimmed_employee0`.`EMPLOYEE_ID`
   * WHERE NVL(`trimmed_employee0`.`DEPARTMENT_ID`, CAST('' AS NUMERIC)) <> NVL(`CTE10``DEPT_ID`,
   * CAST('' AS NUMERIC)))
   *
   * B: When source is a Table (SqlIdentifier) -
   *
   * sourceSelect: SELECT `employee`.`EMPLOYEE_ID`, `employee`.`FIRST_NAME`,
   * `table1`.`ID`, `table1`.`NAME`, `table1`.`EMP_ID`,
   * `table1`.`EMPLOYEE_ID` AS `EMPLOYEE_ID0`,
   * `table1`.`FIRST_NAME` AS `FIRST_NAME0`, `employee`.`EMPLOYEE_ID`
   *  AS `EMPLOYEE_ID1` FROM `foodmart`.`employee`
   *  INNER JOIN `foodmart`.`table1` ON TRUE
   *  WHERE `table1`.`NAME` = 'Derrick' AND `employee`.`FIRST_NAME` = 'Derrick'
   *
   * Here source is : `foodmart`.`employee`
   *
   * Example 2: When there are multiple sources:
   *
   * sourceSelect: SELECT `table1update`.`id`, `table1update`.`name`, table2update`.`id` AS `id0`,
   * `table2update`.`name` AS `name0`, `trimmed_employee`.`employee_id`,`
   * `trimmed_employee`.`first_name`, 10 AS `$f23`, `table1update`.`name` AS `name1`,
   * `table2update`.`middlename` AS `middlename0` FROM `foodmart`.`table1update`
   * INNER JOIN `foodmart`.`table2update` ON TRUE
   * INNER JOIN `foodmart`.`trimmed_employee` ON TRUE
   * WHERE LOWER(`trimmed_employee`.`first_name`) = LOWER(`table1update`.`name`)
   * AND `table2update`.`id` = `trimmed_employee`.`employee_id`
   *
   * Here sources are: `foodmart`.`trimmed_employee`, `foodmart`.`table2update`
   *
   * @param <T> is type of SqlNode
   */
  private interface SourceCollector<T extends SqlNode> {
    List<SqlNode> collectSources(T node);
  }

  private final Map<SqlKind, SourceCollector> sqlKindSourceCollectorMap = new HashMap<>();

  private final SourceCollector collectSourcesFromIdentifier = node ->
      isTargetTable(node) ? new ArrayList<>() : new ArrayList<>(Arrays.asList(node));

  private final SourceCollector collectSourcesFromAs = node ->
      isTargetTable(node) ? new ArrayList<>() : new ArrayList<>(Arrays.asList(node));

  private final SourceCollector collectSourcesFromWithOrSelect = node ->
      new ArrayList<>(Arrays.asList(node));

  private final SourceCollector collectSourcesFromJoin = node -> {
    SqlNode right = ((SqlJoin) node).right;
    SqlNode left = ((SqlJoin) node).left;
    List<SqlNode> sources = sqlKindSourceCollectorMap.get(right.getKind()).collectSources(right);
    sources.addAll(sqlKindSourceCollectorMap.get(left.getKind()).collectSources(left));
    return sources;
  };

  private void init() {
    sqlKindSourceCollectorMap.put(SqlKind.JOIN, collectSourcesFromJoin);
    sqlKindSourceCollectorMap.put(SqlKind.IDENTIFIER, collectSourcesFromIdentifier);
    sqlKindSourceCollectorMap.put(SqlKind.AS, collectSourcesFromAs);
    sqlKindSourceCollectorMap.put(SqlKind.WITH, collectSourcesFromWithOrSelect);
    sqlKindSourceCollectorMap.put(SqlKind.SELECT, collectSourcesFromWithOrSelect);  }

  @Override public void validate(SqlValidator validator, SqlValidatorScope scope) {
    validator.validateUpdate(this);
  }
}
