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
package org.apache.calcite.rel.rel2sql;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUnpivot;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlCaseOperator;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Litmus;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.calcite.rel.rel2sql.SqlImplementor.POS;

/**
 * Class to identify Rel structure which is of UNPIVOT Type.
 */
public class UnpivotRelToSqlUtil {

  /**
   * <p> SQL. </p>
   * <blockquote><pre>{@code
   *  SELECT *
   *        FROM sales
   *        UNPIVOT EXCLUDE NULLS (monthly_sales
   *          FOR month IN (jan_sales AS 'jan',
   *                                    feb_sales AS 'feb',
   *                                        mar_sales AS 'mar'))
   *  }</pre></blockquote>
   *
   * <p> Rel creation</p>
   *
   * <blockquote><pre>{@code
   * builder
   *         .scan("sales")
   *         .unpivot(false, ImmutableList.of("monthly_sales"),//value_column(measureList)
   *             ImmutableList.of("month"),//unpivot_column(axisList)
   *             Pair.zip(
   *                 Arrays.asList(ImmutableList.of(builder.literal("jan")),//column_alias
   *                     ImmutableList.of(builder.literal("feb")),
   *                     ImmutableList.of(builder.literal("march"))),
   *                 Arrays.asList(ImmutableList.of(builder.field("jan_sales")),//column_list
   *                     ImmutableList.of(builder.field("feb_sales")),
   *                     ImmutableList.of(builder.field("mar_sales")))))
   *         .build();
   * }</pre></blockquote>
   *
   * <p> Rel with includeNulls = false after expansion
   * <blockquote><pre>{@code
   * LogicalProject(id=[$0], year=[$1], month=[$2], monthly_sales=[CAST($3):JavaType(int) NOT NULL])
   *   LogicalFilter(condition=[IS NOT NULL($3)])
   *     LogicalProject(id=[$0], year=[$1], month=[$5],
   *     monthly_sales=[CASE(=($5, 'jan'), $2, =($5, 'feb'), $3, =($5, 'march'), $4, null:NULL)])
   *       LogicalJoin(condition=[true], joinType=[inner])
   *         LogicalTableScan(table=[[SALESSCHEMA, sales]])
   *         LogicalValues(tuples=[[{ 'jan' }, { 'feb' }, { 'march' }]])
   *     }</pre></blockquote>
   **/

  protected boolean isRelEquivalentToUnpivotExpansionWithExcludeNulls(
      SqlNode filterNode,
      SqlNode sqlNode) {
    if (sqlNode instanceof SqlSelect && ((SqlSelect) sqlNode).getFrom() instanceof SqlUnpivot) {
      return isFilterNodeEquivalentToUnpivotExpansion(filterNode,
          ((SqlUnpivot) ((SqlSelect) sqlNode).getFrom()).measureList);
    } else {
      return false;
    }
  }

  /**
   * Check if filter node is equivalent to UNPIVOT's expansion when INCLUDE NULLS is false.
   */
  private boolean isFilterNodeEquivalentToUnpivotExpansion(
      SqlNode filterNode, SqlNodeList measureColumnList) {
    SqlNode[] filterOperands =
            ((SqlBasicCall) filterNode).getOperandList().toArray(SqlNode.EMPTY_ARRAY);

    if (measureColumnList.size() > 1) {
      return isNotNullPresentOnAllMeasureColumns(filterNode, measureColumnList, filterOperands);
    } else {
      return isNotNullPresentOnSingleMeasureColumn(filterNode, measureColumnList, filterOperands);
    }
  }

  /**
   * Check if filter node is equivalent to UNPIVOT's expansion
   * when there are multiple measure columns.
   * -if there are multiple measure columns, on unpivot expansion each of the
   * measure columns have NOT NULL filter on them separated by OR
   * ex- measureList(monthly_sales,monthly_expense)
   * then on expansion it becomes monthly_sales IS NOT NULL OR monthly_expense IS NOT NULL
   */
  private boolean isNotNullPresentOnAllMeasureColumns(
      SqlNode filterNode, SqlNodeList measureColumnList, SqlNode[] filterOperands) {
    List<String> measureColumnNames =
        measureColumnList.stream()
            .map(measureColumn -> ((SqlIdentifier) measureColumn).names.get(0))
            .collect(Collectors.toList());
    List<String> filterColumnNames =
        IntStream.range(0, ((SqlBasicCall) filterNode).operandCount())
            .filter(i -> (filterOperands[i]).getKind() == SqlKind.IS_NOT_NULL)
            .mapToObj(
                i -> (
                    (SqlIdentifier)
                        ((SqlBasicCall)
                filterOperands[i]).getOperandList().get(0)).names.get(0))
            .collect(Collectors.toList());
    return filterNode.getKind() == SqlKind.OR
        && filterColumnNames.containsAll(measureColumnNames);
  }

  /**
   * Check if filter node is equivalent to UNPIVOT's expansion when there is single measure column.
   * -if there is single measure column, on unpivot expansion
   * measure column has NOT NULL filter on it
   * ex- measureList(monthly_sales)
   * then on expansion it becomes monthly_sales IS NOT NULL
   */
  private boolean isNotNullPresentOnSingleMeasureColumn(
      SqlNode filterNode, SqlNodeList measureColumnList, SqlNode[] filterOperands) {
    return filterNode.getKind() == SqlKind.IS_NOT_NULL
        && Objects.equals(((SqlIdentifier) measureColumnList.get(0)).names.get(0),
        ((SqlIdentifier) filterOperands[0]).names.get(0));
  }

  /**
   * <p> SQL. </p>
   * <blockquote><pre>{@code
   *  SELECT *
   *        FROM sales
   *        UNPIVOT INCLUDE NULLS (monthly_sales
   *          FOR month IN (jan_sales AS 'jan',
   *                                    feb_sales AS 'feb',
   *                                        mar_sales AS 'mar'))
   *  }</pre></blockquote>
   *
   * <p> Rel creation</p>
   *
   * <blockquote><pre>{@code
   * builder
   *         .scan("sales")
   *             .unpivot(true, ImmutableList.of("monthly_sales"),//value_column(measureList)
   *             ImmutableList.of("month"),//unpivot_column(axisList)
   *                 Pair.zip(
   *                     Arrays.asList(ImmutableList.of(builder.literal("jan")),//column_alias
   *              ImmutableList.of(builder.literal("feb")),
   *              ImmutableList.of(builder.literal("march"))),
   *                     Arrays.asList(ImmutableList.of(builder.field("jan_sales")),//column_list
   *              ImmutableList.of(builder.field("feb_sales")),
   *              ImmutableList.of(builder.field("mar_sales")))))
   *      .build();
   * }</pre></blockquote>
   *
   * <p> Rel with includeNulls = true after expansion </p>
   * <blockquote><pre>{@code
   * LogicalProject(id=[$0], year=[$1], month=[$5],
   * monthly_sales=[CASE(=($5, 'jan'), $2, =($5, 'feb'), $3, =($5, 'march'), $4, null:NULL)])
   *   LogicalJoin(condition=[true], joinType=[inner])
   *     LogicalTableScan(table=[[SALESSCHEMA, sales]])
   *     LogicalValues(tuples=[[{ 'jan' }, { 'feb' }, { 'march' }]])
   * }</pre></blockquote>
   **/
  protected boolean isRelEquivalentToUnpivotExpansionWithIncludeNulls(
      Project projectRel,
      SqlImplementor.Builder builder) {
    // If Project has at least one case op
    // If Project's input is Join
    // If Join with joinType = inner & condition = true
    // & Join's right input is LogicalValues
    // If at least one case is equivalent to UNPIVOT expansion
    return isCaseOperatorPresentInProjectRel(projectRel)
        && isLogicalJoinInputOfProjectRel(projectRel)
        && isJoinTypeInnerWithTrueCondition((LogicalJoin) projectRel.getInput(0))
        && isRightChildOfJoinIsLogicalValues((LogicalJoin) projectRel.getInput(0))
        && isAtleastOneCaseOperatorEquivalentToUnpivotType(projectRel, builder);
  }

  /**
   * Check each case operator if it is equivalent to UNPIVOT expansion of case,
   * and if it matches return true
   * If measure column is a list ,then in that case there are multiple case operators.
   */
  private boolean isAtleastOneCaseOperatorEquivalentToUnpivotType(
      Project projectRel,
      SqlImplementor.Builder builder) {
    Map<String, SqlNodeList> caseAliasVsThenList = getCaseAliasVsThenList(projectRel, builder);
    return caseAliasVsThenList.size() > 0;

  }

  /**
   * Check each case operator if it is equivalent to UNPIVOT expansion of case.
   * And if it matches ,then populate a map with case alias as key and value as
   * the list of then operands
   */
  protected Map<String, SqlNodeList> getCaseAliasVsThenList(
      Project projectRel,
      SqlImplementor.Builder builder) {
    Map<String, SqlNodeList> caseAliasVsThenList = new LinkedHashMap<>();
    Map<RexCall, String> caseRexCallVsAliasMap = getCaseRexCallFromProjectionWithAlias(projectRel);

    for (RexCall caseRex : caseRexCallVsAliasMap.keySet()) {
      boolean caseMatched = isCasePatternOfUnpivotType(caseRex, projectRel, builder);
      if (caseMatched) {
        SqlNodeList thenClauseSqlNodeList = new SqlNodeList(POS);
        SqlCase sqlCase = (SqlCase) builder.context.toSql(null, caseRex);
        List<SqlNode> thenList = sqlCase.getThenOperands().getList();
        thenClauseSqlNodeList.addAll(thenList);
        caseAliasVsThenList.put(caseRexCallVsAliasMap.get(caseRex), thenClauseSqlNodeList);
      }
    }
    return caseAliasVsThenList;
  }

  /**
   * Check if Case Rex pattern equivalent to UNPIVOT expansion.
   */
  private boolean isCasePatternOfUnpivotType(
      RexCall caseRex, Project projectRel, SqlImplementor.Builder builder) {
    //case when LogicalValuesRelAlias=logicalValuesRel[0] then col1 when
    // LogicalValuesRelAlias=logicalValuesRel[1]
    // then col2 ... & so on on else null
    LogicalValues logicalValuesRel = getLogicalValuesRel(projectRel);
    String logicalValuesAlias = getLogicalValueAlias(logicalValuesRel);
    SqlNodeList logicalValuesList = getLogicalValuesList(logicalValuesRel, builder);
    if (isElseClausePresentInCaseRex(caseRex)
        && isLogicalValuesSizeEqualsWhenClauseSize(logicalValuesList, caseRex)) {
      return isCaseAndLogicalValuesPatternMatching(caseRex, projectRel, logicalValuesAlias,
          logicalValuesList);
    } else {
      return false;
    }
  }

  /**
   * Check if case pattern & logical values pattern are equivalent to UNPIVOT expansion.
   * ex- case when month='jan' then jan_sales
   * when month='feb' then feb_sales
   * when month='mar' then march_sales
   * else null
   * AS monthly_sales
   *
   * LogicalValues('jan','feb','mar') AS month
   */
  private boolean isCaseAndLogicalValuesPatternMatching(
      RexCall caseRex, Project projectRel, String logicalValuesAlias,
      SqlNodeList logicalValuesList) {
    boolean casePatternMatched = false;
    int elseClauseIndex = caseRex.getOperands().size() - 1;
    for (int i = 0, j = 0; i < elseClauseIndex; i += 2, j++) {
      List<RexNode> whenOperandList = ((RexCall) (caseRex.operands.get(i))).getOperands();
      if (whenOperandList.size() == 2 && whenOperandList.get(0) instanceof RexInputRef) {
        int indexOfLeftOperandOfWhen = ((RexInputRef) (whenOperandList.get(0))).getIndex();
        String nameOfLeftOperandOfWhen = projectRel.getInput(0).getRowType().getFieldNames()
            .get(indexOfLeftOperandOfWhen);
        casePatternMatched = Objects.equals(nameOfLeftOperandOfWhen, logicalValuesAlias)
            && Objects.equals(whenOperandList.get(1).toString(),
            logicalValuesList.get(j).toString())
            && caseRex.getOperands().get(elseClauseIndex).getType().getSqlTypeName()
            == SqlTypeName.NULL;
        if (!casePatternMatched) {
          break;
        }
      }
    }
    return casePatternMatched;
  }

  private boolean isElseClausePresentInCaseRex(RexCall caseRex) {
    return caseRex.operands.size() % 2 != 0;
  }

  private boolean isLogicalValuesSizeEqualsWhenClauseSize(
      SqlNodeList logicalValuesList, RexCall caseRexCall) {
    int whenClauseCount = (caseRexCall.operands.size() - 1) / 2;
    return logicalValuesList.size() == whenClauseCount;
  }

  protected String getLogicalValueAlias(Values valuesRel) {
    return valuesRel.getRowType().getFieldNames().get(0);
  }

  private boolean isCaseOperatorPresentInProjectRel(Project projectRel) {
    return projectRel.getProjects().stream().anyMatch
        (projection -> projection instanceof RexCall && ((RexCall) projection)
            .op instanceof SqlCaseOperator);
  }

  private boolean isLogicalJoinInputOfProjectRel(Project projectRel) {
    return projectRel.getInput(0) instanceof Join;
  }

  private boolean isJoinTypeInnerWithTrueCondition(Join joinRel) {
    return joinRel.getJoinType() == JoinRelType.INNER && joinRel.getCondition().isAlwaysTrue();
  }

  private boolean isRightChildOfJoinIsLogicalValues(Join joinRel) {
    return joinRel.getRight() instanceof Values;
  }

  protected LogicalValues getLogicalValuesRel(Project projectRel) {
    Join joinRel = (LogicalJoin) projectRel.getInput(0);
    return (LogicalValues) joinRel.getRight();
  }

  /**
   * Fetch all the case operands from projection with case aliases.
   */
  private Map<RexCall, String> getCaseRexCallFromProjectionWithAlias(Project projectRel) {
    Map<RexCall, String> caseRexCallVsAlias = new LinkedHashMap<>();
    for (int i = 0; i < projectRel.getProjects().size(); i++) {
      RexNode projectRex = projectRel.getProjects().get(i);
      if (projectRex instanceof RexCall
          && ((RexCall) projectRex).op instanceof SqlCaseOperator) {
        caseRexCallVsAlias.put((RexCall) projectRex,
            projectRel.getRowType().getFieldNames().get(i));
      }
    }
    return caseRexCallVsAlias;
  }

  protected SqlNodeList getLogicalValuesList(
      LogicalValues logicalValuesRel,
      SqlImplementor.Builder builder) {
    SqlNodeList valueSqlNodeList = new SqlNodeList(POS);
    for (ImmutableList<RexLiteral> value : logicalValuesRel.tuples) {
      SqlNode valueSqlNode = builder.context.toSql(null, value.get(0));
      valueSqlNodeList.add(valueSqlNode);
    }
    return valueSqlNodeList;
  }

  /**
   * Check if the project can be converted to * in case of SqlUnpivot.
   */
  protected boolean isStarInUnPivot(Project projectRel, SqlImplementor.Result result) {
    boolean isStar = false;
    if (result.node instanceof SqlSelect
        && ((SqlSelect) result.node).getFrom() instanceof SqlUnpivot) {
      List<RexNode> projectionExpressions = projectRel.getProjects();
      RelDataType inputRowType = projectRel.getInput().getRowType();
      RelDataType projectRowType = projectRel.getRowType();

      if (inputRowType.getFieldNames().size() == projectRowType.getFieldNames().size()
          && RelOptUtil.eq("project type", projectRowType,
          "project input type", inputRowType, Litmus.IGNORE)) {
        SqlUnpivot sqlUnpivot = (SqlUnpivot) ((SqlSelect) result.node).getFrom();
        List<String> measureColumnNames =
            sqlUnpivot.measureList.stream()
                .map(measureColumn -> ((SqlIdentifier) measureColumn).names.get(0))
                .collect(Collectors.toList());
        List<String> castColumns = new ArrayList<>();
        for (RexNode rex : projectionExpressions) {
          if (rex instanceof RexCall && ((RexCall) rex).op.kind == SqlKind.CAST) {
            castColumns.add(getColumnNameFromCast(rex, inputRowType));
          }
        }
        isStar = castColumns.containsAll(measureColumnNames);
      }
      return isStar;
    }
    return false;
  }

  private String getColumnNameFromCast(RexNode rex, RelDataType inputRowType) {
    String columnName = "";
    if (((RexCall) rex).operands.get(0) instanceof RexInputRef) {
      int index = ((RexInputRef) ((RexCall) rex).operands.get(0)).getIndex();
      columnName = inputRowType.getFieldNames().get(index);
    }
    return columnName;
  }

  /**
   * Create inList for {@link SqlUnpivot}.
   */
  protected SqlNodeList getInListForSqlUnpivot(
      SqlNodeList measureList, SqlNodeList aliasOfInSqlNodeList, SqlNodeList inSqlNodeList) {
    if (measureList.size() > 1) {
      return createAliasedColumnListTypeOfInListForSqlUnpivot(aliasOfInSqlNodeList, inSqlNodeList);
    } else {
      return createAliasedInListForSqlUnpivot(aliasOfInSqlNodeList, inSqlNodeList);
    }
  }

  /**
   * If there are multiple measure columns.
   * then inList will have multiple column's data with single alias
   * corresponding to each measure column
   * ex-measureList(monthly_sales, monthly_expense)
   * then inList corresponding to monthly_sales will be (jan_sales,jan_expense) as jan and so on
   */
  private SqlNodeList createAliasedColumnListTypeOfInListForSqlUnpivot(
      SqlNodeList aliasOfInSqlNodeList, SqlNodeList inSqlNodeList) {
    SqlNodeList aliasedInSqlNodeList = new SqlNodeList(POS);

    for (int i = 0; i < aliasOfInSqlNodeList.size(); i++) {
      List<SqlIdentifier> sqlIdentifierList = new ArrayList<>();
      for (int j = 0; j < inSqlNodeList.size(); j++) {
        SqlNodeList sqlNodeList = (SqlNodeList) inSqlNodeList.get(j);
        sqlIdentifierList.add(
            new SqlIdentifier(((SqlIdentifier) sqlNodeList.get(i)).names.get(1), POS));
      }
      aliasedInSqlNodeList.add(
          SqlStdOperatorTable.AS.createCall(POS,
              SqlLibraryOperators.PARENTHESIS.createCall
                  (POS, sqlIdentifierList), aliasOfInSqlNodeList.get(i)));
    }
    return aliasedInSqlNodeList;
  }

  /**
   * If there is a single measure column ,then inList is a simple list with alias.
   * ex- measureList(monthly_sales)
   * then inList is jan_sales as jan and so on
   */
  private SqlNodeList createAliasedInListForSqlUnpivot(
      SqlNodeList aliasOfInSqlNodeList, SqlNodeList inSqlNodeList) {
    SqlNodeList aliasedInSqlNodeList = new SqlNodeList(POS);

    for (int i = 0; i < aliasOfInSqlNodeList.size(); i++) {
      SqlNodeList identifierList = (SqlNodeList) inSqlNodeList.get(0);
      SqlIdentifier columnName =
          new SqlIdentifier(((SqlIdentifier) identifierList.get(i)).names.get(1), POS);
      aliasedInSqlNodeList.add(
          SqlStdOperatorTable.AS.createCall(POS, columnName,
              aliasOfInSqlNodeList.get(i)));
    }
    return aliasedInSqlNodeList;
  }
}
