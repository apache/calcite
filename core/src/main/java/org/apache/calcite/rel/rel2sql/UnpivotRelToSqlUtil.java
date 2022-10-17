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

import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalValues;
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
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.apache.calcite.rel.rel2sql.SqlImplementor.POS;

public class UnpivotRelToSqlUtil {

  /**
   * <p> SQL </p>
   * <blockquote><pre>{@code
   *  SELECT *
   *        FROM emp
   *        UNPIVOT EXCLUDE NULLS (remuneration
   *          FOR remuneration_type IN (comm AS 'commission',
   *                                    sal AS 'salary'))
   *  }</pre></blockquote>
   *
   * <p> Rel creation</p>
   *
   * <blockquote><pre>{@code
   * builder.scan("EMP")
   *         .unpivot(false, ImmutableList.of("REMUNERATION"),
   *             ImmutableList.of("REMUNERATION_TYPE"),
   *             Pair.zip(
   *                 Arrays.asList(ImmutableList.of(builder.literal("commission")),
   *                     ImmutableList.of(builder.literal("salary"))),
   *                 Arrays.asList(ImmutableList.of(builder.field("COMM")),
   *                     ImmutableList.of(builder.field("SAL")))))
   *         .build()
   * }</pre></blockquote>
   *
   * <p> Rel with includeNulls = false after expansion
   *     <blockquote><pre>{@code
   * LogicalProject.NONE.[](input=LogicalFilter#4,inputs=0..6,exprs=[CAST($7):DECIMAL(7, 2) NOT NULL])
   *   LogicalFilter(condition=[IS NOT NULL($7)])
   *     LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], DEPTNO=[$7], REMUNERATION_TYPE=[$8], REMUNERATION=[CASE(=($8, 'commission'), $6, =($8, 'salary'), $5, null:NULL)])
   *       LogicalJoin(condition=[true], joinType=[inner])
   *         LogicalTableScan(table=[[scott, EMP]])
   *         LogicalValues(tuples=[[{ 'commission' }, { 'salary' }]])
   *     }</pre></blockquote>
   *
   */

  protected boolean isRelEquivalentToUnpivotExpansionWithExcludeNulls(Filter filter, SqlImplementor.Builder builder, SqlNode sqlNode) {
    SqlNode filterNode = builder.context.toSql(null, filter.getCondition());
    if (sqlNode instanceof SqlSelect && ((SqlSelect) sqlNode).getFrom() instanceof SqlUnpivot) {
      return isNotNullFilterOnAxisColumn(filterNode, (SqlUnpivot) ((SqlSelect) sqlNode).getFrom());
    } else {
      return false;
    }
  }

  private boolean isNotNullFilterOnAxisColumn(SqlNode filterNode, SqlUnpivot sqlUnpivot) {
    return filterNode.getKind() == SqlKind.IS_NOT_NULL && Objects.equals(((SqlIdentifier) sqlUnpivot.axisList.get(0)).names.get(0), ((SqlIdentifier) ((SqlNode[]) ((SqlBasicCall) filterNode).operands)[0]).names.get(0));
  }

  /**
   * <p> SQL </p>
   * <blockquote><pre>{@code
   *  SELECT *
   *        FROM emp
   *        UNPIVOT INCLUDE NULLS (remuneration
   *          FOR remuneration_type IN (comm AS 'commission',
   *                                    sal AS 'salary'))
   *  }</pre></blockquote>
   *
   * <p> Rel creation</p>
   *
   * <blockquote><pre>{@code
   * builder.scan("EMP")
   *         .unpivot(true, ImmutableList.of("REMUNERATION"),
   *             ImmutableList.of("REMUNERATION_TYPE"),
   *             Pair.zip(
   *                 Arrays.asList(ImmutableList.of(builder.literal("commission")),
   *                     ImmutableList.of(builder.literal("salary"))),
   *                 Arrays.asList(ImmutableList.of(builder.field("COMM")),
   *                     ImmutableList.of(builder.field("SAL")))))
   *         .build()
   * }</pre></blockquote>
   *
   * <p> Rel with includeNulls = true after expansion
   * <blockquote><pre>{@code
   * LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], DEPTNO=[$7],
   * REMUNERATION_TYPE=[$8], REMUNERATION=[CASE(=($8, 'commission'), $6, =($8, 'salary'), $5,
   * null:NULL)])
   *   LogicalJoin(condition=[true], joinType=[inner])
   *     LogicalTableScan(table=[[scott, EMP]])
   *     LogicalValues(tuples=[[{ 'commission' }, { 'salary' }]])
   * }</pre></blockquote>
   *
   */
  protected boolean isRelEquivalentToUnpivotExpansionWithIncludeNulls(Project projectRel, SqlImplementor.Builder builder) {
    // if project has atleast one case op
    // if projects input is join
    // if join with joinType = inner & condition = true
    // & joins right input is values
    // & every value of the value list is used as the 2nd operands of when clause of projection
    return isCaseOperatorPresentInProjectRel(projectRel)
        && isJoinTheInputOfProject(projectRel)
        && isJoinTypeInnerWithTrueCondition((LogicalJoin) projectRel.getInput(0))
        && isRightChildOfJoinIsLogicalValues((LogicalJoin) projectRel.getInput(0))
        && isAtleastOneCaseOperatorEquivalentToUnpivotType(projectRel, builder);
  }

  private boolean isAtleastOneCaseOperatorEquivalentToUnpivotType(Project projectRel, SqlImplementor.Builder builder) {
    boolean caseMatched = false;
    LogicalValues valuesRel = getValuesRel(projectRel);
    String valuesAlias = getValueAlias(valuesRel);
    SqlNodeList valuesList = getValuesList(valuesRel, builder);
    List<RexCall> caseRexCallList = getCaseOperandFromProjection(projectRel);
    for(RexCall caseRex : caseRexCallList) {
      caseMatched = checkCasePatternOfUnpivot(caseRex, projectRel, valuesAlias, valuesList);
      if(caseMatched) break;
    }
    return caseMatched;
  }

  private boolean checkCasePatternOfUnpivot(RexCall caseRex, Project projectRel, String valuesAlias, SqlNodeList valuesList) {
    //case when valuesAlias=value[0] then col1 when valuesAlias=value[1] then col2 ... & so on on else null
    boolean casePatternMatched = false;
    int elseClauseIndex =  caseRex.getOperands().size()-1;
    for(int i = 0,j = 0; i < elseClauseIndex; i+=2, j++) {
      List<RexNode> operands = ((RexCall)(caseRex.operands.get(i))).getOperands();
      int indexOfFirstOp = ((RexInputRef)(operands.get(0))).getIndex();
      String nameOfFirstOp = projectRel.getInput(0).getRowType().getFieldNames().get(indexOfFirstOp);
      casePatternMatched = Objects.equals(nameOfFirstOp, valuesAlias)
          && Objects.equals(operands.get(1).toString(), valuesList.get(j).toString());
      if(casePatternMatched == false) break;
    }
    return casePatternMatched && caseRex.getOperands().get(elseClauseIndex).getType().getSqlTypeName() == SqlTypeName.NULL ;
  }

  protected String getValueAlias(Values valuesRel) {
    return valuesRel.getRowType().getFieldNames().get(0);
  }

  private boolean isCaseOperatorPresentInProjectRel(Project projectRel) {
    return projectRel.getProjects().stream().anyMatch(projection -> projection instanceof RexCall && ((RexCall) projection).op instanceof SqlCaseOperator);
  }

  private boolean isJoinTheInputOfProject(Project projectRel) {
    return projectRel.getInput(0) instanceof Join;
  }

  private boolean isJoinTypeInnerWithTrueCondition(Join joinRel) {
    return joinRel.getJoinType() == JoinRelType.INNER && joinRel.getCondition().isAlwaysTrue();
  }

  private boolean isRightChildOfJoinIsLogicalValues(Join joinRel) {
    return joinRel.getRight() instanceof Values;
  }

  protected LogicalValues getValuesRel(Project projectRel) {
    Join joinRel = (LogicalJoin) projectRel.getInput(0);
    return (LogicalValues)joinRel.getRight();
  }

  protected String getAliasOfCase(Project projectRel) {
    String aliasOfCase = "";
    for (int i = 0; i<  projectRel.getProjects().size(); i++) {
      if (projectRel.getProjects().get(i) instanceof RexCall && ((RexCall) projectRel.getProjects().get(i)).op instanceof SqlCaseOperator) {
        aliasOfCase = projectRel.getRowType().getFieldNames().get(i);
      }
    }
    return aliasOfCase;
  }


  private List<RexCall> getCaseOperandFromProjection(Project projectRel) {
    List<RexCall> caseRexCalls = new ArrayList<>();
    for (RexNode projection : projectRel.getProjects()) {
      if (projection instanceof RexCall && ((RexCall) projection).op instanceof SqlCaseOperator) {
        caseRexCalls.add((RexCall) projection);
      }
    }
    return caseRexCalls;
  }

  protected SqlNodeList getThenClauseSqlNodeList(Project projectRel, SqlImplementor.Builder builder) {
    SqlNodeList thenClauseSqlNodeList = new SqlNodeList(POS);
    for (RexNode projection : projectRel.getProjects()) {
      if (projection instanceof RexCall && ((RexCall) projection).op instanceof SqlCaseOperator) {
        SqlCase sqlCase = (SqlCase) builder.context.toSql(null, projection);
        List<SqlNode> thenList = sqlCase.getThenOperands().getList();
        thenClauseSqlNodeList.addAll(thenList);
      }
    }
    return thenClauseSqlNodeList;
  }

  protected SqlNodeList getValuesList(LogicalValues logicalValuesRel, SqlImplementor.Builder builder) {
    SqlNodeList valueSqlNodeList = new SqlNodeList(POS);
    for (ImmutableList<RexLiteral> value : logicalValuesRel.tuples.asList()) {
      SqlNode valueSqlNode = builder.context.toSql(null, value.get(0));
      valueSqlNodeList.add(valueSqlNode);
    }
    return valueSqlNodeList;
  }
}
