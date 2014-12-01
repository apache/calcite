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
package org.apache.calcite.sql.validate;

import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.calcite.sql.SqlUtil.stripAs;

/**
 * Scope for resolving identifiers within a SELECT statement that has a
 * GROUP BY clause.
 *
 * <p>The same set of identifiers are in scope, but it won't allow access to
 * identifiers or expressions which are not group-expressions.
 */
public class AggregatingSelectScope
    extends DelegatingScope implements AggregatingScope {
  //~ Instance fields --------------------------------------------------------

  private final SqlSelect select;
  private final boolean distinct;

  /** Use while under construction. */
  private final List<SqlNode> temporaryGroupExprList = Lists.newArrayList();

  /** Use after construction is complete. Assigned from
   * {@link #temporaryGroupExprList} towards the end of the constructor. */
  public final ImmutableList<SqlNode> groupExprList;
  public final ImmutableBitSet groupSet;
  public final ImmutableList<ImmutableBitSet> groupSets;
  public final boolean indicator;
  public final Map<Integer, Integer> groupExprProjection;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an AggregatingSelectScope
   *
   * @param selectScope Parent scope
   * @param select      Enclosing SELECT node
   * @param distinct    Whether SELECT is DISTINCT
   */
  AggregatingSelectScope(
      SqlValidatorScope selectScope,
      SqlSelect select,
      boolean distinct) {
    // The select scope is the parent in the sense that all columns which
    // are available in the select scope are available. Whether they are
    // valid as aggregation expressions... now that's a different matter.
    super(selectScope);
    this.select = select;
    this.distinct = distinct;
    final Map<Integer, Integer> groupExprProjection = Maps.newHashMap();
    final ImmutableList.Builder<ImmutableList<ImmutableBitSet>> builder =
        ImmutableList.builder();
    if (select.getGroup() != null) {
      // We deep-copy the group-list in case subsequent validation
      // modifies it and makes it no longer equivalent. While copying,
      // we fully qualify all identifiers.
      final SqlNodeList groupList =
          SqlValidatorUtil.DeepCopier.copy(parent, select.getGroup());
      for (SqlNode groupExpr : groupList) {
        SqlValidatorUtil.analyzeGroupItem(this, temporaryGroupExprList,
            groupExprProjection, builder, groupExpr);
      }
    }
    this.groupExprList = ImmutableList.copyOf(temporaryGroupExprList);
    this.groupExprProjection = ImmutableMap.copyOf(groupExprProjection);

    final Set<ImmutableBitSet> flatGroupSets =
        Sets.newTreeSet(ImmutableBitSet.COMPARATOR);
    for (List<ImmutableBitSet> groupSet : Linq4j.product(builder.build())) {
      flatGroupSets.add(ImmutableBitSet.union(groupSet));
    }

    // For GROUP BY (), we need a singleton grouping set.
    if (flatGroupSets.isEmpty()) {
      flatGroupSets.add(ImmutableBitSet.of());
    }

    this.groupSet = ImmutableBitSet.range(groupExprList.size());
    this.groupSets = ImmutableList.copyOf(flatGroupSets);
    this.indicator = !groupSets.equals(ImmutableList.of(groupSet));
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Returns the expressions that are in the GROUP BY clause (or the SELECT
   * DISTINCT clause, if distinct) and that can therefore be referenced
   * without being wrapped in aggregate functions.
   *
   * <p>The expressions are fully-qualified, and any "*" in select clauses are
   * expanded.
   *
   * @return list of grouping expressions
   */
  private ImmutableList<SqlNode> getGroupExprs() {
    if (distinct) {
      // Cannot compute this in the constructor: select list has not been
      // expanded yet.
      assert select.isDistinct();

      // Remove the AS operator so the expressions are consistent with
      // OrderExpressionExpander.
      ImmutableList.Builder<SqlNode> groupExprs = ImmutableList.builder();
      final SelectScope selectScope = (SelectScope) parent;
      for (SqlNode selectItem : selectScope.getExpandedSelectList()) {
        groupExprs.add(stripAs(selectItem));
      }
      return groupExprs.build();
    } else if (select.getGroup() != null) {
      if (groupExprList != null) {
        return groupExprList;
      } else {
        return ImmutableList.copyOf(temporaryGroupExprList);
      }
    } else {
      return ImmutableList.of();
    }
  }

  public SqlNode getNode() {
    return select;
  }

  /** Returns whether a field should be nullable due to grouping sets. */
  public boolean isNullable(int i) {
    return i < groupExprList.size() && !allContain(groupSets, i);
  }

  private static boolean allContain(List<ImmutableBitSet> bitSets, int bit) {
    for (ImmutableBitSet bitSet : bitSets) {
      if (!bitSet.get(bit)) {
        return false;
      }
    }
    return true;
  }

  @Override public RelDataType nullifyType(SqlNode node, RelDataType type) {
    for (Ord<SqlNode> groupExpr : Ord.zip(groupExprList)) {
      if (groupExpr.e.equalsDeep(node, false)) {
        if (isNullable(groupExpr.i)) {
          return validator.getTypeFactory().createTypeWithNullability(type,
              true);
        }
      }
    }
    return type;
  }

  public SqlValidatorScope getOperandScope(SqlCall call) {
    if (call.getOperator().isAggregator()) {
      // If we're the 'SUM' node in 'select a + sum(b + c) from t
      // group by a', then we should validate our arguments in
      // the non-aggregating scope, where 'b' and 'c' are valid
      // column references.
      return parent;
    } else {
      // Check whether expression is constant within the group.
      //
      // If not, throws. Example, 'empno' in
      //    SELECT empno FROM emp GROUP BY deptno
      //
      // If it perfectly matches an expression in the GROUP BY
      // clause, we validate its arguments in the non-aggregating
      // scope. Example, 'empno + 1' in
      //
      //   SELECT empno + 1 FROM emp GROUP BY empno + 1

      final boolean matches = checkAggregateExpr(call, false);
      if (matches) {
        return parent;
      }
    }
    return super.getOperandScope(call);
  }

  public boolean checkAggregateExpr(SqlNode expr, boolean deep) {
    // Fully-qualify any identifiers in expr.
    if (deep) {
      expr = validator.expand(expr, this);
    }

    // Make sure expression is valid, throws if not.
    List<SqlNode> groupExprs = getGroupExprs();
    final AggChecker aggChecker =
        new AggChecker(
            validator,
            this,
            groupExprs,
            distinct);
    if (deep) {
      expr.accept(aggChecker);
    }

    // Return whether expression exactly matches one of the group
    // expressions.
    return aggChecker.isGroupExpr(expr);
  }

  public void validateExpr(SqlNode expr) {
    checkAggregateExpr(expr, true);
  }

  /** Returns whether a given expression is equal to one of the grouping
   * expressions. Determines whether it is valid as an operand to GROUPING. */
  public boolean isGroupingExpr(SqlNode operand) {
    return lookupGroupingExpr(operand) >= 0;
  }

  public int lookupGroupingExpr(SqlNode operand) {
    for (Ord<SqlNode> groupExpr : Ord.zip(groupExprList)) {
      if (operand.equalsDeep(groupExpr.e, false)) {
        return groupExpr.i;
      }
    }
    return -1;
  }
}

// End AggregatingSelectScope.java
