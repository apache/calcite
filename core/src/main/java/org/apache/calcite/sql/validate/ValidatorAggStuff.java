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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * WIP for extract agg helper functions.
 */
public class ValidatorAggStuff {


  private final AggFinder aggFinder;
  private final AggFinder aggOrOverFinder;
  private final AggFinder aggOrOverOrGroupFinder;
  private final AggFinder groupFinder;
  private final AggFinder overFinder;

  private final SqlCluster sqlCluster;
  private final ScopeMapImpl scopeMap;


  public ValidatorAggStuff(
      SqlCluster sqlCluster,
      ScopeMapImpl scopeMap) {

    SqlNameMatcher nameMatcher = sqlCluster.getCatalogReader().nameMatcher();
    SqlOperatorTable opTab = sqlCluster.getOpTab();

    this.sqlCluster = sqlCluster;
    this.scopeMap = scopeMap;

    aggFinder = new AggFinder(opTab, false, true, false, null, nameMatcher);
    aggOrOverFinder =
        new AggFinder(opTab, true, true, false, null, nameMatcher);
    overFinder =
        new AggFinder(opTab, true, false, false, aggOrOverFinder, nameMatcher);
    groupFinder = new AggFinder(opTab, false, false, true, null, nameMatcher);
    aggOrOverOrGroupFinder =
        new AggFinder(opTab, true, true, true, null, nameMatcher);
  }

  public @Nullable SqlNode findAgg(SqlNode node) {
    return aggFinder.findAgg(node);
  }

  public Iterable<SqlCall> findAllAgg(SqlNode sqlNode) {
    return aggFinder.findAll(ImmutableList.of(sqlNode));
  }

  public @Nullable SqlNode findOver(SqlNode node) {
    return overFinder.findAgg(node);
  }

  public boolean isAggregate(SqlSelect select) {
    if (getAggregate(select) != null) {
      return true;
    }
    // Also when nested window aggregates are present
    for (SqlCall call : overFinder.findAll(SqlNonNullableAccessors.getSelectList(select))) {
      assert call.getKind() == SqlKind.OVER;
      if (isNestedAggregateWindow(call.operand(0))) {
        return true;
      }
      if (isOverAggregateWindow(call.operand(1))) {
        return true;
      }
    }
    return false;
  }

  protected boolean isNestedAggregateWindow(SqlNode node) {
    AggFinder nestedAggFinder =
        new AggFinder(sqlCluster.getOpTab(), false, false, false, aggFinder,
            sqlCluster.getCatalogReader().nameMatcher());
    return nestedAggFinder.findAgg(node) != null;
  }

  protected boolean isOverAggregateWindow(SqlNode node) {
    return aggFinder.findAgg(node) != null;
  }


  /** Returns the parse tree node (GROUP BY, HAVING, or an aggregate function
   * call) that causes {@code select} to be an aggregate query, or null if it
   * is not an aggregate query.
   *
   * <p>The node is useful context for error messages,
   * but you cannot assume that the node is the only aggregate function. */
  protected @Nullable SqlNode getAggregate(SqlSelect select) {
    SqlNode node = select.getGroup();
    if (node != null) {
      return node;
    }
    node = select.getHaving();
    if (node != null) {
      return node;
    }
    return getAgg(select);
  }

  /** If there is at least one call to an aggregate function, returns the
   * first. */
  private @Nullable SqlNode getAgg(SqlSelect select) {
    final SelectScope selectScope = scopeMap.getRawSelectScope(select);
    if (selectScope != null) {
      final List<SqlNode> selectList = selectScope.getExpandedSelectList();
      if (selectList != null) {
        return aggFinder.findAgg(selectList);
      }
    }
    return aggFinder.findAgg(SqlNonNullableAccessors.getSelectList(select));
  }

  public AggFinder getAggOrOverFinder() {
    return aggOrOverFinder;
  }

  public AggFinder getAggOrOverOrGroupFinder() {
    return aggOrOverOrGroupFinder;
  }

  public AggFinder getGroupFinder() {
    return groupFinder;
  }

  public AggFinder getOverFinder() {
    return overFinder;
  }
}
