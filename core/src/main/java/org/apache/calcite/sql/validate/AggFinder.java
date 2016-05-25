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
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.util.Util;

import com.google.common.collect.Lists;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

/**
 * Visitor which looks for an aggregate function inside a tree of
 * {@link SqlNode} objects.
 */
class AggFinder extends SqlBasicVisitor<Void> {
  //~ Instance fields --------------------------------------------------------

  // Maximum allowed nesting level of aggregates
  private static final int MAX_AGG_LEVEL = 2;
  private final SqlOperatorTable opTab;
  private final boolean over;

  private boolean nestedAgg;                        // Allow nested aggregates

  // Stores aggregate nesting level while visiting the tree to keep track of
  // nested aggregates within window aggregates. An explicit stack is used
  // instead of recursion to obey the SqlVisitor interface
  private Deque<Integer> aggLevelStack;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an AggFinder.
   *
   * @param opTab Operator table
   * @param over Whether to find windowed function calls {@code Agg(x) OVER
   *             windowSpec}
   */
  AggFinder(SqlOperatorTable opTab, boolean over) {
    this.opTab = opTab;
    this.over = over;
    this.nestedAgg = false;
    this.aggLevelStack = new ArrayDeque<Integer>();
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Allows nested aggregates within window aggregates
   */
  public void enableNestedAggregates()  {
    this.nestedAgg = true;
    this.aggLevelStack.clear();
  }

  /**
   * Disallows nested aggregates within window aggregates
   */
  public void disableNestedAggregates()  {
    this.nestedAgg = false;
    this.aggLevelStack.clear();
  }

  public void addAggLevel(int aggLevel) {
    aggLevelStack.push(aggLevel);
  }

  public void removeAggLevel() {
    if (!aggLevelStack.isEmpty()) {
      aggLevelStack.pop();
    }
  }

  public int getAggLevel() {
    if (!aggLevelStack.isEmpty()) {
      return aggLevelStack.peek();
    } else {
      return -1;
    }
  }

  public boolean isEmptyAggLevel() {
    return aggLevelStack.isEmpty();
  }

  /**
   * Finds an aggregate.
   *
   * @param node Parse tree to search
   * @return First aggregate function in parse tree, or null if not found
   */
  public SqlNode findAgg(SqlNode node) {
    try {
      node.accept(this);
      return null;
    } catch (Util.FoundOne e) {
      Util.swallow(e, null);
      return (SqlNode) e.getNode();
    }
  }

  public SqlNode findAgg(List<SqlNode> nodes) {
    try {
      for (SqlNode node : nodes) {
        node.accept(this);
      }
      return null;
    } catch (Util.FoundOne e) {
      Util.swallow(e, null);
      return (SqlNode) e.getNode();
    }
  }

  public Void visit(SqlCall call) {
    final SqlOperator operator = call.getOperator();
    final int parAggLevel = this.getAggLevel(); //parent aggregate nesting level
    // If nested aggregates disallowed or found an aggregate at invalid level
    if (operator.isAggregator()) {
      if (!nestedAgg || (parAggLevel + 1) > MAX_AGG_LEVEL) {
        throw new Util.FoundOne(call);
      } else {
        if (parAggLevel >= 0) {
          this.addAggLevel(parAggLevel + 1);
        }
      }
    } else {
      // Add the parent aggregate level before visiting its children
      if (parAggLevel >= 0) {
        this.addAggLevel(parAggLevel);
      }
    }
    // User-defined function may not be resolved yet.
    if (operator instanceof SqlFunction
        && ((SqlFunction) operator).getFunctionType()
        == SqlFunctionCategory.USER_DEFINED_FUNCTION) {
      final List<SqlOperator> list = Lists.newArrayList();
      opTab.lookupOperatorOverloads(((SqlFunction) operator).getSqlIdentifier(),
          SqlFunctionCategory.USER_DEFINED_FUNCTION, SqlSyntax.FUNCTION, list);
      for (SqlOperator sqlOperator : list) {
        if (sqlOperator.isAggregator()) {
          // If nested aggregates disallowed or found aggregate at invalid level
          if (!nestedAgg || (parAggLevel + 1) > MAX_AGG_LEVEL) {
            throw new Util.FoundOne(call);
          } else {
            if (parAggLevel >= 0) {
              this.addAggLevel(parAggLevel + 1);
            }
          }
        }
      }
    }
    if (call.isA(SqlKind.QUERY)) {
      // don't traverse into queries
      return null;
    }
    if (call.getKind() == SqlKind.OVER) {
      if (over) {
        throw new Util.FoundOne(call);
      } else {
        // an aggregate function over a window is not an aggregate!
        return null;
      }
    }
    super.visit(call);
    // Remove the parent aggregate level after visiting its children
    if (parAggLevel >= 0) {
      this.removeAggLevel();
    }
    return null;
  }
}

// End AggFinder.java
