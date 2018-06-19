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
package org.apache.calcite.rel;

import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMatch;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Basic implementation of {@link RelShuttle} that calls
 * {@link RelNode#accept(RelShuttle)} on each child, and
 * {@link RelNode#copy(org.apache.calcite.plan.RelTraitSet, java.util.List)} if
 * any children change.
 */
public class RelShuttleImpl implements RelShuttle {

  protected final Deque<Boolean> childrenChangedFlagStack = new ArrayDeque<>();

  protected final Deque<List<RelNode>> childrenStack = new ArrayDeque<>();

  @Override public boolean visit(RelNode node) {
    boolean proceed = doVisit(node);
    childrenChangedFlagStack.push(false);
    childrenStack.push(new ArrayList<>());
    return proceed;
  }

  public boolean doVisit(RelNode node) {
    if (node instanceof LogicalAggregate) {
      return visitLogicalAggregate((LogicalAggregate) node);
    }
    if (node instanceof LogicalMatch) {
      return visitLogicalMatch((LogicalMatch) node);
    }
    if (node instanceof TableScan) {
      return visitTableScan((TableScan) node);
    }
    if (node instanceof TableFunctionScan) {
      return visitTableFunctionScan((TableFunctionScan) node);
    }
    if (node instanceof LogicalValues) {
      return visitLogicalValues((LogicalValues) node);
    }
    if (node instanceof LogicalFilter) {
      return visitLogicalFilter((LogicalFilter) node);
    }
    if (node instanceof LogicalProject) {
      return visitLogicalProject((LogicalProject) node);
    }
    if (node instanceof LogicalJoin) {
      return visitLogicalJoin((LogicalJoin) node);
    }
    if (node instanceof LogicalCorrelate) {
      return visitLogicalCorrelate((LogicalCorrelate) node);
    }
    if (node instanceof LogicalUnion) {
      return visitLogicalUnion((LogicalUnion) node);
    }
    if (node instanceof LogicalIntersect) {
      return visitLogicalIntersect((LogicalIntersect) node);
    }
    if (node instanceof LogicalMinus) {
      return visitLogicalMinus((LogicalMinus) node);
    }
    if (node instanceof LogicalSort) {
      return visitLogicalSort((LogicalSort) node);
    }
    if (node instanceof LogicalExchange) {
      return visitLogicalExchange((LogicalExchange) node);
    }
    // TODO support unsupported node types
    return visitRelNode(node);
  }

  public boolean visitLogicalAggregate(LogicalAggregate aggregate) {
    return true;
  }

  public boolean visitLogicalMatch(LogicalMatch match) {
    return true;
  }

  public boolean visitTableScan(TableScan scan) {
    return true;
  }

  public boolean visitTableFunctionScan(TableFunctionScan scan) {
    return true;
  }

  public boolean visitLogicalValues(LogicalValues values) {
    return true;
  }

  public boolean visitLogicalFilter(LogicalFilter filter) {
    return true;
  }

  public boolean visitLogicalProject(LogicalProject project) {
    return true;
  }

  public boolean visitLogicalJoin(LogicalJoin join) {
    return true;
  }

  public boolean visitLogicalCorrelate(LogicalCorrelate correlate) {
    return true;
  }

  public boolean visitLogicalUnion(LogicalUnion union) {
    return true;
  }

  public boolean visitLogicalIntersect(LogicalIntersect intersect) {
    return true;
  }

  public boolean visitLogicalMinus(LogicalMinus minus) {
    return true;
  }

  public boolean visitLogicalSort(LogicalSort sort) {
    return true;
  }

  public boolean visitLogicalExchange(LogicalExchange exchange) {
    return true;
  }

  public boolean visitRelNode(RelNode node) {
    return true;
  }

  @Override public RelNode leave(RelNode node) {
    RelNode next = doLeave(node);
    if (childrenChangedFlagStack.peek()) {
      next = next.copy(next.getTraitSet(), childrenStack.peek());
    }
    childrenChangedFlagStack.pop();
    childrenStack.pop();
    if (!childrenStack.isEmpty()) {
      childrenStack.peek().add(next);
      boolean isChanged = childrenChangedFlagStack.pop();
      childrenChangedFlagStack.push(isChanged || node != next);
    }
    return next;
  }

  public RelNode doLeave(RelNode node) {
    if (node instanceof LogicalAggregate) {
      return leaveLogicalAggregate((LogicalAggregate) node);
    }
    if (node instanceof LogicalMatch) {
      return leaveLogicalMatch((LogicalMatch) node);
    }
    if (node instanceof TableScan) {
      return leaveTableScan((TableScan) node);
    }
    if (node instanceof TableFunctionScan) {
      return leaveTableFunctionScan((TableFunctionScan) node);
    }
    if (node instanceof LogicalValues) {
      return leaveLogicalValues((LogicalValues) node);
    }
    if (node instanceof LogicalFilter) {
      return leaveLogicalFilter((LogicalFilter) node);
    }
    if (node instanceof LogicalProject) {
      return leaveLogicalProject((LogicalProject) node);
    }
    if (node instanceof LogicalJoin) {
      return leaveLogicalJoin((LogicalJoin) node);
    }
    if (node instanceof LogicalCorrelate) {
      return leaveLogicalCorrelate((LogicalCorrelate) node);
    }
    if (node instanceof LogicalUnion) {
      return leaveLogicalUnion((LogicalUnion) node);
    }
    if (node instanceof LogicalIntersect) {
      return leaveLogicalIntersect((LogicalIntersect) node);
    }
    if (node instanceof LogicalMinus) {
      return leaveLogicalMinus((LogicalMinus) node);
    }
    if (node instanceof LogicalSort) {
      return leaveLogicalSort((LogicalSort) node);
    }
    if (node instanceof LogicalExchange) {
      return leaveLogicalExchange((LogicalExchange) node);
    }
    // TODO support unsupported node types
    return leaveRelNode(node);

  }

  public RelNode leaveLogicalAggregate(LogicalAggregate aggregate) {
    return aggregate;
  }

  public RelNode leaveLogicalMatch(LogicalMatch match) {
    return match;
  }

  public RelNode leaveTableScan(TableScan scan) {
    return scan;
  }

  public RelNode leaveTableFunctionScan(TableFunctionScan scan) {
    return scan;
  }

  public RelNode leaveLogicalValues(LogicalValues values) {
    return values;
  }

  public RelNode leaveLogicalFilter(LogicalFilter filter) {
    return filter;
  }

  public RelNode leaveLogicalProject(LogicalProject project) {
    return project;
  }

  public RelNode leaveLogicalJoin(LogicalJoin join) {
    return join;
  }

  public RelNode leaveLogicalCorrelate(LogicalCorrelate correlate) {
    return correlate;
  }

  public RelNode leaveLogicalUnion(LogicalUnion union) {
    return union;
  }

  public RelNode leaveLogicalIntersect(LogicalIntersect intersect) {
    return intersect;
  }

  public RelNode leaveLogicalMinus(LogicalMinus minus) {
    return minus;
  }

  public RelNode leaveLogicalSort(LogicalSort sort) {
    return sort;
  }

  public RelNode leaveLogicalExchange(LogicalExchange exchange) {
    return exchange;
  }

  public RelNode leaveRelNode(RelNode node) {
    return node;
  }

}

// End RelShuttleImpl.java
