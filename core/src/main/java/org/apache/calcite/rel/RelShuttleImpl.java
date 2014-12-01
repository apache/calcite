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

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.core.Correlator;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.util.Stacks;

import java.util.ArrayList;
import java.util.List;

/**
 * Basic implementation of {@link RelShuttle} that calls
 * {@link RelNode#accept(RelShuttle)} on each child, and
 * {@link RelNode#copy(org.apache.calcite.plan.RelTraitSet, java.util.List)} if
 * any children change.
 */
public class RelShuttleImpl implements RelShuttle {
  protected final List<RelNode> stack = new ArrayList<RelNode>();

  /**
   * Visits a particular child of a parent.
   */
  protected RelNode visitChild(RelNode parent, int i, RelNode child) {
    Stacks.push(stack, parent);
    try {
      RelNode child2 = child.accept(this);
      if (child2 != child) {
        final List<RelNode> newInputs =
            new ArrayList<RelNode>(parent.getInputs());
        newInputs.set(i, child2);
        return parent.copy(parent.getTraitSet(), newInputs);
      }
      return parent;
    } finally {
      Stacks.pop(stack, parent);
    }
  }

  protected RelNode visitChildren(RelNode rel) {
    for (Ord<RelNode> input : Ord.zip(rel.getInputs())) {
      rel = visitChild(rel, input.i, input.e);
    }
    return rel;
  }

  public RelNode visit(LogicalAggregate aggregate) {
    return visitChild(aggregate, 0, aggregate.getInput());
  }

  public RelNode visit(TableScan scan) {
    return scan;
  }

  public RelNode visit(TableFunctionScan scan) {
    return visitChildren(scan);
  }

  public RelNode visit(LogicalValues values) {
    return values;
  }

  public RelNode visit(LogicalFilter filter) {
    return visitChild(filter, 0, filter.getInput());
  }

  public RelNode visit(LogicalProject project) {
    return visitChild(project, 0, project.getInput());
  }

  public RelNode visit(LogicalJoin join) {
    return visitChildren(join);
  }

  public RelNode visit(Correlator correlator) {
    return visitChildren(correlator);
  }

  public RelNode visit(LogicalUnion union) {
    return visitChildren(union);
  }

  public RelNode visit(LogicalIntersect intersect) {
    return visitChildren(intersect);
  }

  public RelNode visit(LogicalMinus minus) {
    return visitChildren(minus);
  }

  public RelNode visit(Sort sort) {
    return visitChildren(sort);
  }

  public RelNode visit(RelNode other) {
    return visitChildren(other);
  }
}

// End RelShuttleImpl.java
