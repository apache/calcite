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
package org.eigenbase.rel;

import java.util.ArrayList;
import java.util.List;

import org.eigenbase.util.Stacks;

import net.hydromatic.linq4j.Ord;

/**
 * Basic implementation of {@link RelShuttle} that calls
 * {@link RelNode#accept(RelShuttle)} on each child, and
 * {@link RelNode#copy(org.eigenbase.relopt.RelTraitSet, java.util.List)} if
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

  public RelNode visit(AggregateRel aggregate) {
    return visitChild(aggregate, 0, aggregate.getChild());
  }

  public RelNode visit(TableAccessRelBase scan) {
    return scan;
  }

  public RelNode visit(TableFunctionRelBase scan) {
    return visitChildren(scan);
  }

  public RelNode visit(ValuesRel values) {
    return values;
  }

  public RelNode visit(FilterRel filter) {
    return visitChild(filter, 0, filter.getChild());
  }

  public RelNode visit(ProjectRel project) {
    return visitChild(project, 0, project.getChild());
  }

  public RelNode visit(JoinRel join) {
    return visitChildren(join);
  }

  public RelNode visit(CorrelatorRel correlator) {
    return visitChildren(correlator);
  }

  public RelNode visit(UnionRel union) {
    return visitChildren(union);
  }

  public RelNode visit(IntersectRel intersect) {
    return visitChildren(intersect);
  }

  public RelNode visit(MinusRel minus) {
    return visitChildren(minus);
  }

  public RelNode visit(SortRel sort) {
    return visitChildren(sort);
  }

  public RelNode visit(RelNode other) {
    return visitChildren(other);
  }
}

// End RelShuttleImpl.java
