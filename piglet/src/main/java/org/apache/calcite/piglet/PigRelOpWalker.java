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
package org.apache.calcite.piglet;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.newplan.PlanWalker;
import org.apache.pig.newplan.logical.relational.LogicalRelationalNodesVisitor;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;

import java.util.Collection;

/**
 * Post-order walker for Pig logical relational plans. Walks the plan
 * from sinks to sources.
 */
class PigRelOpWalker extends PlanWalker {
  /**
   * Visitor that allow doing pre-visit.
   */
  abstract static class PlanPreVisitor extends LogicalRelationalNodesVisitor {
    PlanPreVisitor(OperatorPlan plan, PlanWalker walker) throws FrontendException {
      super(plan, walker);
    }

    /**
     * Called before a node.
     *
     * @param root Pig logical operator to check
     * @return Returns whether the node has been visited before
     */
    public abstract boolean preVisit(LogicalRelationalOperator root);
  }

  PigRelOpWalker(OperatorPlan plan) {
    super(plan);
  }

  @Override public void walk(PlanVisitor planVisitor) throws FrontendException {
    if (!(planVisitor instanceof PigRelOpVisitor)) {
      throw new FrontendException("Expected PigRelOpVisitor", 2223);
    }

    final PigRelOpVisitor pigRelVistor = (PigRelOpVisitor) planVisitor;
    postOrderWalk(pigRelVistor.getCurrentRoot(), pigRelVistor);
  }

  /**
   * Does post-order walk on the Pig logical relational plans from sinks to sources.
   *
   * @param root The root Pig logical relational operator
   * @param visitor The visitor of each Pig logical operator node
   * @throws FrontendException Exception during processing Pig operator
   */
  private void postOrderWalk(Operator root, PlanPreVisitor visitor) throws FrontendException {
    if (root == null || visitor.preVisit((LogicalRelationalOperator) root)) {
      return;
    }

    Collection<Operator> nexts =
        Utils.mergeCollection(plan.getPredecessors(root), plan.getSoftLinkPredecessors(root));
    if (nexts != null) {
      for (Operator op : nexts) {
        postOrderWalk(op, visitor);
      }
    }
    root.accept(visitor);
  }

  @Override public PlanWalker spawnChildWalker(OperatorPlan operatorPlan) {
    return new PigRelOpWalker(operatorPlan);
  }
}

// End PigRelOpWalker.java
