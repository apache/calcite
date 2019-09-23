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
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;

import java.util.Collection;

/**
 * Post-order walker for Pig expression plans. Walk the plan from
 * source to sinks.
 */
class PigRelExWalker extends PlanWalker {
  PigRelExWalker(OperatorPlan plan) {
    super(plan);
  }

  @Override public void walk(PlanVisitor planVisitor) throws FrontendException {
    if (!(planVisitor instanceof PigRelExVisitor)) {
      throw new FrontendException("Expected PigRelOpVisitor", 2223);
    }
    if (!(getPlan() instanceof LogicalExpressionPlan)) {
      throw new FrontendException("Expected LogicalExpressionPlan", 2223);
    }

    final PigRelExVisitor pigRelVistor = (PigRelExVisitor) planVisitor;
    final LogicalExpressionPlan plan = (LogicalExpressionPlan) getPlan();

    if (plan.getSources().isEmpty()) {
      return;
    }

    if (plan.getSources().size() > 1) {
      throw new FrontendException(
          "Found LogicalExpressionPlan with more than one root.  Unexpected.", 2224);
    }

    postOrderWalk(plan.getSources().get(0), pigRelVistor);
  }

  /**
   * Does post-order walk on the Pig expression plan from source to sinks.
   *
   * @param root The root expression operator
   * @param visitor The visitor of each Pig expression node.
   * @throws FrontendException Exception during processing Pig operator
   */
  private void postOrderWalk(Operator root, PlanVisitor visitor) throws FrontendException {
    final Collection<Operator> nexts =
        Utils.mergeCollection(plan.getSuccessors(root), plan.getSuccessors(root));
    if (nexts != null) {
      for (Operator op : nexts) {
        postOrderWalk(op, visitor);
      }
    }
    root.accept(visitor);
  }

  @Override public PlanWalker spawnChildWalker(OperatorPlan operatorPlan)  {
    return new PigRelExWalker(operatorPlan);
  }
}

// End PigRelExWalker.java
