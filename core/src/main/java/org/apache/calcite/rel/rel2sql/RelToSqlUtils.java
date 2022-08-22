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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for rel2sql package.
 */
public class RelToSqlUtils {

  /** Returns list of all RexInputRef objects from the given condition. */
  private List<RexNode> getRexInputRefListFromCondition(RexNode condition,
      List<RexNode> inputRefRexList) {
    if (condition instanceof RexInputRef) {
      inputRefRexList.add(condition);
    } else if (condition instanceof RexCall) {
      for (RexNode operand : ((RexCall) condition).getOperands()) {
        if (operand instanceof RexLiteral) {
          continue;
        } else {
          getRexInputRefListFromCondition(operand, inputRefRexList);
        }
      }
    }
    return inputRefRexList;
  }

  /** Returns whether an operand is Analytical Function by traversing till next project rel. */
  private boolean isOperandAnalyticalInFollowingProject(RelNode rel, Integer rexOperandIndex) {
    if (rel instanceof Project) {
      return (((Project) rel).getChildExps().size() - 1) >= rexOperandIndex
          && ((Project) rel).getChildExps().get(rexOperandIndex) instanceof RexOver;
    } else {
      if (rel.getInputs().size() > 0) {
        return isOperandAnalyticalInFollowingProject(rel.getInput(0), rexOperandIndex);
      }
    }
    return false;
  }

  /** Returns whether an Analytical Function is present in filter condition. */
  protected boolean hasAnalyticalFunctionInFilter(Filter rel) {
    Integer rexOperandIndex = null;
    RexNode filterCondition = rel.getCondition();
    if (filterCondition instanceof RexCall) {
      for (RexNode conditionRex : ((RexCall) filterCondition).getOperands()) {
        if (conditionRex instanceof  RexLiteral) {
          continue;
        }

        List<RexNode> inputRefRexList = new ArrayList<>();
        List<RexNode> rexOperandList =
            getRexInputRefListFromCondition(conditionRex, inputRefRexList);

        for (RexNode rexOperand : rexOperandList) {
          if (rexOperand instanceof RexInputRef) {
            rexOperandIndex = ((RexInputRef) rexOperand).getIndex();
            if (isOperandAnalyticalInFollowingProject(rel, rexOperandIndex)) {
              return true;
            }
          }
        }
      }
    }
    return false;
  }

  /** Returns whether any Analytical Function (RexOver) is present in projection. */
  protected boolean isAnalyticalFunctionPresentInProjection(Project projectRel) {
    for (RexNode currentRex : projectRel.getChildExps()) {
      if (isAnalyticalRex(currentRex)) {
        return true;
      }
    }
    return false;
  }

  protected boolean isAnalyticalRex(RexNode rexNode) {
    if (rexNode instanceof RexOver) {
      return true;
    } else if (rexNode instanceof RexCall) {
      for (RexNode operand : ((RexCall) rexNode).getOperands()) {
        if (isAnalyticalRex(operand)) {
          return true;
        }
      }
    }
    return false;
  }
}
