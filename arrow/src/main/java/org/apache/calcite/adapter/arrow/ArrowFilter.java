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

package org.apache.calcite.adapter.arrow;

import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Util;

import java.util.*;

public class ArrowFilter extends Filter implements ArrowRel {

  private String match;

  public ArrowFilter(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode child,
      RexNode condition) {
    super(cluster, traitSet, child, condition);

    Translator translator = new Translator(getRowType());
//    this.match = translator.translateMatch(condition);
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(0.1);
  }

  public ArrowFilter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
    return new ArrowFilter(getCluster(), traitSet, input, condition);
  }

  public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());
    implementor.add(null, Collections.singletonList(match));
  }

  static class Translator {
    private final RelDataType rowType;
    private final List<String> fieldNames;

    Translator(RelDataType rowType) {
      this.rowType = rowType;
      this.fieldNames = ArrowRules.arrowFieldNames(rowType);
    }

//    private String translateMatch(RexNode condition) {
//      // CQL does not support disjunctions
//      List<RexNode> disjunctions = RelOptUtil.disjunctions(condition);
//      if (disjunctions.size() == 1) {
//        return translateAnd(disjunctions.get(0));
//      } else {
//        throw new AssertionError("cannot translate " + condition);
//      }
//    }

//    private String translateAnd(RexNode condition) {
//      List<String> predicates = new ArrayList<>();
//      for (RexNode node : RelOptUtil.conjunctions(condition)) {
////        predicates.add(translateMatch2(node));
//      }
//      return Util.toString(predicates, "", " AND ", "");
//    }

  }
}
