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
package org.apache.calcite.rel.metadata;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.plan.volcano.CascadeRelSet;
import org.apache.calcite.plan.volcano.CascadeRelSubset;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.BuiltInMetadata.LowerBoundCost;
import org.apache.calcite.util.BuiltInMethod;

/**
 * Default implementations of the
 * {@link BuiltInMetadata.LowerBoundCost}
 * metadata provider for the standard algebra.
 */
public class RelMdLowerBoundCost implements MetadataHandler<LowerBoundCost> {

  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          new RelMdLowerBoundCost(), BuiltInMethod.LOWER_BOUND_COST.method);

  //~ Constructors -----------------------------------------------------------

  protected RelMdLowerBoundCost() {}

  //~ Methods ----------------------------------------------------------------

  public MetadataDef<LowerBoundCost> getDef() {
    return BuiltInMetadata.LowerBoundCost.DEF;
  }

  private boolean isLogical(RelNode relNode) {
    return relNode.getTraitSet().getTrait(ConventionTraitDef.INSTANCE)
        == Convention.NONE;
  }

  public RelOptCost getLowerBoundCost(CascadeRelSubset subset,
      RelMetadataQuery mq, RelOptPlanner planner) {

    if (isLogical(subset)) {
      // currently only support physical, will improve in the future
      return null;
    }

    RelOptCost winner = subset.getWinnerCost();
    if (winner != null) {
      // when this subset is fully optimized, just return the winner
      return winner;
    }

    // if group is not fully explored. Its properties like cardinality
    // would get changed after exploration. So it cannot return a valid LB
    if (subset.getSet().getState() != CascadeRelSet.ExploreState.EXPLORED) {
      return null;
    }

    RelOptCost lowerBound = null;
    for (RelNode relNode : subset.getRels()) {
      try {
        RelOptCost lb = mq.getLowerBoundCost(relNode, planner);
        if (lb == null) {
          return null;
        }
        if (lowerBound == null || lb.isLt(lowerBound)) {
          lowerBound = lb;
        }
      } catch (CyclicMetadataException e) {
        if (lowerBound == null) {
          // a cyclic metadata query means this node has an INF LB
          lowerBound = planner.getCostFactory().makeInfiniteCost();
        }
      }
    }
    return lowerBound;
  }

  public RelOptCost getLowerBoundCost(RelSubset subset,
      RelMetadataQuery mq, RelOptPlanner planner) {
    // cannot achieve the exploring state
    return null;
  }

  public RelOptCost getLowerBoundCost(RelNode node,
      RelMetadataQuery mq, RelOptPlanner planner) {
    if (isLogical(node)) {
      // currently only support physical, will improve in the future
      return null;
    }

    RelOptCost selfCost = node.computeSelfCost(planner, mq);
    if (selfCost.isInfinite()) {
      selfCost = null;
    }
    for (RelNode input : node.getInputs()) {
      RelOptCost lb = mq.getLowerBoundCost(input, planner);
      if (lb != null) {
        selfCost = selfCost == null ? lb : selfCost.plus(lb);
      }
    }
    return selfCost;
  }

  public RelOptCost getLowerBoundCost(AbstractConverter ac,
      RelMetadataQuery mq, RelOptPlanner planner) {
    return mq.getLowerBoundCost(ac.getInput(), planner);
  }
}
