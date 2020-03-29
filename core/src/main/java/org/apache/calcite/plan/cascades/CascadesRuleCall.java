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
package org.apache.calcite.plan.cascades;

import org.apache.calcite.plan.RelHintsPropagator;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.calcite.plan.cascades.CascadesUtils.isLogical;

/**
 * Rule match for {@link CascadesPlanner}.
 */
public class CascadesRuleCall extends RelOptRuleCall {
  private List<RelNode> convertedRels = new ArrayList<>();
  private RelTraitSet requestedTraits;

  protected CascadesRuleCall(RelOptPlanner planner,
      RelOptRuleOperand operand, RelNode[] rels) {
    super(planner, operand, rels, Collections.emptyMap());
  }

  @Override public void transformTo(RelNode rel,
      Map<RelNode, RelNode> equiv,
      RelHintsPropagator handler) {
    assert equiv.isEmpty() : equiv; // TODO support equiv map.
    //assert resultRel == null;
   // rel = handler.propagate(rels[0], rel); // TODO support hints
    assert equiv == null || equiv.isEmpty();

    // Physical rels will be registered after inputs optimization
    if (isLogical(rel)) {
      rel = rel.onRegister(getPlanner());
      getPlanner().ensureRegistered(rel, rels[0]);
    }
    rels[0].getCluster().invalidateMetadataQuery();
    convertedRels.add(rel);
  }

  public void match() {
    RelOptRule rule = getRule();
    if (rule.matches(this)) {
      rule.onMatch(this);
    }
  }

  public List<RelNode> getConvertedRels() {
    return convertedRels;
  }

  public RelTraitSet requestedTraits() {
    return requestedTraits;
  }

  public void setRequestedTraits(RelTraitSet requestedTraits) {
    this.requestedTraits = requestedTraits;
  }

  @Override public String toString() {
    return "CascadesRuleCall{"
        + "rule=" + rule
        + ", rels=" + Arrays.toString(rels)
        + '}';
  }
}
