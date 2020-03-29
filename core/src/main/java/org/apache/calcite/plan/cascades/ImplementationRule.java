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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * Rule that converts logical Rels to physical ones.
 * @param <R> RelNode.
 */
public abstract class ImplementationRule<R extends RelNode>  extends RelOptRule {

  public  ImplementationRule(Class<R> clazz,
      Predicate<? super R> predicate, RelTrait in, RelTrait out,
      RelBuilderFactory relBuilderFactory, String descriptionPrefix) {
    super(convertOperand(clazz, predicate, in),
        relBuilderFactory,
        createDescription(descriptionPrefix, in, out));

    // Source and target traits must have same type
    assert in.getTraitDef() == out.getTraitDef();
  }

  private static String createDescription(String descriptionPrefix,
      RelTrait in, RelTrait out) {
    return String.format(Locale.ROOT, "%s(in:%s,out:%s)",
        Objects.toString(descriptionPrefix, "ImplementationRule"), in, out);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    CascadesRuleCall cascadesCall = (CascadesRuleCall) call;
    R rel = call.rel(0);
    RelTraitSet requestedTraits = cascadesCall.requestedTraits();
    implement(rel, requestedTraits, cascadesCall);
  }

  /**
   * Carries out the physical optimization. All newly created {@link PhysicalNode}s will not be
   * registered in planner until their inputs are optimized for requested traits. When inputs
   * optimization done, method {@link PhysicalNode#withNewInputs(List)} will be called with
   * optimized inputs. Only the {@link PhysicalNode} returned from this method will be registered
   * in the planner.
   *
   * @param rel RelNode to be physically optimized.
   * @param requestedTraits Traits requested by parent node,
   * @param call Rule match.
   */
  public abstract void implement(R rel, RelTraitSet requestedTraits, CascadesRuleCall call);
}
