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
package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.tools.RelBuilder;

import org.immutables.value.Value;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Planner rule that reorders inputs of an {@link Intersect} to put smaller inputs first.
 * This helps reduce the size of intermediate results.
 *
 * <p>Intersect(A, B, ...) where B is smallest will reorder to Intersect(B, A, ...)
 */
@Value.Enclosing
public class IntersectReorderRule extends RelRule<IntersectReorderRule.Config>
      implements SubstitutionRule {
  /** Creates an IntersectReorderRule. */
  protected IntersectReorderRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Intersect intersect = call.rel(0);
    final RelMetadataQuery mq = call.getMetadataQuery();
    final List<RelNode> inputs = intersect.getInputs();

    List<RelNode> sortedInputs = inputs.stream()
        .sorted(Comparator.comparingDouble(mq::getRowCount))
        .collect(Collectors.toList());

    if (inputs.equals(sortedInputs)) {
      return;
    }

    final RelBuilder relBuilder = call.builder();
    relBuilder.pushAll(sortedInputs);
    relBuilder.intersect(intersect.all, sortedInputs.size());

    call.transformTo(relBuilder.build());
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableIntersectReorderRule.Config.of()
        .withOperandSupplier(b0 ->
            b0.operand(LogicalIntersect.class)
                .predicate(intersect -> intersect.getInputs().size() > 1)
                .anyInputs())
        .withDescription("IntersectReorderRule");

    @Override default IntersectReorderRule toRule() {
      return new IntersectReorderRule(this);
    }
  }
}
