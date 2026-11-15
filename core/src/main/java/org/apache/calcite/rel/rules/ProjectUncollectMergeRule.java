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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;

import org.immutables.value.Value;

/**
 * Rule that narrows an {@link Uncollect} to the columns actually used by the
 * {@link Project} above it.
 *
 * <p>Passthrough fields unused by the project are removed from the {@code Uncollect};
 * an unreferenced {@code ORDINALITY} column is dropped.
 *
 * <p>Collection fields can never be removed: with multiple collections,
 * {@code UNNEST(a, b)} emits {@code max(|a|, |b|)} rows (zip semantics), so
 * dropping a collection could change the row count. Even when all of a
 * collection's element columns are unreferenced, its cardinality still
 * produces one empty row per element.
 */
@Value.Enclosing
public class ProjectUncollectMergeRule
    extends RelRule<ProjectUncollectMergeRule.Config>
    implements TransformationRule {

  protected ProjectUncollectMergeRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Project project = call.rel(0);
    final Uncollect uncollect = call.rel(1);

    final ImmutableBitSet passthrough = uncollect.getPassthroughFieldIndices();
    final int passthroughCount = passthrough.cardinality();
    final int outputCount = uncollect.getRowType().getFieldCount();

    final ImmutableBitSet refs =
        RelOptUtil.InputFinder.bits(project.getProjects(), null);

    // Keep the passthrough fields referenced by the project.
    // Passthrough fields occupy output positions 0 to passthroughCount-1, in
    // ascending input-index order.
    final ImmutableBitSet.Builder keptBuilder = ImmutableBitSet.builder();
    int outPos = 0;
    for (int field : passthrough) {
      if (refs.get(outPos)) {
        keptBuilder.set(field);
      }
      outPos++;
    }
    final ImmutableBitSet kept = keptBuilder.build();
    final boolean keepOrdinality =
        uncollect.withOrdinality && refs.get(outputCount - 1);

    if (kept.cardinality() == passthroughCount
        && keepOrdinality == uncollect.withOrdinality) {
      // Nothing to prune.
      return;
    }

    final Uncollect newUncollect =
        new Uncollect(uncollect.getCluster(), uncollect.getTraitSet(),
            uncollect.getInput(), keepOrdinality, uncollect.getItemAliases(),
            kept, uncollect.getCollectionFieldIndices(),
            uncollect.expandStructFields, uncollect.isOuter);

    // Map old output indices to new ones; dropped columns stay unmapped and
    // are never referenced.
    final int keptCount = kept.cardinality();
    final int newOutputCount = newUncollect.getRowType().getFieldCount();
    final Mappings.TargetMapping mapping =
        Mappings.create(MappingType.PARTIAL_FUNCTION, outputCount, newOutputCount);
    int oldPos = 0;
    int newPos = 0;
    for (int field : passthrough) {
      if (kept.get(field)) {
        mapping.set(oldPos, newPos++);
      }
      oldPos++;
    }
    // Element columns (and the ordinality column, if kept) shift down uniformly.
    final int elementEnd = keepOrdinality == uncollect.withOrdinality
        ? outputCount : outputCount - 1;
    for (int i = passthroughCount; i < elementEnd; i++) {
      mapping.set(i, keptCount + i - passthroughCount);
    }

    final RexShuttle remapper =
        new RexPermuteInputsShuttle(mapping, true, newUncollect);
    final RelBuilder builder = call.builder();
    // Preserve the field names.
    builder.push(newUncollect)
        .project(remapper.apply(project.getProjects()),
            project.getRowType().getFieldNames(), true);
    call.transformTo(builder.build());
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableProjectUncollectMergeRule.Config.of()
        .withOperandSupplier(b0 -> b0.operand(Project.class)
            .oneInput(b1 -> b1.operand(Uncollect.class).anyInputs()))
        .as(Config.class);

    @Override default ProjectUncollectMergeRule toRule() {
      return new ProjectUncollectMergeRule(this);
    }
  }
}
