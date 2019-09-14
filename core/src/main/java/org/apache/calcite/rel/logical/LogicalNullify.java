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
package org.apache.calcite.rel.logical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Nullify;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdDistribution;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Objects;

/**
 * Sub-class of {@link org.apache.calcite.rel.core.Nullify}
 * not targeted at any particular engine or calling convention.
 */
public class LogicalNullify extends Nullify {
  private final ImmutableSet<CorrelationId> variablesSet;

  //~ Constructors -----------------------------------------------------------

  protected LogicalNullify(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode child,
      RexNode predicate,
      List<? extends RexNode> attributes,
      ImmutableSet<CorrelationId> variablesSet) {
    super(cluster, traits, child, predicate, attributes);
    this.variablesSet = Objects.requireNonNull(variablesSet);
  }

  /** Creates a LogicalNullify. */
  public static LogicalNullify create(final RelNode input, RexNode condition,
      List<? extends RexNode> attributes,
      ImmutableSet<CorrelationId> variablesSet) {
    final RelOptCluster cluster = input.getCluster();
    final RelMetadataQuery mq = cluster.getMetadataQuery();
    final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE)
        .replaceIfs(RelCollationTraitDef.INSTANCE,
            () -> RelMdCollation.filter(mq, input))
        .replaceIf(RelDistributionTraitDef.INSTANCE,
            () -> RelMdDistribution.filter(mq, input));
    return new LogicalNullify(cluster, traitSet, input, condition, attributes, variablesSet);
  }

  @Override public Nullify copy(RelTraitSet traitSet, RelNode input,
      RexNode predicate, List<RexNode> attributes) {
    return new LogicalNullify(getCluster(), traitSet, input, predicate, attributes, variablesSet);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public RelNode accept(RelShuttle shuttle) {
    return shuttle.visit(this);
  }

  @Override public RelWriter explainTerms(final RelWriter pw) {
    return super.explainTerms(pw)
        .itemIf("variablesSet", variablesSet, !variablesSet.isEmpty());
  }
}

// End LogicalNullify.java
