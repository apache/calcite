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
import org.apache.calcite.rel.core.BestMatch;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdDistribution;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import com.google.common.collect.ImmutableSet;

import java.util.Objects;

/**
 * Sub-class of {@link org.apache.calcite.rel.core.BestMatch}
 * not targeted at any particular engine or calling convention.
 */
public class LogicalBestMatch extends BestMatch {
  private final ImmutableSet<CorrelationId> variablesSet;

  //~ Constructors -----------------------------------------------------------

  protected LogicalBestMatch(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input,
      ImmutableSet<CorrelationId> variablesSet) {
    super(cluster, traits, input);
    this.variablesSet = Objects.requireNonNull(variablesSet);
  }

  /** Creates a LogicalBestMatch. */
  public static LogicalBestMatch create(final RelNode input,
      ImmutableSet<CorrelationId> variablesSet) {
    final RelOptCluster cluster = input.getCluster();
    final RelMetadataQuery mq = cluster.getMetadataQuery();
    final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE)
        .replaceIfs(RelCollationTraitDef.INSTANCE,
            () -> RelMdCollation.filter(mq, input))
        .replaceIf(RelDistributionTraitDef.INSTANCE,
            () -> RelMdDistribution.filter(mq, input));
    return new LogicalBestMatch(cluster, traitSet, input, variablesSet);
  }

  @Override public BestMatch copy(final RelTraitSet traitSet, final RelNode input) {
    return new LogicalBestMatch(getCluster(), traitSet, input, variablesSet);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public RelNode accept(RelShuttle shuttle) {
    return shuttle.visit(this);
  }
}

// End LogicalBestMatch.java
