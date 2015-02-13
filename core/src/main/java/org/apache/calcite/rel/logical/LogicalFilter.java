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
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdDistribution;
import org.apache.calcite.rex.RexNode;

import com.google.common.base.Supplier;

import java.util.List;

/**
 * Sub-class of {@link org.apache.calcite.rel.core.Filter}
 * not targeted at any particular engine or calling convention.
 */
public final class LogicalFilter extends Filter {
  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a LogicalFilter.
   *
   * <p>Use {@link #create} unless you know what you're doing.
   *
   * @param cluster   Cluster that this relational expression belongs to
   * @param child     Input relational expression
   * @param condition Boolean expression which determines whether a row is
   *                  allowed to pass
   */
  public LogicalFilter(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode child,
      RexNode condition) {
    super(cluster, traitSet, child, condition);
  }

  @Deprecated // to be removed before 2.0
  public LogicalFilter(
      RelOptCluster cluster,
      RelNode child,
      RexNode condition) {
    this(cluster, cluster.traitSetOf(Convention.NONE), child, condition);
  }

  /**
   * Creates a LogicalFilter by parsing serialized output.
   */
  public LogicalFilter(RelInput input) {
    super(input);
  }

  /** Creates a LogicalFilter. */
  public static LogicalFilter create(final RelNode input, RexNode condition) {
    final RelOptCluster cluster = input.getCluster();
    final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE)
        .replaceIfs(RelCollationTraitDef.INSTANCE,
            new Supplier<List<RelCollation>>() {
              public List<RelCollation> get() {
                return RelMdCollation.filter(input);
              }
            })
        .replaceIf(RelDistributionTraitDef.INSTANCE,
            new Supplier<RelDistribution>() {
              public RelDistribution get() {
                return RelMdDistribution.filter(input);
              }
            });
    return new LogicalFilter(cluster, traitSet, input, condition);
  }

  //~ Methods ----------------------------------------------------------------

  public LogicalFilter copy(RelTraitSet traitSet, RelNode input,
      RexNode condition) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    return new LogicalFilter(getCluster(), traitSet, input, condition);
  }

  @Override public RelNode accept(RelShuttle shuttle) {
    return shuttle.visit(this);
  }
}

// End LogicalFilter.java
