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
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

import com.google.common.base.Supplier;

import java.util.List;

/**
 * Sub-class of {@link org.apache.calcite.rel.core.Project} not
 * targeted at any particular engine or calling convention.
 */
public final class LogicalProject extends Project {
  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a LogicalProject.
   *
   * <p>Use {@link #create} unless you know what you're doing.
   *
   * @param cluster  Cluster this relational expression belongs to
   * @param traitSet traits of this rel
   * @param input    input relational expression
   * @param exps     List of expressions for the input columns
   * @param rowType  output row type
   */
  public LogicalProject(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      List<? extends RexNode> exps,
      RelDataType rowType) {
    super(cluster, traitSet, input, exps, rowType);
    assert traitSet.containsIfApplicable(Convention.NONE);
  }

  @Deprecated // to be removed before 2.0
  public LogicalProject(RelOptCluster cluster, RelNode child,
      List<RexNode> exps, List<String> fieldNames) {
    this(cluster, cluster.traitSetOf(RelCollations.EMPTY), child, exps,
        RexUtil.createStructType(cluster.getTypeFactory(), exps, fieldNames));
  }

  /**
   * Creates a LogicalProject by parsing serialized output.
   */
  public LogicalProject(RelInput input) {
    super(input);
  }

  //~ Methods ----------------------------------------------------------------

  /** Creates a LogicalProject. */
  public static LogicalProject create(final RelNode input,
      final List<? extends RexNode> projects, List<String> fieldNames) {
    final RelOptCluster cluster = input.getCluster();
    final RelDataType rowType =
        RexUtil.createStructType(cluster.getTypeFactory(), projects,
            fieldNames);
    return create(input, projects, rowType);
  }

  /** Creates a LogicalProject, specifying row type rather than field names. */
  public static LogicalProject create(final RelNode input,
      final List<? extends RexNode> projects, RelDataType rowType) {
    final RelOptCluster cluster = input.getCluster();
    final RelTraitSet traitSet =
        cluster.traitSet().replace(Convention.NONE)
            .replaceIf(RelCollationTraitDef.INSTANCE,
                new Supplier<List<RelCollation>>() {
                  public List<RelCollation> get() {
                    return RelMdCollation.project(input, projects);
                  }
                });
    return new LogicalProject(cluster, traitSet, input, projects, rowType);
  }

  @Override public LogicalProject copy(RelTraitSet traitSet, RelNode input,
      List<RexNode> exps, RelDataType rowType) {
    return new LogicalProject(getCluster(), traitSet, input, exps, rowType);
  }

  @Override public RelNode accept(RelShuttle shuttle) {
    return shuttle.visit(this);
  }
}

// End LogicalProject.java
