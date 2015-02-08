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
import org.apache.calcite.rel.RelCollationImpl;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

import java.util.List;

/**
 * Sub-class of {@link org.apache.calcite.rel.core.Project} not
 * targeted at any particular engine or calling convention.
 */
public final class LogicalProject extends Project {
  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a LogicalProject with no sort keys.
   *
   * @param cluster    Cluster this relational expression belongs to
   * @param child      input relational expression
   * @param exps       set of expressions for the input columns
   * @param fieldNames aliases of the expressions
   */
  public LogicalProject(
      RelOptCluster cluster,
      RelNode child,
      List<RexNode> exps,
      List<String> fieldNames) {
    this(
        cluster,
        cluster.traitSetOf(RelCollationImpl.EMPTY),
        child,
        exps,
        RexUtil.createStructType(
            cluster.getTypeFactory(),
            exps,
            fieldNames));
  }

  /**
   * Creates a LogicalProject.
   *
   * @param cluster  Cluster this relational expression belongs to
   * @param traitSet traits of this rel
   * @param child    input relational expression
   * @param exps     List of expressions for the input columns
   * @param rowType  output row type
   */
  public LogicalProject(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode child,
      List<? extends RexNode> exps,
      RelDataType rowType) {
    super(cluster, traitSet, child, exps, rowType);
    assert traitSet.containsIfApplicable(Convention.NONE);
  }

  /**
   * Creates a LogicalProject by parsing serialized output.
   */
  public LogicalProject(RelInput input) {
    super(input);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public LogicalProject copy(RelTraitSet traitSet, RelNode input,
      List<RexNode> exps, RelDataType rowType) {
    return new LogicalProject(getCluster(), traitSet, input, exps, rowType);
  }

  @Override public RelNode accept(RelShuttle shuttle) {
    return shuttle.visit(this);
  }
}

// End LogicalProject.java
