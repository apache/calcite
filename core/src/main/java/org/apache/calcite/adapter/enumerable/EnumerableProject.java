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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/** Implementation of {@link org.apache.calcite.rel.core.Project} in
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}. */
public class EnumerableProject extends Project implements EnumerableRel {
  public EnumerableProject(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode child,
      List<? extends RexNode> exps,
      RelDataType rowType) {
    super(cluster, traitSet, child, exps, rowType);
    assert getConvention() instanceof EnumerableConvention;
  }

  public EnumerableProject copy(RelTraitSet traitSet, RelNode input,
      List<RexNode> exps, RelDataType rowType) {
    return new EnumerableProject(getCluster(), traitSet, input,
        exps, rowType);
  }

  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    // EnumerableCalcRel is always better
    throw new UnsupportedOperationException();
  }
}

// End EnumerableProject.java
