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
package org.apache.calcite.plan.cascades.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.cascades.CascadesTestUtils;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 *
 */
public class CascadesTestFilter extends Filter implements PhysicalNode {
  public CascadesTestFilter(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode child,
      RexNode condition) {
    super(cluster, traitSet, child, condition);
    assert getConvention() == CascadesTestUtils.CASCADES_TEST_CONVENTION;
  }

  public CascadesTestFilter copy(RelTraitSet traitSet, RelNode input,
      RexNode condition) {
    return new CascadesTestFilter(getCluster(), traitSet, input, condition);
  }

  @Override public PhysicalNode withNewInputs(List<RelNode> newInputs) {
    assert newInputs.size() == 1;
    RelNode input = newInputs.get(0);
    // Filter preserves children's convention.
    return copy(input.getTraitSet(), input, condition);
  }
}
