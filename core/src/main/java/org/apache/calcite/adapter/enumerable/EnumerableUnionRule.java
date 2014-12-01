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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalUnion;

/**
 * Rule to convert an {@link org.apache.calcite.rel.logical.LogicalUnion} to an
 * {@link EnumerableUnion}.
 */
class EnumerableUnionRule extends ConverterRule {
  EnumerableUnionRule() {
    super(LogicalUnion.class, Convention.NONE, EnumerableConvention.INSTANCE,
        "EnumerableUnionRule");
  }

  public RelNode convert(RelNode rel) {
    final LogicalUnion union = (LogicalUnion) rel;
    final EnumerableConvention out = EnumerableConvention.INSTANCE;
    final RelTraitSet traitSet = union.getTraitSet().replace(out);
    return new EnumerableUnion(rel.getCluster(), traitSet,
        convertList(union.getInputs(), out), union.all);
  }
}

// End EnumerableUnionRule.java
