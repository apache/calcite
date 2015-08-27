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
import org.apache.calcite.rel.core.Uncollect;

/**
 * Rule to convert an {@link org.apache.calcite.rel.core.Uncollect} to an
 * {@link EnumerableUncollect}.
 */
class EnumerableUncollectRule extends ConverterRule {
  EnumerableUncollectRule() {
    super(Uncollect.class, Convention.NONE, EnumerableConvention.INSTANCE,
        "EnumerableUncollectRule");
  }

  public RelNode convert(RelNode rel) {
    final Uncollect uncollect = (Uncollect) rel;
    final RelTraitSet traitSet =
        uncollect.getTraitSet().replace(EnumerableConvention.INSTANCE);
    final RelNode input = uncollect.getInput();
    final RelNode newInput = convert(input,
        input.getTraitSet().replace(EnumerableConvention.INSTANCE));
    return EnumerableUncollect.create(traitSet, newInput,
        uncollect.withOrdinality);
  }
}

// End EnumerableUncollectRule.java
