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
import org.apache.calcite.rel.core.Collect;

/**
 * Rule to convert an {@link org.apache.calcite.rel.core.Collect} to an
 * {@link EnumerableCollect}.
 *
 * @see EnumerableRules#ENUMERABLE_COLLECT_RULE
 */
class EnumerableCollectRule extends ConverterRule {
  /** Default configuration. */
  public static final Config DEFAULT_CONFIG = Config.INSTANCE
      .withConversion(Collect.class, Convention.NONE,
          EnumerableConvention.INSTANCE, "EnumerableCollectRule")
      .withRuleFactory(EnumerableCollectRule::new);

  /** Called from the Config. */
  protected EnumerableCollectRule(Config config) {
    super(config);
  }

  @Override public RelNode convert(RelNode rel) {
    final Collect collect = (Collect) rel;
    final RelTraitSet traitSet =
        collect.getTraitSet().replace(EnumerableConvention.INSTANCE);
    final RelNode input = collect.getInput();
    return new EnumerableCollect(
        rel.getCluster(),
        traitSet,
        convert(input,
            input.getTraitSet().replace(EnumerableConvention.INSTANCE)),
        collect.getFieldName());
  }
}
