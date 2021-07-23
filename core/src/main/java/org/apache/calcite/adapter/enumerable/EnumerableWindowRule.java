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
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalWindow;

/**
 * Rule to convert a {@link LogicalWindow} to an {@link EnumerableWindow}.
 * You may provide a custom config to convert other nodes that extend {@link Window}.
 *
 * @see EnumerableRules#ENUMERABLE_WINDOW_RULE
 */
class EnumerableWindowRule extends ConverterRule {
  /** Default configuration. */
  static final Config DEFAULT_CONFIG = Config.INSTANCE
      .withConversion(LogicalWindow.class, Convention.NONE,
          EnumerableConvention.INSTANCE, "EnumerableWindowRule")
      .withRuleFactory(EnumerableWindowRule::new);

  /** Called from the Config. */
  protected EnumerableWindowRule(Config config) {
    super(config);
  }

  @Override public RelNode convert(RelNode rel) {
    final Window winAgg = (Window) rel;
    final RelTraitSet traitSet =
        rel.getCluster().traitSet().replace(EnumerableConvention.INSTANCE);
    final RelNode child = winAgg.getInput();
    final RelNode convertedChild = convert(child, traitSet);
    return new EnumerableWindow(rel.getCluster(), traitSet, convertedChild,
        winAgg.getConstants(), winAgg.getRowType(), winAgg.groups);
  }
}
