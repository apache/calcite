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

import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

/**
 * Planner rule that converts {@link org.apache.calcite.interpreter.BindableRel}
 * to {@link org.apache.calcite.adapter.enumerable.EnumerableRel} by creating
 * an {@link org.apache.calcite.adapter.enumerable.EnumerableInterpreter}.
 *
 * @see EnumerableRules#TO_INTERPRETER
 */
public class EnumerableInterpreterRule extends ConverterRule {
  /** Default configuration. */
  public static final Config DEFAULT_CONFIG = Config.INSTANCE
      .withConversion(RelNode.class, BindableConvention.INSTANCE,
          EnumerableConvention.INSTANCE, "EnumerableInterpreterRule")
      .withRuleFactory(EnumerableInterpreterRule::new);

  /** @deprecated Use {@link EnumerableRules#TO_INTERPRETER}. */
  @Deprecated // to be removed before 1.25
  public static final EnumerableInterpreterRule INSTANCE =
      DEFAULT_CONFIG.toRule(EnumerableInterpreterRule.class);

  protected EnumerableInterpreterRule(Config config) {
    super(config);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public RelNode convert(RelNode rel) {
    return EnumerableInterpreter.create(rel, 0.5d);
  }
}
