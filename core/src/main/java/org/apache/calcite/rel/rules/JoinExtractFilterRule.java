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
package org.apache.calcite.rel.rules;

import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.tools.RelBuilderFactory;

import org.immutables.value.Value;

/**
 * Rule to convert an
 * {@link org.apache.calcite.rel.logical.LogicalJoin inner join} to a
 * {@link org.apache.calcite.rel.logical.LogicalFilter filter} on top of a
 * {@link org.apache.calcite.rel.logical.LogicalJoin cartesian inner join}.
 *
 * <p>One benefit of this transformation is that after it, the join condition
 * can be combined with conditions and expressions above the join.
 *
 * <p>Can be configured to match any sub-class of
 * {@link org.apache.calcite.rel.core.Join}, not just
 * {@link org.apache.calcite.rel.logical.LogicalJoin}.
 *
 * @see CoreRules#JOIN_EXTRACT_FILTER
 */
@Value.Enclosing
public final class JoinExtractFilterRule extends AbstractJoinExtractFilterRule {

  /** Creates a JoinExtractFilterRule. */
  JoinExtractFilterRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public JoinExtractFilterRule(Class<? extends Join> clazz,
      RelBuilderFactory relBuilderFactory) {
    this(Config.DEFAULT
        .withRelBuilderFactory(relBuilderFactory)
        .withOperandSupplier(b ->
            b.operand(clazz).anyInputs())
        .as(Config.class));
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends AbstractJoinExtractFilterRule.Config {
    Config DEFAULT = ImmutableJoinExtractFilterRule.Config.of()
        .withOperandSupplier(b -> b.operand(LogicalJoin.class).anyInputs());

    @Override default JoinExtractFilterRule toRule() {
      return new JoinExtractFilterRule(this);
    }
  }
}
