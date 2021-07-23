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
package org.apache.calcite.plan;

import org.apache.calcite.rel.RelNode;

/**
 * Customize the propagation of the {@link org.apache.calcite.rel.hint.RelHint}s
 * from the root relational expression of a rule call {@link RelOptRuleCall} to
 * the new equivalent expression.
 *
 * @see RelOptUtil#propagateRelHints(RelNode, RelNode)
 */
@FunctionalInterface
public interface RelHintsPropagator {

  /**
   * Propagates the hints from a rule call's root relational expression {@code oriNode}
   * to the new equivalent relational expression {@code equiv}.
   *
   * @param oriNode Root relational expression of a rule call
   * @param equiv   Equivalent expression
   * @return New relational expression that would register into the planner
   */
  RelNode propagate(RelNode oriNode, RelNode equiv);
}
