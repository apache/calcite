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
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalProject;

/**
 * Rule to convert a {@link LogicalProject} to an {@link EnumerableProject}.
 * You may provide a custom config to convert other nodes that extend {@link Project}.
 *
 * @see EnumerableRules#ENUMERABLE_PROJECT_RULE
 */
class EnumerableProjectRule extends ConverterRule {
  /** Default configuration. */
  static final Config DEFAULT_CONFIG = Config.INSTANCE
      .as(Config.class)
      .withConversion(LogicalProject.class, p -> !p.containsOver(),
          Convention.NONE, EnumerableConvention.INSTANCE,
          "EnumerableProjectRule")
      .withRuleFactory(EnumerableProjectRule::new);

  /** Creates an EnumerableProjectRule. */
  protected EnumerableProjectRule(Config config) {
    super(config);
  }

  @Override public boolean matches(RelOptRuleCall call) {
    Project project = call.rel(0);
    return project.getVariablesSet().isEmpty();
  }

  @Override public RelNode convert(RelNode rel) {
    final Project project = (Project) rel;
    return EnumerableProject.create(
        convert(project.getInput(),
            project.getInput().getTraitSet()
                .replace(EnumerableConvention.INSTANCE)),
        project.getProjects(),
        project.getRowType());
  }
}
