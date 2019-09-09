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
package org.apache.calcite.rel.rules.materialize;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilderFactory;

/** Rule that matches Project on Filter. */
public class MaterializedViewProjectFilterRule extends MaterializedViewJoinRule {

  public static final MaterializedViewProjectFilterRule INSTANCE =
      new MaterializedViewProjectFilterRule(RelFactories.LOGICAL_BUILDER,
          true, null, true);

  public MaterializedViewProjectFilterRule(RelBuilderFactory relBuilderFactory,
      boolean generateUnionRewriting, HepProgram unionRewritingPullProgram,
      boolean fastBailOut) {
    super(
        operand(Project.class,
            operand(Filter.class, any())),
        relBuilderFactory,
        "MaterializedViewJoinRule(Project-Filter)",
        generateUnionRewriting, unionRewritingPullProgram, fastBailOut);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Project project = call.rel(0);
    final Filter filter = call.rel(1);
    perform(call, project, filter);
  }
}
