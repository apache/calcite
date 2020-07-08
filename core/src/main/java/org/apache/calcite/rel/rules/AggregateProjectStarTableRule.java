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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.schema.impl.StarTable;

/** Variant of {@link AggregateStarTableRule} that accepts a {@link Project}
 * between the {@link Aggregate} and its {@link StarTable.StarTableScan}
 * input. */
public class AggregateProjectStarTableRule extends AggregateStarTableRule {
  /** Creates an AggregateProjectStarTableRule. */
  protected AggregateProjectStarTableRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final Project project = call.rel(1);
    final StarTable.StarTableScan scan = call.rel(2);
    final RelNode rel =
        AggregateProjectMergeRule.apply(call, aggregate, project);
    final Aggregate aggregate2;
    final Project project2;
    if (rel instanceof Aggregate) {
      project2 = null;
      aggregate2 = (Aggregate) rel;
    } else if (rel instanceof Project) {
      project2 = (Project) rel;
      aggregate2 = (Aggregate) project2.getInput();
    } else {
      return;
    }
    apply(call, project2, aggregate2, scan);
  }

  /** Rule configuration. */
  public interface Config extends AggregateStarTableRule.Config {
    Config DEFAULT = EMPTY.as(Config.class)
        .withOperandFor(Aggregate.class, Project.class,
            StarTable.StarTableScan.class);

    @Override default AggregateProjectStarTableRule toRule() {
      return new AggregateProjectStarTableRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Aggregate> aggregateClass,
        Class<? extends Project> projectClass,
        Class<StarTable.StarTableScan> scanClass) {
      return withOperandSupplier(b0 ->
          b0.operand(aggregateClass)
              .predicate(Aggregate::isSimple)
              .oneInput(b1 ->
                  b1.operand(projectClass)
                      .oneInput(b2 ->
                          b2.operand(scanClass).noInputs())))
          .as(Config.class);
    }
  }
}
