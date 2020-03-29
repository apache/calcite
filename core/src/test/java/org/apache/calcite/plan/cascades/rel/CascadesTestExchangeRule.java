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
package org.apache.calcite.plan.cascades.rel;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.cascades.CascadesRuleCall;
import org.apache.calcite.plan.cascades.CascadesTestUtils;
import org.apache.calcite.plan.cascades.ImplementationRule;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalExchange;

/**
 *
 */
public class CascadesTestExchangeRule extends ImplementationRule<LogicalExchange> {
  public static final CascadesTestExchangeRule CASCADES_EXCHANGE_RULE =
      new CascadesTestExchangeRule();
  public CascadesTestExchangeRule() {
    super(LogicalExchange.class,
        r -> true,
        Convention.NONE, CascadesTestUtils.CASCADES_TEST_CONVENTION, RelFactories.LOGICAL_BUILDER,
        "CascadesExchangeRule");
  }

  @Override public void implement(LogicalExchange exchange, RelTraitSet requestedTraits,
      CascadesRuleCall call) {
    // Exchange preserves any other trait except distribution. We may request RelDistribution.ANY.
    requestedTraits = requestedTraits.plus(RelDistributions.ANY);
    CascadesTestExchange newExchange =  CascadesTestExchange.create(
        convert(exchange.getInput(), requestedTraits),
        exchange.getDistribution()
    );
    call.transformTo(newExchange);
  }
}
