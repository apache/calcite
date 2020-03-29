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

import org.apache.calcite.plan.cascades.Enforcer;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;

/**
 *
 */
public class CascadesTestDistributionEnforcer extends Enforcer<RelDistribution> {
  public static final CascadesTestDistributionEnforcer INSTANCE =
      new CascadesTestDistributionEnforcer();

  public CascadesTestDistributionEnforcer() {
    super(RelDistributionTraitDef.INSTANCE);
  }

  @Override public RelNode enforce(RelNode rel, RelDistribution to) {
    Exchange exchange = (Exchange) traitDef.convert(null, rel, to, false);

    if (exchange == null) {
      return null;
    }

    return CascadesTestExchange.create(rel, exchange.getDistribution());
  }
}
