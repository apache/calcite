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
package org.apache.calcite.plan.volcano;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.PlannerFactory;
import org.apache.calcite.plan.RelOptCostFactory;

/**
 * create a volcano planner for with specific configurations
 */
public abstract class VolcanoPlannerFactory implements PlannerFactory {

  // A factory that create VolcanoPlanner
  public static final VolcanoPlannerFactory VOLCANO = new VolcanoPlannerFactory() {
    /**
     * creating planners according to system properties
     */
    public VolcanoPlanner create(
        RelOptCostFactory costFactory, Context externalContext) {
      return new VolcanoPlanner(costFactory, externalContext);
    }
  };

  // A factory that create CascadePlanner
  public static final VolcanoPlannerFactory CASCADE = new VolcanoPlannerFactory() {
    /**
     * creating planners according to system properties
     */
    public VolcanoPlanner create(
        RelOptCostFactory costFactory, Context externalContext) {
      return new CascadePlanner(costFactory, externalContext);
    }
  };

  public static final String CASCADE_PLANNER =
      VolcanoPlannerFactory.class.getName() + "#CASCADE";

  protected VolcanoPlannerFactory() {}

  public abstract VolcanoPlanner create(RelOptCostFactory costFactory, Context externalContext);

  /**
   * creating planners according to system properties
   */
  public static VolcanoPlanner getPlanner() {
    return getPlanner(null, null);
  }

  public static VolcanoPlanner getPlanner(
      RelOptCostFactory costFactory, Context externalContext) {
    VolcanoPlannerFactory factory;
    if ("true".equalsIgnoreCase(System.getProperty("calcite.cascade", "false"))) {
      factory = CASCADE;
    } else {
      factory = VOLCANO;
    }
    CalciteConnectionConfig config = externalContext == null
        ? null : externalContext.unwrap(CalciteConnectionConfigImpl.class);
    if (config != null) {
      factory = config.plannerFactory(VolcanoPlannerFactory.class, factory);
    }
    return factory.create(costFactory, externalContext);
  }
}
