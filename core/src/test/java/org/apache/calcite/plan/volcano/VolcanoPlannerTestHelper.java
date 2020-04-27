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

import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCostFactory;

/**
 * containing some helper method for tet cases
 */
public class VolcanoPlannerTestHelper {

  private VolcanoPlannerTestHelper() {}

  /**
   * creating planners according to system properties
   */
  public static VolcanoPlanner getPlanner() {
    return getPlanner(null, null);
  }

  /**
   * creating planners according to system properties
   */
  public static VolcanoPlanner getPlanner(RelOptCostFactory costFactory,
      Context externalContext) {
    if ("true".equalsIgnoreCase(System.getProperty("cascade", "false"))) {
      return new CascadePlanner(costFactory, externalContext);
    } else {
      return new VolcanoPlanner(costFactory, externalContext);
    }
  }
}
