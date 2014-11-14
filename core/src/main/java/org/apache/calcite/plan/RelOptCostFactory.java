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

/**
 * Cost model for query planning.
 */
public interface RelOptCostFactory {
  /**
   * Creates a cost object.
   */
  RelOptCost makeCost(double rowCount, double cpu, double io);

  /**
   * Creates a cost object representing an enormous non-infinite cost.
   */
  RelOptCost makeHugeCost();

  /**
   * Creates a cost object representing infinite cost.
   */
  RelOptCost makeInfiniteCost();

  /**
   * Creates a cost object representing a small positive cost.
   */
  RelOptCost makeTinyCost();

  /**
   * Creates a cost object representing zero cost.
   */
  RelOptCost makeZeroCost();
}

// End RelOptCostFactory.java
