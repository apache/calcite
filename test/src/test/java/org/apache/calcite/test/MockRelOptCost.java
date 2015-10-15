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
package org.apache.calcite.test;

import org.apache.calcite.plan.RelOptCost;

/**
 * MockRelOptCost is a mock implementation of the {@link RelOptCost} interface.
 * TODO: constructors for various scenarios
 */
public class MockRelOptCost implements RelOptCost {
  //~ Methods ----------------------------------------------------------------

  @Override public boolean equals(Object obj) {
    return this == obj
        || obj instanceof MockRelOptCost
        && equals((MockRelOptCost) obj);
  }

  @Override public int hashCode() {
    return 1;
  }

  public double getCpu() {
    return 0;
  }

  public boolean isInfinite() {
    return false;
  }

  public double getIo() {
    return 0;
  }

  public boolean isLe(RelOptCost cost) {
    return true;
  }

  public boolean isLt(RelOptCost cost) {
    return false;
  }

  public double getRows() {
    return 0;
  }

  public boolean equals(RelOptCost cost) {
    return true;
  }

  public boolean isEqWithEpsilon(RelOptCost cost) {
    return true;
  }

  public RelOptCost minus(RelOptCost cost) {
    return this;
  }

  public RelOptCost multiplyBy(double factor) {
    return this;
  }

  public double divideBy(RelOptCost cost) {
    return 1;
  }

  public RelOptCost plus(RelOptCost cost) {
    return this;
  }

  public String toString() {
    return "MockRelOptCost(0)";
  }
}

// End MockRelOptCost.java
