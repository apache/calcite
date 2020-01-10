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

class RuleStatistics implements Comparable<RuleStatistics> {
  private static final double ALPHA = 0.8;

  private int sampleCount;
  private double rels;
  private double sets;
  private double setChurn;
  private double registerChurn;

  public void add(int rels, int sets, int setChurn, int registerChurn) {
    this.sampleCount++;
    this.rels = decay(this.rels, rels);
    this.sets = decay(this.sets, sets);
    this.setChurn = decay(this.setChurn, setChurn);
    this.registerChurn = decay(this.registerChurn, registerChurn);
  }

  private double decay(double prev, double next) {
    return prev * ALPHA + next * (1 - ALPHA);
  }

  public boolean reduces() {
    return rels < 0.1 || sets < 0.1;
  }

  public boolean expands() {
    return rels > 0.1 || sets > 0.1;
  }

  @Override public int compareTo(RuleStatistics o) {
    if (this.sampleCount != 0 && o.sampleCount != 0) {
      if (rels != o.rels) {
        return Double.compare(rels, o.rels);
      }
      if (sets != o.sets) {
        return Double.compare(sets, o.sets);
      }
      if (setChurn != o.setChurn) {
        return Double.compare(setChurn, o.setChurn);
      }
      if (registerChurn != o.registerChurn) {
        return Double.compare(registerChurn, o.registerChurn);
      }
    }
    return 0;
  }

  @Override public String toString() {
    return sampleCount
        + ", rels " + rels
        + ", sets " + sets
        + ", setChurn " + setChurn
        + ", registers " + registerChurn;
  }
}
