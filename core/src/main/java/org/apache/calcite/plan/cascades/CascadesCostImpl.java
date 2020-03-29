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
package org.apache.calcite.plan.cascades;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptUtil;

/**
 * Default cascades cost implementation.
 */
public class CascadesCostImpl  implements CascadesCost {

  public static final RelOptCostFactory FACTORY = new Factory();

  static final CascadesCost INFINITY =
      new CascadesCostImpl(
          Double.POSITIVE_INFINITY,
          Double.POSITIVE_INFINITY,
          Double.POSITIVE_INFINITY) {
        public String toString() {
          return "{inf}";
        }
      };
  static final CascadesCost HUGE =
      new CascadesCostImpl(Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE) {
        public String toString() {
          return "{huge}";
        }
      };

  static final CascadesCost ZERO =
      new CascadesCostImpl(0.0, 0.0, 0.0) {
        public String toString() {
          return "{0}";
        }
      };

  static final CascadesCost TINY =
      new CascadesCostImpl(1.0, 1.0, 0.0) {
        public String toString() {
          return "{tiny}";
        }
      };

  private final double rowCount;

  private final double cpu;

  private final double io;


  CascadesCostImpl(double rowCount, double cpu, double io) {
    this.rowCount = rowCount;
    this.cpu = cpu;
    this.io = io;
  }

  @Override public double scalarCost() {
    return rowCount / 3 + cpu / 3 + io / 3;
  }

  @Override public double getRows() {
    return rowCount;
  }

  @Override public double getCpu() {
    return cpu;
  }

  @Override public double getIo() {
    return io;
  }

  @Override public boolean isInfinite() {
    return (this == INFINITY)
        || (this.rowCount == Double.POSITIVE_INFINITY)
        || (this.cpu == Double.POSITIVE_INFINITY)
        || (this.io == Double.POSITIVE_INFINITY);
  }

  @Override public boolean equals(RelOptCost cost) {
    return this == cost
        || cost instanceof CascadesCostImpl
        && (this.rowCount == ((CascadesCostImpl) cost).rowCount)
        && (this.cpu == ((CascadesCostImpl) cost).cpu)
        && (this.io == ((CascadesCostImpl) cost).io);
  }

  @Override public boolean isEqWithEpsilon(RelOptCost cost) {
    if (!(cost instanceof CascadesCostImpl)) {
      return false;
    }
    CascadesCostImpl that = (CascadesCostImpl) cost;
    return (this == that)
        || ((Math.abs(this.rowCount - that.rowCount) < RelOptUtil.EPSILON)
        && (Math.abs(this.cpu - that.cpu) < RelOptUtil.EPSILON)
        && (Math.abs(this.io - that.io) < RelOptUtil.EPSILON));
  }

  @Override public boolean isLe(RelOptCost cost) {
    CascadesCostImpl that = (CascadesCostImpl) cost;
    return scalarCost() <= that.scalarCost();
  }

  @Override public boolean isLt(RelOptCost cost) {
    CascadesCostImpl that = (CascadesCostImpl) cost;
    return scalarCost() < that.scalarCost();
  }

  @Override public RelOptCost plus(RelOptCost cost) {
    CascadesCostImpl that = (CascadesCostImpl) cost;
    if ((this == INFINITY) || (that == INFINITY)) {
      return INFINITY;
    }
    return new CascadesCostImpl(
        this.rowCount + that.rowCount,
        this.cpu + that.cpu,
        this.io + that.io);
  }

  @Override public RelOptCost minus(RelOptCost cost) {
    if (this == INFINITY) {
      return this;
    }
    CascadesCostImpl that = (CascadesCostImpl) cost;
    return new CascadesCostImpl(
        this.rowCount - that.rowCount,
        this.cpu - that.cpu,
        this.io - that.io);
  }

  @Override public RelOptCost multiplyBy(double factor) {
    if (this == INFINITY) {
      return this;
    }
    return new CascadesCostImpl(rowCount * factor, cpu * factor, io * factor);
  }

  @Override public double divideBy(RelOptCost cost) {
    throw new UnsupportedOperationException();
  }

  public String toString() {
    return "{total=" + scalarCost() + ", " + rowCount + " rows, " + cpu + " cpu, " + io + " io}";
  }

  private static class Factory implements RelOptCostFactory {
    public RelOptCost makeCost(double dRows, double dCpu, double dIo) {
      return new CascadesCostImpl(dRows, dCpu, dIo);
    }

    public RelOptCost makeHugeCost() {
      return HUGE;
    }

    public RelOptCost makeInfiniteCost() {
      return INFINITY;
    }

    public RelOptCost makeTinyCost() {
      return TINY;
    }

    public RelOptCost makeZeroCost() {
      return ZERO;
    }
  }
}
