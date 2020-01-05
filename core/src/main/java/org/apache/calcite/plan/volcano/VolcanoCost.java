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

import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptUtil;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Objects;

/**
 * <code>VolcanoCost</code> represents the cost of a plan node.
 *
 * <p>This class is immutable: none of the methods modify any member
 * variables.</p>
 */
class VolcanoCost implements RelOptCost {
  //~ Static fields/initializers ---------------------------------------------

  private static final MathContext MATH_CONTEXT = new MathContext(5);

  static final VolcanoCost INFINITY =
      new VolcanoCost(
          Double.POSITIVE_INFINITY,
          Double.POSITIVE_INFINITY,
          Double.POSITIVE_INFINITY,
          0) {
        public String toString() {
          return "{inf}";
        }
      };

  static final VolcanoCost HUGE =
      new VolcanoCost(Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE, 0) {
        public String toString() {
          return "{huge}";
        }

        @Override protected double overallCost() {
          return Double.MAX_VALUE;
        }
      };

  static final VolcanoCost ZERO =
      new VolcanoCost(0.0, 0.0, 0.0, 0) {
        public String toString() {
          return "{0}";
        }
      };

  static final VolcanoCost TINY =
      new VolcanoCost(1.0, 1.0, 0.0, 0) {
        public String toString() {
          return "{tiny}";
        }
      };

  public static final Factory FACTORY = new Factory(
      CalciteSystemProperty.VOLCANO_DEFAULT_CPU_PER_IO.value());

  //~ Instance fields --------------------------------------------------------

  final double cpu;
  final double io;
  final double rowCount;
  final double cpuPerIo;

  //~ Constructors -----------------------------------------------------------

  VolcanoCost(double rowCount, double cpu, double io, double cpuPerIo) {
    this.rowCount = rowCount;
    this.cpu = cpu;
    this.io = io;
    this.cpuPerIo = cpuPerIo;
  }

  //~ Methods ----------------------------------------------------------------

  protected double overallCost() {
    if (isInfinite()) {
      return Double.POSITIVE_INFINITY;
    }
    return cpu + io * cpuPerIo;
  }

  public double getCpu() {
    return cpu;
  }

  public boolean isInfinite() {
    return (this == INFINITY)
        || (rowCount == Double.POSITIVE_INFINITY)
        || (cpu == Double.POSITIVE_INFINITY)
        || (io == Double.POSITIVE_INFINITY);
  }

  public double getIo() {
    return io;
  }

  public boolean isLe(RelOptCost other) {
    VolcanoCost that = (VolcanoCost) other;
    return this == that
        || that == INFINITY
        || cpu <= that.cpu && io <= that.io
        || overallCost() <= that.overallCost();
  }

  public boolean isLt(RelOptCost other) {
    VolcanoCost that = (VolcanoCost) other;
    return this != that
        && (cpu < that.cpu && io <= that.io
        || cpu <= that.cpu && io < that.io
        || overallCost() < that.overallCost());
  }

  public double getRows() {
    return rowCount;
  }

  @Override public int hashCode() {
    return Objects.hash(rowCount, cpu, io);
  }

  public boolean equals(RelOptCost other) {
    return this == other
        || other instanceof VolcanoCost
        && this.cpu == ((VolcanoCost) other).cpu
        && this.io == ((VolcanoCost) other).io;
  }

  @Override public boolean equals(Object obj) {
    return obj instanceof RelOptCost && equals((RelOptCost) obj);
  }

  public boolean isEqWithEpsilon(RelOptCost other) {
    VolcanoCost that = (VolcanoCost) other;
    return this == that
        || Math.abs(this.cpu - that.cpu) < RelOptUtil.EPSILON
        && Math.abs(this.io - that.io) < RelOptUtil.EPSILON;
  }

  public RelOptCost minus(RelOptCost other) {
    if (this == INFINITY) {
      return this;
    }
    VolcanoCost that = (VolcanoCost) other;
    assertSameCpuPerIo(that, " - ");
    return new VolcanoCost(
        rowCount - that.rowCount,
        cpu - that.cpu,
        io - that.io,
        cpuPerIo);
  }

  public RelOptCost multiplyBy(double factor) {
    if (this == INFINITY) {
      return this;
    }
    return new VolcanoCost(
        rowCount * factor, cpu * factor, io * factor, cpuPerIo);
  }

  public double divideBy(RelOptCost cost) {
    // Compute the geometric average of the ratios of all of the factors
    // which are non-zero and finite.
    VolcanoCost that = (VolcanoCost) cost;
    assertSameCpuPerIo(that, " / ");
    double d = 1;
    double n = 0;
    if ((this.cpu != 0)
        && !Double.isInfinite(this.cpu)
        && (that.cpu != 0)
        && !Double.isInfinite(that.cpu)) {
      d *= this.cpu / that.cpu;
      ++n;
    }
    if ((this.io != 0)
        && !Double.isInfinite(this.io)
        && (that.io != 0)
        && !Double.isInfinite(that.io)) {
      d *= this.io / that.io;
      ++n;
    }
    if (n == 0) {
      return 1.0;
    }
    return Math.pow(d, 1 / n);
  }

  public RelOptCost plus(RelOptCost other) {
    VolcanoCost that = (VolcanoCost) other;
    if ((this == INFINITY) || (that == INFINITY)) {
      return INFINITY;
    }
    assertSameCpuPerIo(that, " + ");
    return new VolcanoCost(
        rowCount + that.rowCount,
        cpu + that.cpu,
        io + that.io,
        cpuPerIo);
  }

  private void assertSameCpuPerIo(VolcanoCost that, String op) {
    // static HUGE, ZERO, TINY instances have cpuPerIo=0
    assert this.cpuPerIo == that.cpuPerIo || cpuPerIo == 0 || that.cpuPerIo == 0
        : "Cost should have the same cpuPerIo. Actual costs are " + this + op + that;
  }

  private String format(double value) {
    return new BigDecimal(value, MATH_CONTEXT).stripTrailingZeros().toPlainString();
  }

  public String toString() {
    return "{" + format(overallCost()) + " = "
        + format(rowCount) + " rows, " + format(cpu) + " cpu, " + format(io) + " io}";
  }

  /** Implementation of {@link org.apache.calcite.plan.RelOptCostFactory}
   * that creates {@link org.apache.calcite.plan.volcano.VolcanoCost}s. */
  static class Factory implements RelOptCostFactory {
    private final double cpuPerIo;

    private Factory(double cpuPerIo) {
      this.cpuPerIo = cpuPerIo;
    }

    public static Factory of(double cpuPerIo) {
      if (cpuPerIo == FACTORY.cpuPerIo) {
        return FACTORY;
      }
      return new Factory(cpuPerIo);
    }

    public RelOptCost makeCost(double dRows, double dCpu, double dIo) {
      return new VolcanoCost(dRows, dCpu, dIo, cpuPerIo);
    }

    public RelOptCost makeHugeCost() {
      return VolcanoCost.HUGE;
    }

    public RelOptCost makeInfiniteCost() {
      return VolcanoCost.INFINITY;
    }

    public RelOptCost makeTinyCost() {
      return VolcanoCost.TINY;
    }

    public RelOptCost makeZeroCost() {
      return VolcanoCost.ZERO;
    }
  }
}
