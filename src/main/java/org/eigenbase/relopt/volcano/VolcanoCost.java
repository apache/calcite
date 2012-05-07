/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.eigenbase.relopt.volcano;

import org.eigenbase.relopt.*;


/**
 * <code>VolcanoCost</code> represents the cost of a plan node.
 *
 * <p>This class is immutable: none of the methods (besides {@link #set})
 * modifies any member variables.</p>
 */
class VolcanoCost
    implements RelOptCost
{
    //~ Static fields/initializers ---------------------------------------------

    static final VolcanoCost INFINITY =
        new VolcanoCost(
            Double.POSITIVE_INFINITY,
            Double.POSITIVE_INFINITY,
            Double.POSITIVE_INFINITY)
        {
            public String toString()
            {
                return "{inf}";
            }
        };

    static final VolcanoCost HUGE =
        new VolcanoCost(Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE) {
            public String toString()
            {
                return "{huge}";
            }
        };

    static final VolcanoCost ZERO =
        new VolcanoCost(0.0, 0.0, 0.0) {
            public String toString()
            {
                return "{0}";
            }
        };

    static final VolcanoCost TINY =
        new VolcanoCost(1.0, 1.0, 0.0) {
            public String toString()
            {
                return "{tiny}";
            }
        };

    //~ Instance fields --------------------------------------------------------

    double dCpu;
    double dIo;
    double dRows;

    //~ Constructors -----------------------------------------------------------

    VolcanoCost(
        double dRows,
        double dCpu,
        double dIo)
    {
        set(dRows, dCpu, dIo);
    }

    //~ Methods ----------------------------------------------------------------

    public double getCpu()
    {
        return dCpu;
    }

    public boolean isInfinite()
    {
        return (this == INFINITY) || (this.dRows == Double.POSITIVE_INFINITY)
            || (this.dCpu == Double.POSITIVE_INFINITY)
            || (this.dIo == Double.POSITIVE_INFINITY);
    }

    public double getIo()
    {
        return dIo;
    }

    public boolean isLe(RelOptCost other)
    {
        VolcanoCost that = (VolcanoCost) other;
        return (this == that)
            || ((this.dRows <= that.dRows)
                && (this.dCpu <= that.dCpu)
                && (this.dIo <= that.dIo));
    }

    public boolean isLt(RelOptCost other)
    {
        return isLe(other) && !equals(other);
    }

    public double getRows()
    {
        return dRows;
    }

    public boolean equals(RelOptCost other)
    {
        if (!(other instanceof VolcanoCost)) {
            return false;
        }
        VolcanoCost that = (VolcanoCost) other;
        return (this == that)
            || ((this.dRows == that.dRows)
                && (this.dCpu == that.dCpu)
                && (this.dIo == that.dIo));
    }

    public boolean isEqWithEpsilon(RelOptCost other)
    {
        if (!(other instanceof VolcanoCost)) {
            return false;
        }
        VolcanoCost that = (VolcanoCost) other;
        return (this == that)
            || ((Math.abs(this.dRows - that.dRows) < RelOptUtil.EPSILON)
                && (Math.abs(this.dCpu - that.dCpu) < RelOptUtil.EPSILON)
                && (Math.abs(this.dIo - that.dIo) < RelOptUtil.EPSILON));
    }

    public RelOptCost minus(RelOptCost other)
    {
        if (this == INFINITY) {
            return this;
        }
        VolcanoCost that = (VolcanoCost) other;
        return new VolcanoCost(
            this.dRows - that.dRows,
            this.dCpu - that.dCpu,
            this.dIo - that.dIo);
    }

    public RelOptCost multiplyBy(double factor)
    {
        if (this == INFINITY) {
            return this;
        }
        return new VolcanoCost(dRows * factor, dCpu * factor, dIo * factor);
    }

    public double divideBy(RelOptCost cost)
    {
        // Compute the geometric average of the ratios of all of the factors
        // which are non-zero and finite.
        VolcanoCost that = (VolcanoCost) cost;
        double d = 1;
        double n = 0;
        if ((this.dRows != 0)
            && !Double.isInfinite(this.dRows)
            && (that.dRows != 0)
            && !Double.isInfinite(that.dRows))
        {
            d *= this.dRows / that.dRows;
            ++n;
        }
        if ((this.dCpu != 0)
            && !Double.isInfinite(this.dCpu)
            && (that.dCpu != 0)
            && !Double.isInfinite(that.dCpu))
        {
            d *= this.dCpu / that.dCpu;
            ++n;
        }
        if ((this.dIo != 0)
            && !Double.isInfinite(this.dIo)
            && (that.dIo != 0)
            && !Double.isInfinite(that.dIo))
        {
            d *= this.dIo / that.dIo;
            ++n;
        }
        if (n == 0) {
            return 1.0;
        }
        return Math.pow(d, 1 / n);
    }

    public RelOptCost plus(RelOptCost other)
    {
        VolcanoCost that = (VolcanoCost) other;
        if ((this == INFINITY) || (that == INFINITY)) {
            return INFINITY;
        }
        return new VolcanoCost(
            this.dRows + that.dRows,
            this.dCpu + that.dCpu,
            this.dIo + that.dIo);
    }

    public void set(
        double dRows,
        double dCpu,
        double dIo)
    {
        this.dRows = dRows;
        this.dCpu = dCpu;
        this.dIo = dIo;
    }

    public String toString()
    {
        return "{" + dRows + " rows, " + dCpu + " cpu, " + dIo + " io}";
    }
}

// End VolcanoCost.java
