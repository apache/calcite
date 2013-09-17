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
package org.eigenbase.relopt;

/**
 * RelOptCostImpl provides a default implementation for the {@link RelOptCost}
 * interface. It it defined in terms of a single scalar quantity; somewhat
 * arbitrarily, it returns this scalar for rows processed and zero for both CPU
 * and I/O.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class RelOptCostImpl
    implements RelOptCost
{
    //~ Instance fields --------------------------------------------------------

    private final double value;

    //~ Constructors -----------------------------------------------------------

    public RelOptCostImpl(double value)
    {
        this.value = value;
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptCost
    public double getRows()
    {
        return value;
    }

    // implement RelOptCost
    public double getIo()
    {
        return 0;
    }

    // implement RelOptCost
    public double getCpu()
    {
        return 0;
    }

    // implement RelOptCost
    public boolean isInfinite()
    {
        return Double.isInfinite(value);
    }

    // implement RelOptCost
    public boolean isLe(RelOptCost other)
    {
        return getRows() <= other.getRows();
    }

    // implement RelOptCost
    public boolean isLt(RelOptCost other)
    {
        return getRows() < other.getRows();
    }

    // implement RelOptCost
    public boolean equals(RelOptCost other)
    {
        return getRows() == other.getRows();
    }

    // implement RelOptCost
    public boolean isEqWithEpsilon(RelOptCost other)
    {
        return Math.abs(getRows() - other.getRows()) < RelOptUtil.EPSILON;
    }

    // implement RelOptCost
    public RelOptCost minus(RelOptCost other)
    {
        return new RelOptCostImpl(getRows() - other.getRows());
    }

    // implement RelOptCost
    public RelOptCost plus(RelOptCost other)
    {
        return new RelOptCostImpl(getRows() + other.getRows());
    }

    // implement RelOptCost
    public RelOptCost multiplyBy(double factor)
    {
        return new RelOptCostImpl(getRows() * factor);
    }

    public double divideBy(RelOptCost cost)
    {
        RelOptCostImpl that = (RelOptCostImpl) cost;
        return this.getRows() / that.getRows();
    }

    // implement RelOptCost
    public String toString()
    {
        if (value == Double.MAX_VALUE) {
            return "huge";
        } else {
            return Double.toString(value);
        }
    }
}

// End RelOptCostImpl.java
