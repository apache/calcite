/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package org.eigenbase.rel;

import java.util.*;

import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.util.*;


/**
 * <code>AggregateRelBase</code> is an abstract base class for implementations
 * of {@link AggregateRel}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class AggregateRelBase
    extends SingleRel
{
    //~ Instance fields --------------------------------------------------------

    protected List<AggregateCall> aggCalls;
    protected int groupCount;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates an AggregateRelBase.
     *
     * @param cluster Cluster
     * @param traits Traits
     * @param child Child
     * @param groupCount Size of grouping key
     * @param aggCalls Collection of calls to aggregate functions
     */
    protected AggregateRelBase(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode child,
        int groupCount,
        List<AggregateCall> aggCalls)
    {
        super(cluster, traits, child);
        Util.pre(aggCalls != null, "aggCalls != null");
        this.groupCount = groupCount;
        this.aggCalls = aggCalls;
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelNode
    public boolean isDistinct()
    {
        // REVIEW jvs 5-Apr-2008:  Shouldn't this just return true always?
        // How can the result of aggregation have any duplicates?

        return (aggCalls.size() == 0)
            && (groupCount == getChild().getRowType().getFieldList().size());
    }

    /**
     * Returns a list of calls to aggregate functions.
     *
     * @return list of calls to aggregate functions
     */
    public List<AggregateCall> getAggCallList()
    {
        return aggCalls;
    }

    /**
     * Returns the number of grouping fields. These fields are the leading
     * fields in both the input and output records.
     *
     * @return number of grouping fields
     */
    public int getGroupCount()
    {
        return groupCount;
    }

    public void explain(RelOptPlanWriter pw)
    {
        List<String> names = new ArrayList<String>();
        List<Object> values = new ArrayList<Object>();
        names.add("child");
        names.add("groupCount");
        values.add(groupCount);
        int i = -1;
        for (AggregateCall aggCall : aggCalls) {
            ++i;
            final String name;
            if (aggCall.getName() != null) {
                name = aggCall.getName();
            } else {
                name = "agg#" + i;
            }
            names.add(name);
            values.add(aggCall);
        }
        pw.explain(this, names, values);
    }

    // implement RelNode
    public double getRows()
    {
        // Assume that each sort column has 50% of the value count.
        // Therefore one sort column has .5 * rowCount,
        // 2 sort columns give .75 * rowCount.
        // Zero sort columns yields 1 row (or 0 if the input is empty).
        if (groupCount == 0) {
            return 1;
        } else {
            double rowCount = super.getRows();
            rowCount *= (1.0 - Math.pow(.5, groupCount));
            return rowCount;
        }
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        // REVIEW jvs 24-Aug-2008:  This is bogus, but no more bogus
        // than what's currently in JoinRelBase.
        double rowCount = RelMetadataQuery.getRowCount(this);
        return planner.makeCost(rowCount, 0, 0);
    }

    protected RelDataType deriveRowType()
    {
        return getCluster().getTypeFactory().createStructType(
            new RelDataTypeFactory.FieldInfo() {
                public int getFieldCount()
                {
                    return groupCount + aggCalls.size();
                }

                public String getFieldName(int index)
                {
                    if (index < groupCount) {
                        return getChild().getRowType().getFields()[index]
                            .getName();
                    } else {
                        final AggregateCall aggCall =
                            aggCalls.get(index - groupCount);
                        if (aggCall.getName() != null) {
                            return aggCall.getName();
                        } else {
                            return "$f" + index;
                        }
                    }
                }

                public RelDataType getFieldType(int index)
                {
                    if (index < groupCount) {
                        return getChild().getRowType().getFields()[index]
                            .getType();
                    } else {
                        final AggregateCall aggCall =
                            aggCalls.get(index - groupCount);
                        assert typeMatchesInferred(aggCall, true);
                        return aggCall.getType();
                    }
                }
            });
    }

    /**
     * Returns whether the inferred type of an {@link AggregateCall} matches the
     * type it was given when it was created.
     *
     * @param aggCall Aggregate call
     * @param fail Whether to fail if the types do not match
     *
     * @return Whether the inferred and declared types match
     */
    private boolean typeMatchesInferred(
        final AggregateCall aggCall,
        final boolean fail)
    {
        SqlAggFunction aggFunction = (SqlAggFunction) aggCall.getAggregation();
        AggCallBinding callBinding = aggCall.createBinding(this);
        RelDataType type = aggFunction.inferReturnType(callBinding);
        RelDataType expectedType = aggCall.getType();
        return RelOptUtil.eq(
            "aggCall type",
            expectedType,
            "inferred type",
            type,
            fail);
    }

    /**
     * Returns whether any of the aggregates are DISTINCT.
     */
    public boolean containsDistinctCall()
    {
        for (AggregateCall call : aggCalls) {
            if (call.isDistinct()) {
                return true;
            }
        }
        return false;
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Implementation of the {@link SqlOperatorBinding} interface for an {@link
     * AggregateCall aggregate call} applied to a set of operands in the context
     * of a {@link AggregateRel}.
     */
    public static class AggCallBinding
        extends SqlOperatorBinding
    {
        private final AggregateRelBase aggregateRel;
        private final List<Integer> operands;

        /**
         * Creates an AggCallBinding
         *
         * @param typeFactory Type factory
         * @param aggFunction Aggregation function
         * @param aggregateRel Relational expression which is context
         * @param operands Operand ordinals
         */
        AggCallBinding(
            RelDataTypeFactory typeFactory,
            SqlAggFunction aggFunction,
            AggregateRelBase aggregateRel,
            List<Integer> operands)
        {
            super(typeFactory, aggFunction);
            this.aggregateRel = aggregateRel;
            this.operands = operands;
        }

        public int getOperandCount()
        {
            return operands.size();
        }

        public RelDataType getOperandType(int ordinal)
        {
            final RelDataType childType = aggregateRel.getChild().getRowType();
            int operand = operands.get(ordinal);
            return childType.getFields()[operand].getType();
        }

        public EigenbaseException newError(
            SqlValidatorException e)
        {
            return SqlUtil.newContextException(SqlParserPos.ZERO, e);
        }
    }
}

// End AggregateRelBase.java
