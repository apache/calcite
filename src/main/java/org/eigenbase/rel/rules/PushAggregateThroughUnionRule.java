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
package org.eigenbase.rel.rules;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.fun.*;

/**
 * PushAggregateThroughUnionRule implements the rule for pushing an
 * {@link AggregateRel} past a non-distinct {@link UnionRel}.
 */
public class PushAggregateThroughUnionRule extends RelOptRule
{
    public static final PushAggregateThroughUnionRule instance =
        new PushAggregateThroughUnionRule();

    /**
     * Private constructor.
     */
    private PushAggregateThroughUnionRule()
    {
        super(
            some(
                AggregateRel.class, any(UnionRel.class)));
    }

    public void onMatch(RelOptRuleCall call)
    {
        AggregateRel aggRel = call.rel(0);
        UnionRel unionRel = call.rel(1);

        if (!unionRel.all) {
            // This transformation is only valid for UNION ALL.
            // Consider t1(i) with rows (5), (5) and t2(i) with
            // rows (5), (10), and the query
            // select sum(i) from (select i from t1) union (select i from t2).
            // The correct answer is 15.  If we apply the transformation,
            // we get
            // select sum(i) from
            // (select sum(i) as i from t1) union (select sum(i) as i from t2)
            // which yields 25 (incorrect).
            return;
        }

        RelOptCluster cluster = unionRel.getCluster();

        BitSet groupByKeyMask = new BitSet();
        for (int i = 0; i < aggRel.getGroupCount(); i++) {
            groupByKeyMask.set(i);
        }

        List<AggregateCall> transformedAggCalls =
            transformAggCalls(
                aggRel.getCluster().getTypeFactory(),
                aggRel.getGroupSet().cardinality(),
                aggRel.getAggCallList());
        if (transformedAggCalls == null) {
            // we've detected the presence of something like AVG,
            // which we can't handle
            return;
        }

        boolean anyTransformed = false;

        // create corresponding aggs on top of each union child
        List<RelNode> newUnionInputs = new ArrayList<RelNode>();
        for (RelNode input : unionRel.getInputs()) {
            boolean alreadyUnique =
                RelMdUtil.areColumnsDefinitelyUnique(
                    input,
                    aggRel.getGroupSet());

            if (alreadyUnique) {
                newUnionInputs.add(input);
            } else {
                anyTransformed = true;
                newUnionInputs.add(
                    new AggregateRel(
                        cluster, input,
                        aggRel.getGroupSet(),
                        aggRel.getAggCallList()));
            }
        }

        if (!anyTransformed) {
            // none of the children could benefit from the pushdown,
            // so bail out (preventing the infinite loop to which most
            // planners would succumb)
            return;
        }

        // create a new union whose children are the aggs created above
        UnionRel newUnionRel = new UnionRel(cluster, newUnionInputs, true);

        AggregateRel newTopAggRel = new AggregateRel(
            cluster,
            newUnionRel,
            aggRel.getGroupSet(),
            transformedAggCalls);

        // In case we transformed any COUNT (which is always NOT NULL)
        // to SUM (which is always NULLABLE), cast back to keep the
        // planner happy.
        RelNode castRel = RelOptUtil.createCastRel(
            newTopAggRel,
            aggRel.getRowType(),
            false);

        call.transformTo(castRel);
    }

    private List<AggregateCall> transformAggCalls(
        RelDataTypeFactory typeFactory,
        int nGroupCols,
        List<AggregateCall> origCalls)
    {
        List<AggregateCall> newCalls = new ArrayList<AggregateCall>();
        int iInput = nGroupCols;
        for (AggregateCall origCall : origCalls) {
            if (origCall.isDistinct()) {
                return null;
            }
            if (origCall.getAggregation().getName().equals("AVG")) {
                return null;
            }
            // TODO jvs 13-May-2009: don't assume we know how to handle
            // everything else.
            Aggregation aggFun;
            RelDataType aggType;
            if (origCall.getAggregation().getName().equals("COUNT")) {
                aggType = typeFactory.createTypeWithNullability(
                    origCall.getType(), true);
                aggFun = new SqlSumAggFunction(aggType);
            } else {
                aggFun = origCall.getAggregation();
                aggType = origCall.getType();
            }
            AggregateCall newCall =
                new AggregateCall(
                    aggFun,
                    origCall.isDistinct(),
                    Collections.singletonList(iInput),
                    aggType,
                    origCall.getName());
            newCalls.add(newCall);
            ++iInput;
        }
        return newCalls;
    }
}

// End PushAggregateThroughUnionRule.java
