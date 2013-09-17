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
package org.eigenbase.rel;

import java.util.*;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.util.Pair;
import org.eigenbase.util.Util;

import net.hydromatic.linq4j.Ord;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;

/**
 * A relational expression representing a set of window aggregates.
 *
 * <p>A window rel can handle several window aggregate functions, over several
 * partitions, with pre- and post-expressions, and an optional post-filter.
 * Each of the partitions is defined by a partition key (zero or more columns)
 * and a range (logical or physical). The partitions expect the data to be
 * sorted correctly on input to the relational expression.
 *
 * <p>Each {@link org.eigenbase.rel.WindowRelBase.Window} has a set of
 * {@link org.eigenbase.rex.RexOver} objects.
 *
 * <p>Created by {@link org.eigenbase.rel.rules.WindowedAggSplitterRule}.
 */
public final class WindowRel extends WindowRelBase {
    /**
     * Creates a WindowRel.
     *
     * @param cluster Cluster
     * @param child Input relational expression
     * @param rowType Output row type
     * @param windows Windows
     */
    public WindowRel(
        RelOptCluster cluster, RelTraitSet traits, RelNode child,
        RelDataType rowType, List<Window> windows)
    {
        super(cluster, traits, child, rowType, windows);
    }

    @Override public WindowRel copy(RelTraitSet traitSet, List<RelNode> inputs)
    {
        return new WindowRel(
            getCluster(), traitSet, sole(inputs), rowType, windows);
    }

    /** Creates a WindowRel. */
    public static RelNode create(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode child,
        final RexProgram program,
        RelDataType outRowType)
    {
        // Build a list of distinct windows, partitions and aggregate
        // functions.
        final Multimap<WindowKey, RexOver> windowMap =
            LinkedListMultimap.create();

        // Build a list of windows, partitions, and aggregate functions. Each
        // aggregate function will add its arguments as outputs of the input
        // program.
        for (RexNode agg : program.getExprList()) {
            if (agg instanceof RexOver) {
                addWindows(windowMap, (RexOver) agg);
            }
        }

        final Map<RexOver, WindowRelBase.RexWinAggCall> aggMap =
            new HashMap<RexOver, WindowRelBase.RexWinAggCall>();
        List<Window> windowList = new ArrayList<Window>();
        for (Map.Entry<WindowKey, Collection<RexOver>> entry
            : windowMap.asMap().entrySet())
        {
            final WindowKey windowKey = entry.getKey();
            final List<RexWinAggCall> aggCalls =
                new ArrayList<RexWinAggCall>();
            for (RexOver over : entry.getValue()) {
                final RexWinAggCall aggCall =
                    new RexWinAggCall(
                        over.getAggOperator(),
                        over.getType(),
                        toInputRefs(over.operands),
                        aggMap.size());
                aggCalls.add(aggCall);
                aggMap.put(over, aggCall);
            }
            windowList.add(
                new Window(
                    windowKey.groupSet,
                    windowKey.isRows,
                    windowKey.lowerBound,
                    windowKey.upperBound,
                    windowKey.orderKeys,
                    aggCalls));
        }

        // Figure out the type of the inputs to the output program.
        // They are: the inputs to this rel, followed by the outputs of
        // each window.
        final List<WindowRelBase.RexWinAggCall> flattenedAggCallList =
            new ArrayList<WindowRelBase.RexWinAggCall>();
        List<Map.Entry<String, RelDataType>> fieldList =
            new ArrayList<Map.Entry<String, RelDataType>>(
                child.getRowType().getFieldList());
        final int offset = fieldList.size();

        // Use better field names for agg calls that are projected.
        Map<Integer, String> fieldNames = new HashMap<Integer, String>();
        for (Ord<RexLocalRef> ref : Ord.zip(program.getProjectList())) {
            final int index = ref.e.getIndex();
            if (index >= offset) {
                fieldNames.put(
                    index - offset, outRowType.getFieldNames().get(ref.i));
            }
        }

        for (Ord<Window> window : Ord.zip(windowList)) {
            for (Ord<RexWinAggCall> over : Ord.zip(window.e.aggCalls)) {
                // Add the k-th over expression of
                // the i-th window to the output of the program.
                String name = fieldNames.get(over.i);
                if (name == null || name.startsWith("$")) {
                    name = "w" + window.i + "$o" + over.i;
                }
                fieldList.add(Pair.of(name, over.e.getType()));
                flattenedAggCallList.add(over.e);
            }
        }
        final RelDataType intermediateRowType =
            cluster.getTypeFactory().createStructType(fieldList);

        final int inputFieldCount = child.getRowType().getFieldCount();

        // The output program is the windowed agg's program, combined with
        // the output calc (if it exists).
        RexShuttle shuttle =
            new RexShuttle() {
                public RexNode visitOver(RexOver over) {
                    // Look up the aggCall which this expr was translated to.
                    final WindowRelBase.RexWinAggCall aggCall =
                        aggMap.get(over);
                    assert aggCall != null;
                    assert RelOptUtil.eq(
                        "over",
                        over.getType(),
                        "aggCall",
                        aggCall.getType(),
                        true);

                    // Find the index of the aggCall among all partitions of all
                    // windows.
                    final int aggCallIndex =
                        flattenedAggCallList.indexOf(aggCall);
                    assert aggCallIndex >= 0;

                    // Replace expression with a reference to the window slot.
                    final int index = inputFieldCount + aggCallIndex;
                    assert RelOptUtil.eq(
                        "over",
                        over.getType(),
                        "intermed",
                        intermediateRowType.getFieldList().get(index).getType(),
                        true);
                    return new RexInputRef(
                        index,
                        over.getType());
                }

                public RexNode visitLocalRef(RexLocalRef localRef) {
                    final int index = localRef.getIndex();
                    if (index < inputFieldCount) {
                        // Reference to input field.
                        return localRef;
                    }
                    return new RexLocalRef(
                        flattenedAggCallList.size() + index,
                        localRef.getType());
                }
            };
        // TODO: The order that the "over" calls occur in the windows and
        // partitions may not match the order in which they occurred in the
        // original expression. We should add a project to permute them.

        WindowRel window =
            new WindowRel(
                cluster, traitSet, child, intermediateRowType, windowList);

        return CalcRel.createProject(
            window,
            new AbstractList<RexNode>() {
              public RexNode get(int index) {
                  final RexLocalRef ref = program.getProjectList().get(index);
                  return new RexInputRef(ref.getIndex(), ref.getType());
              }

              public int size() {
                  return program.getProjectList().size();
              }
            },
            outRowType.getFieldNames());
    }

    private static List<RexNode> toInputRefs(final List<RexNode> operands) {
        return new AbstractList<RexNode>() {
            public int size() {
                return operands.size();
            }
            public RexNode get(int index) {
                final RexNode operand = operands.get(index);
                assert operand instanceof RexLocalRef;
                final RexLocalRef ref = (RexLocalRef) operand;
                return new RexInputRef(ref.getIndex(), ref.getType());
            }
        };
    }

    private static class WindowKey {
        private final BitSet groupSet;
        private final RelCollation orderKeys;
        private final boolean isRows;
        private final SqlNode lowerBound;
        private final SqlNode upperBound;

        public WindowKey(
            BitSet groupSet,
            RelCollation orderKeys,
            boolean isRows,
            SqlNode lowerBound,
            SqlNode upperBound)
        {
            this.groupSet = groupSet;
            this.orderKeys = orderKeys;
            this.isRows = isRows;
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
        }

        @Override
        public int hashCode() {
            return Util.hashV(
                groupSet, orderKeys, isRows, lowerBound, upperBound);
        }

        @Override
        public boolean equals(Object obj) {
            return obj == this
                || obj instanceof WindowKey
                && groupSet.equals(((WindowKey) obj).groupSet)
                && orderKeys.equals(((WindowKey) obj).orderKeys)
                && Util.equal(lowerBound, ((WindowKey) obj).lowerBound)
                && Util.equal(upperBound, ((WindowKey) obj).upperBound)
                && isRows == ((WindowKey) obj).isRows;
        }
    }

    private static void addWindows(
        Multimap<WindowKey, RexOver> windowMap,
        RexOver over)
    {
        final RexWindow aggWindow = over.getWindow();

        // Look up or create a window.
        RelCollation orderKeys = getCollation(aggWindow.orderKeys);
        BitSet groupSet =
            Util.bitSetOf(getProjectOrdinals(aggWindow.partitionKeys));

        WindowKey windowKey =
            new WindowKey(
                groupSet, orderKeys, aggWindow.isRows(),
                aggWindow.getLowerBound(), aggWindow.getUpperBound());
        windowMap.put(windowKey, over);
    }
}

// End WindowRel.java
