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
import org.eigenbase.util.ImmutableIntList;
import org.eigenbase.util.IntList;
import org.eigenbase.util.Util;

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
 * {@link org.eigenbase.rel.WindowRelBase.Partition} objects, and each partition
 * has a set of {@link org.eigenbase.rex.RexOver} objects.
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
    public static WindowRel create(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode child,
        RexProgram program,
        RelDataType rowType)
    {
        // Build a list of distinct windows, partitions and aggregate
        // functions.
        List<WindowRel.Window> windowList =
            new ArrayList<WindowRel.Window>();
        final Map<RexOver, WindowRelBase.RexWinAggCall> aggMap =
            new HashMap<RexOver, WindowRelBase.RexWinAggCall>();


        // Build a list of windows, partitions, and aggregate functions. Each
        // aggregate function will add its arguments as outputs of the input
        // program.
        RexProgramBuilder inputProgramBuilder = null;
        for (RexNode agg : program.getExprList()) {
            if (agg instanceof RexOver) {
                final RexOver over = (RexOver) agg;
                WindowRelBase.RexWinAggCall aggCall =
                    addWindows(windowList, over, inputProgramBuilder);
                aggMap.put(over, aggCall);
            }
        }
        final RexProgram inputProgram = inputProgramBuilder.getProgram();

        // Now the windows are complete, compute their digests.
        for (WindowRel.Window window : windowList) {
            window.computeDigest();
        }

        // Figure out the type of the inputs to the output program.
        // They are: the inputs to this rel, followed by the outputs of
        // each window.
        final List<WindowRelBase.RexWinAggCall> flattenedAggCallList =
            new ArrayList<WindowRelBase.RexWinAggCall>();
        List<String> intermediateNameList =
            new ArrayList<String>(child.getRowType().getFieldNames());
        final List<RelDataType> intermediateTypeList =
            new ArrayList<RelDataType>(
                RelOptUtil.getFieldTypeList(child.getRowType()));

        int i = -1;
        for (WindowRel.Window window : windowList) {
            ++i;
            int j = -1;
            for (WindowRel.Partition p : window.partitionList) {
                ++j;
                int k = -1;
                for (WindowRelBase.RexWinAggCall over : p.overList) {
                    ++k;

                    // Add the k'th over expression of the j'th partition of
                    // the i'th window to the output of the program.
                    intermediateNameList.add("w" + i + "$p" + j + "$o" + k);
                    intermediateTypeList.add(over.getType());
                    flattenedAggCallList.add(over);
                }
            }
        }
        RelDataType intermediateRowType =
            cluster.getTypeFactory().createStructType(
                intermediateTypeList,
                intermediateNameList);

        // The output program is the windowed agg's program, combined with
        // the output calc (if it exists).
        RexProgramBuilder outputProgramBuilder =
            new RexProgramBuilder(
                intermediateRowType,
                cluster.getRexBuilder());
        final int inputFieldCount = child.getRowType().getFieldCount();
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
                        intermediateTypeList.get(index),
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
        for (RexNode expr : program.getExprList()) {
            expr = expr.accept(shuttle);
            outputProgramBuilder.registerInput(expr);
        }

        final List<String> fieldNames =
            rowType.getFieldNames();
        i = -1;
        for (RexLocalRef ref : program.getProjectList()) {
            ++i;
            int index = ref.getIndex();
            final RexNode expr = program.getExprList().get(index);
            RexNode expr2 = expr.accept(shuttle);
            outputProgramBuilder.addProject(
                outputProgramBuilder.registerInput(expr2),
                fieldNames.get(i));
        }

        // Create the output program.
        final RexProgram outputProgram;
        if (null == null) {
            outputProgram = outputProgramBuilder.getProgram();
            assert RelOptUtil.eq(
                "type1",
                outputProgram.getOutputRowType(),
                "type2",
                rowType,
                true);
        } else {
            // Merge intermediate program (from winAggRel) with output program
            // (from outCalc).
            RexProgram intermediateProgram = outputProgramBuilder.getProgram();
            outputProgram =
                RexProgramBuilder.mergePrograms(
                    ((CalcRel) null).getProgram(),
                    intermediateProgram,
                    cluster.getRexBuilder());
            assert RelOptUtil.eq(
                "type1",
                outputProgram.getInputRowType(),
                "type2",
                intermediateRowType,
                true);
            assert RelOptUtil.eq(
                "type1",
                outputProgram.getOutputRowType(),
                "type2",
                ((CalcRel) null).getRowType(),
                true);
        }

        // TODO:
        return new WindowRel(cluster, traitSet, child, rowType, null);
    }

    private static WindowRelBase.RexWinAggCall addWindows(
        List<WindowRelBase.Window> windowList,
        RexOver over,
        RexProgramBuilder programBuilder)
    {
        final RexWindow aggWindow = over.getWindow();

        // Look up or create a window.
        ImmutableIntList orderKeys =
            getProjectOrdinals(programBuilder, aggWindow.orderKeys);
        WindowRel.Window fennelWindow =
            lookupWindow(
                windowList,
                aggWindow.isRows(),
                aggWindow.getLowerBound(),
                aggWindow.getUpperBound(),
                orderKeys);

        // Lookup or create a partition within the window.
        ImmutableIntList partitionKeys =
            getProjectOrdinals(programBuilder, aggWindow.partitionKeys);
        WindowRel.Partition fennelPartition =
            fennelWindow.lookupOrCreatePartition(partitionKeys);
        Util.discard(fennelPartition);

        // Create a clone the 'over' expression, omitting the window (which is
        // already part of the partition spec), and add the clone to the
        // partition.
        return fennelPartition.addOver(
            over.getType(),
            over.getAggOperator(),
            over.getOperands(),
            programBuilder);
    }

    /**
     * Converts a list of expressions into a list of ordinals that these
     * expressions are projected from a {@link org.eigenbase.rex.RexProgramBuilder}. If an
     * expression is not projected, adds it.
     *
     *
     * @param programBuilder Program builder
     * @param exprs List of expressions
     *
     * @return List of ordinals where expressions are projected
     */
    private static ImmutableIntList getProjectOrdinals(
        RexProgramBuilder programBuilder,
        List<RexNode> exprs)
    {
        IntList list = new IntList();
        for (RexNode expr : exprs) {
            List<RexLocalRef> projectList = programBuilder.getProjectList();
            int index = projectList.indexOf(expr);
            if (index < 0) {
                index = projectList.size();
                programBuilder.addProject(expr, null);
            }
            list.add(index);
        }
        return list.asImmutable();
    }

    private static WindowRel.Window lookupWindow(
        List<WindowRel.Window> windowList,
        boolean physical,
        SqlNode lowerBound,
        SqlNode upperBound,
        ImmutableIntList orderKeys)
    {
        for (WindowRel.Window window : windowList) {
            if ((physical == window.physical)
                && Util.equal(lowerBound, window.lowerBound)
                && Util.equal(upperBound, window.upperBound)
                && orderKeys.equals(window.orderKeys))
            {
                return window;
            }
        }
        final WindowRel.Window window =
            new WindowRel.Window(
                physical,
                lowerBound,
                upperBound,
                orderKeys);
        windowList.add(window);
        return window;
    }
}

// End WindowRel.java
