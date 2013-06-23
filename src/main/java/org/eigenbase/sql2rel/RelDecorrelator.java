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
package org.eigenbase.sql2rel;

import java.math.*;

import java.util.*;
import java.util.logging.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.relopt.hep.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.trace.*;
import org.eigenbase.util.*;
import org.eigenbase.util.mapping.Mappings;


/**
 * RelDecorrelator replaces all correlated expressions(corExp) in a relational
 * expression (RelNode) tree with non-correlated expressions that are produced
 * from joining the RelNode that produces the corExp with the RelNode that
 * references it.
 */
public class RelDecorrelator
    implements ReflectiveVisitor
{
    //~ Static fields/initializers ---------------------------------------------

    private static final Logger sqlToRelTracer =
        EigenbaseTrace.getSqlToRelTracer();

    //~ Instance fields --------------------------------------------------------

    // maps built during translation
    private final Map<RelNode, SortedSet<CorrelatorRel.Correlation>>
        mapRefRelToCorVar;
    private final SortedMap<CorrelatorRel.Correlation, CorrelatorRel>
        mapCorVarToCorRel;
    private final Map<RexFieldAccess, CorrelatorRel.Correlation>
        mapFieldAccessToCorVar;

    private final DecorrelateRelVisitor decorrelateVisitor;

    private final RexBuilder rexBuilder;

    // The rel which is being visited
    private RelNode currentRel;

    // maps built during decorrelation
    private final Map<RelNode, RelNode> mapOldToNewRel;

    // map rel to all the newly created correlated variables in its output
    private final Map<RelNode, SortedMap<CorrelatorRel.Correlation, Integer>>
        mapNewRelToMapCorVarToOutputPos;

    // another map to map old input positions to new input positions
    // this is from the view point of the parent rel of a new rel.
    private final Map<RelNode, Map<Integer, Integer>>
        mapNewRelToMapOldToNewOutputPos;

    private final HashSet<CorrelatorRel> generatedCorRels =
        new HashSet<CorrelatorRel>();

    //~ Constructors -----------------------------------------------------------

    public RelDecorrelator(
        RexBuilder rexBuilder,
        Map<RelNode, SortedSet<CorrelatorRel.Correlation>> mapRefRelToCorVar,
        SortedMap<CorrelatorRel.Correlation, CorrelatorRel> mapCorVarToCorRel,
        Map<RexFieldAccess, CorrelatorRel.Correlation> mapFieldAccessToCorVar)
    {
        this.rexBuilder = rexBuilder;
        this.mapRefRelToCorVar = mapRefRelToCorVar;
        this.mapCorVarToCorRel = mapCorVarToCorRel;
        this.mapFieldAccessToCorVar = mapFieldAccessToCorVar;

        decorrelateVisitor = new DecorrelateRelVisitor();
        mapOldToNewRel = new HashMap<RelNode, RelNode>();
        mapNewRelToMapCorVarToOutputPos =
            new HashMap<RelNode,
                SortedMap<CorrelatorRel.Correlation, Integer>>();
        mapNewRelToMapOldToNewOutputPos =
            new HashMap<RelNode, Map<Integer, Integer>>();
    }

    //~ Methods ----------------------------------------------------------------

    public RelNode decorrelate(RelNode root)
    {
        // first adjust count() expression if any
        HepProgramBuilder programBuilder = new HepProgramBuilder();

        programBuilder.addRuleInstance(
            new AdjustProjectForCountAggregateRule(false));
        programBuilder.addRuleInstance(
            new AdjustProjectForCountAggregateRule(true));

        HepPlanner planner =
            new HepPlanner(
                programBuilder.createProgram(),
                true);

        planner.setRoot(root);
        root = planner.findBestExp();

        // Perform decorrelation.
        mapOldToNewRel.clear();
        mapNewRelToMapCorVarToOutputPos.clear();
        mapNewRelToMapOldToNewOutputPos.clear();

        decorrelateVisitor.visit(root, 0, null);

        if (mapOldToNewRel.containsKey(root)) {
            // has been rewritten
            return mapOldToNewRel.get(root);
        } else {
            // not rewritten
            return root;
        }
    }

    public RelNode removeCorrelationViaRule(RelNode root)
    {
        HepProgramBuilder programBuilder = new HepProgramBuilder();

        programBuilder.addRuleInstance(new RemoveSingleAggregateRule());
        programBuilder.addRuleInstance(
            new RemoveCorrelationForScalarProjectRule());
        programBuilder.addRuleInstance(
            new RemoveCorrelationForScalarAggregateRule());

        HepPlanner planner =
            new HepPlanner(
                programBuilder.createProgram(),
                true);

        planner.setRoot(root);
        RelNode newRootRel = planner.findBestExp();

        return newRootRel;
    }

    protected RexNode decorrelateExpr(RexNode exp)
    {
        DecorrelateRexShuttle shuttle = new DecorrelateRexShuttle();
        return exp.accept(shuttle);
    }

    protected RexNode removeCorrelationExpr(
        RexNode exp,
        boolean projectPulledAboveLeftCorrelator)
    {
        RemoveCorrelationRexShuttle shuttle =
            new RemoveCorrelationRexShuttle(
                rexBuilder,
                projectPulledAboveLeftCorrelator);
        return exp.accept(shuttle);
    }

    protected RexNode removeCorrelationExpr(
        RexNode exp,
        boolean projectPulledAboveLeftCorrelator,
        RexInputRef nullIndicator)
    {
        RemoveCorrelationRexShuttle shuttle =
            new RemoveCorrelationRexShuttle(
                rexBuilder,
                projectPulledAboveLeftCorrelator,
                nullIndicator);
        return exp.accept(shuttle);
    }

    protected RexNode removeCorrelationExpr(
        RexNode exp,
        boolean projectPulledAboveLeftCorrelator,
        Set<Integer> isCount)
    {
        RemoveCorrelationRexShuttle shuttle =
            new RemoveCorrelationRexShuttle(
                rexBuilder,
                projectPulledAboveLeftCorrelator,
                isCount);
        return exp.accept(shuttle);
    }

    public void decorrelateRelGeneric(RelNode rel)
    {
        RelNode newRel = rel.copy(rel.getTraitSet(), rel.getInputs());

        if (rel.getInputs().size() > 0) {
            List<RelNode> oldInputs = rel.getInputs();
            List<RelNode> newInputs = new ArrayList<RelNode>();
            for (int i = 0; i < oldInputs.size(); ++i) {
                RelNode newInputRel = mapOldToNewRel.get(oldInputs.get(i));
                if ((newInputRel == null)
                    || mapNewRelToMapCorVarToOutputPos.containsKey(
                        newInputRel))
                {
                    // if child is not rewritten, or if it produces correlated
                    // variables, terminate rewrite
                    return;
                } else {
                    newInputs.add(newInputRel);
                    newRel.replaceInput(i, newInputRel);
                }
            }

            if (!Util.equalShallow(oldInputs, newInputs)) {
                newRel = rel.copy(rel.getTraitSet(), newInputs);
            }
        }

        // the output position should not change since there're no corVars
        // coming from below.
        Map<Integer, Integer> mapOldToNewOutputPos =
            new HashMap<Integer, Integer>();
        for (int i = 0; i < rel.getRowType().getFieldCount(); i++) {
            mapOldToNewOutputPos.put(i, i);
        }
        mapOldToNewRel.put(rel, newRel);
        mapNewRelToMapOldToNewOutputPos.put(newRel, mapOldToNewOutputPos);
    }

    /**
     * Rewrite SortRel.
     *
     * @param rel SortRel to be rewritten
     */
    public void decorrelateRel(SortRel rel)
    {
        //
        // Rewrite logic:
        //
        // 1. change the collations field to reference the new input.
        //

        // SortRel itself should not reference cor vars.
        assert (!mapRefRelToCorVar.containsKey(rel));

        // SortRel only references field positions in collations field.
        // The collations field in the newRel now need to refer to the
        // new output positions in its input.
        // Its output does not change the input ordering, so there's no
        // need to call propagateExpr.

        RelNode oldChildRel = rel.getChild();

        RelNode newChildRel = mapOldToNewRel.get(oldChildRel);
        if (newChildRel == null) {
            // If child has not been rewritten, do not rewrite this rel.
            return;
        }

        Map<Integer, Integer> childMapOldToNewOutputPos =
            mapNewRelToMapOldToNewOutputPos.get(newChildRel);
        assert childMapOldToNewOutputPos != null;
        Mappings.TargetMapping mapping =
            Mappings.target(
                childMapOldToNewOutputPos,
                oldChildRel.getRowType().getFieldCount(),
                newChildRel.getRowType().getFieldCount());

        RelCollation oldCollation = rel.getCollation();
        RelCollation newCollation = RexUtil.apply(mapping, oldCollation);

        SortRel newRel =
            new SortRel(
                rel.getCluster(),
                rel.getCluster().traitSetOf(Convention.NONE),
                newChildRel,
                newCollation);

        mapOldToNewRel.put(rel, newRel);

        // SortRel does not change input ordering
        mapNewRelToMapOldToNewOutputPos.put(newRel, childMapOldToNewOutputPos);
    }

    /**
     * Rewrite AggregateRel.
     *
     * @param rel the project rel to rewrite
     */
    public void decorrelateRel(AggregateRel rel)
    {
        //
        // Rewrite logic:
        //
        // 1. Permute the group by keys to the front.
        // 2. If the child of an AggregateRel produces correlated variables,
        //    add them to the group list.
        // 3. Change aggCalls to reference the new ProjectRel.
        //

        // AggregaterRel itself should not reference cor vars.
        assert (!mapRefRelToCorVar.containsKey(rel));

        RelNode oldChildRel = rel.getChild();

        RelNode newChildRel = mapOldToNewRel.get(oldChildRel);
        if (newChildRel == null) {
            // If child has not been rewritten, do not rewrite this rel.
            return;
        }

        Map<Integer, Integer> childMapOldToNewOutputPos =
            mapNewRelToMapOldToNewOutputPos.get(newChildRel);
        assert (childMapOldToNewOutputPos != null);

        // map from newChildRel
        Map<Integer, Integer> mapNewChildToProjOutputPos =
            new HashMap<Integer, Integer>();
        final int oldGroupKeyCount = rel.getGroupSet().cardinality();

        // ProjectRel projects the original expressions,
        // plus any correlated variables the child wants to pass along.
        List<RexNode> exprs = new ArrayList<RexNode>();
        List<String> exprNames = new ArrayList<String>();

        RelDataTypeField [] newChildOutput =
            newChildRel.getRowType().getFields();

        RelDataType fieldType;
        RexInputRef newInput;

        int newChildPos;
        int newPos;

        // oldChildRel has the original group by keys in the front.
        for (newPos = 0; newPos < oldGroupKeyCount; newPos++) {
            newChildPos = childMapOldToNewOutputPos.get(newPos);

            fieldType = newChildOutput[newChildPos].getType();
            newInput = new RexInputRef(newChildPos, fieldType);
            exprs.add(newInput);
            exprNames.add(newChildOutput[newChildPos].getName());

            mapNewChildToProjOutputPos.put(newChildPos, newPos);
        }

        SortedMap<CorrelatorRel.Correlation, Integer> mapCorVarToOutputPos =
            new TreeMap<CorrelatorRel.Correlation, Integer>();

        boolean produceCorVar =
            mapNewRelToMapCorVarToOutputPos.containsKey(newChildRel);
        if (produceCorVar) {
            // If child produces correlated variables, move them to the front,
            // right after any existing groupby fields.

            SortedMap<CorrelatorRel.Correlation, Integer>
                childMapCorVarToOutputPos =
                mapNewRelToMapCorVarToOutputPos.get(newChildRel);

            // Now add the corVars from the child, starting from
            // position oldGroupKeyCount.
            for (
                CorrelatorRel.Correlation corVar
                : childMapCorVarToOutputPos.keySet())
            {
                newChildPos = childMapCorVarToOutputPos.get(corVar);

                fieldType = newChildOutput[newChildPos].getType();
                newInput = new RexInputRef(newChildPos, fieldType);
                exprs.add(newInput);
                exprNames.add(newChildOutput[newChildPos].getName());

                mapCorVarToOutputPos.put(corVar, newPos);
                mapNewChildToProjOutputPos.put(newChildPos, newPos);
                newPos++;
            }
        }

        // add the remaining fields
        final int newGroupKeyCount = newPos;
        for (int i = 0; i < newChildOutput.length; i++) {
            if (!mapNewChildToProjOutputPos.containsKey(i)) {
                fieldType = newChildOutput[i].getType();
                newInput = new RexInputRef(i, fieldType);
                exprs.add(newInput);
                exprNames.add(newChildOutput[i].getName());

                mapNewChildToProjOutputPos.put(i, newPos);
                newPos++;
            }
        }

        assert (newPos == newChildOutput.length);

        // This ProjectRel will be what the old child maps to,
        // replacing any previous mapping from old child).
        RelNode newProjectRel =
            CalcRel.createProject(newChildRel, exprs, exprNames);

        // update mappings:
        // oldChildRel ----> newChildRel
        //
        //                   newProjectRel
        //                        |
        // oldChildRel ---->  newChildRel
        //
        // is transformed to
        //
        // oldChildRel ----> newProjectRel
        //                        |
        //                   newChildRel
        Map<Integer, Integer> combinedMap = new HashMap<Integer, Integer>();

        for (Integer oldChildPos : childMapOldToNewOutputPos.keySet()) {
            combinedMap.put(
                oldChildPos,
                mapNewChildToProjOutputPos.get(
                    childMapOldToNewOutputPos.get(oldChildPos)));
        }

        mapOldToNewRel.put(oldChildRel, newProjectRel);
        mapNewRelToMapOldToNewOutputPos.put(newProjectRel, combinedMap);

        if (produceCorVar) {
            mapNewRelToMapCorVarToOutputPos.put(
                newProjectRel,
                mapCorVarToOutputPos);
        }

        // now it's time to rewrite AggregateRel
        List<AggregateCall> newAggCalls = new ArrayList<AggregateCall>();
        List<AggregateCall> oldAggCalls = rel.getAggCallList();

        // AggregateRel.Call oldAggCall;
        int oldChildOutputFieldCount = oldChildRel.getRowType().getFieldCount();
        int newChildOutputFieldCount =
            newProjectRel.getRowType().getFieldCount();

        int i = -1;
        for (AggregateCall oldAggCall : oldAggCalls) {
            ++i;
            List<Integer> oldAggArgs = oldAggCall.getArgList();

            List<Integer> aggArgs = new ArrayList<Integer>();

            // Adjust the aggregator argument positions.
            // Note aggregator does not change input ordering, so the child
            // output position mapping can be used to derive the new positions
            // for the argument.
            for (int k = 0; k < oldAggArgs.size(); k++) {
                int oldPos = oldAggArgs.get(k);
                aggArgs.add(combinedMap.get(oldPos));
            }

            newAggCalls.add(
                new AggregateCall(
                    oldAggCall.getAggregation(),
                    oldAggCall.isDistinct(),
                    aggArgs,
                    oldAggCall.getType(),
                    oldAggCall.getName()));

            // The old to new output position mapping will be the same as that
            // of newProjectRel, plus any aggregates that the oldAgg produces.
            combinedMap.put(
                oldChildOutputFieldCount + i,
                newChildOutputFieldCount + i);
        }

        AggregateRel newAggregateRel =
            new AggregateRel(
                rel.getCluster(),
                newProjectRel,
                Util.bitSetBetween(0, newGroupKeyCount),
                newAggCalls);

        mapOldToNewRel.put(rel, newAggregateRel);

        mapNewRelToMapOldToNewOutputPos.put(newAggregateRel, combinedMap);

        if (produceCorVar) {
            // AggregaterRel does not change input ordering so corVars will be
            // located at the same position as the input newProjectRel.
            mapNewRelToMapCorVarToOutputPos.put(
                newAggregateRel,
                mapCorVarToOutputPos);
        }
    }

    /**
     * Rewrite ProjectRel.
     *
     * @param rel the project rel to rewrite
     */
    public void decorrelateRel(ProjectRel rel)
    {
        //
        // Rewrite logic:
        //
        // 1. Pass along any correlated variables coming from the child.
        //

        RelNode oldChildRel = rel.getChild();

        RelNode newChildRel = mapOldToNewRel.get(oldChildRel);
        if (newChildRel == null) {
            // If child has not been rewritten, do not rewrite this rel.
            return;
        }
        RexNode [] oldProj = rel.getProjectExps();
        RelDataTypeField [] relOutput = rel.getRowType().getFields();

        Map<Integer, Integer> childMapOldToNewOutputPos =
            mapNewRelToMapOldToNewOutputPos.get(newChildRel);
        assert (childMapOldToNewOutputPos != null);

        Map<Integer, Integer> mapOldToNewOutputPos =
            new HashMap<Integer, Integer>();

        boolean produceCorVar =
            mapNewRelToMapCorVarToOutputPos.containsKey(newChildRel);

        // ProjectRel projects the original expressions,
        // plus any correlated variables the child wants to pass along.
        List<RexNode> exprs = new ArrayList<RexNode>();
        List<String> exprNames = new ArrayList<String>();

        // If this ProjectRel has correlated reference, create value generator
        // and produce the correlated variables in the new output.
        if (mapRefRelToCorVar.containsKey(rel)) {
            decorrelateInputWithValueGenerator(rel);

            // The old child should be mapped to the JoinRel created by
            // rewriteInputWithValueGenerator().
            newChildRel = mapOldToNewRel.get(oldChildRel);
            produceCorVar = true;
        }

        // ProjectRel projects the original expressions
        int newPos;
        for (newPos = 0; newPos < oldProj.length; newPos++) {
            exprs.add(newPos, decorrelateExpr(oldProj[newPos]));
            exprNames.add(newPos, relOutput[newPos].getName());
            mapOldToNewOutputPos.put(newPos, newPos);
        }

        SortedMap<CorrelatorRel.Correlation, Integer> mapCorVarToOutputPos =
            new TreeMap<CorrelatorRel.Correlation, Integer>();

        // Project any correlated variables the child wants to pass along.
        if (produceCorVar) {
            SortedMap<CorrelatorRel.Correlation, Integer>
                childMapCorVarToOutputPos =
                mapNewRelToMapCorVarToOutputPos.get(newChildRel);

            // propagate cor vars from the new child
            int corVarPos;
            RelDataType fieldType;
            RexInputRef newInput;
            RelDataTypeField [] newChildOutput =
                newChildRel.getRowType().getFields();
            for (
                CorrelatorRel.Correlation corVar
                : childMapCorVarToOutputPos.keySet())
            {
                corVarPos = childMapCorVarToOutputPos.get(corVar);
                fieldType = newChildOutput[corVarPos].getType();
                newInput = new RexInputRef(corVarPos, fieldType);
                exprs.add(newInput);
                exprNames.add(newChildOutput[corVarPos].getName());
                mapCorVarToOutputPos.put(corVar, newPos);
                newPos++;
            }
        }

        RelNode newProjectRel =
            CalcRel.createProject(newChildRel, exprs, exprNames);

        mapOldToNewRel.put(rel, newProjectRel);
        mapNewRelToMapOldToNewOutputPos.put(
            newProjectRel,
            mapOldToNewOutputPos);

        if (produceCorVar) {
            mapNewRelToMapCorVarToOutputPos.put(
                newProjectRel,
                mapCorVarToOutputPos);
        }
    }

    /**
     * Create RelNode tree that produces a list of correlated variables.
     *
     * @param correlations correlated variables to generate
     * @param valueGenFieldOffset offset in the output that generated columns
     * will start
     * @param mapCorVarToOutputPos output positions for the correlated variables
     * generated
     *
     * @return RelNode the root of the resultant RelNode tree
     */
    private RelNode createValueGenerator(
        SortedSet<CorrelatorRel.Correlation> correlations,
        int valueGenFieldOffset,
        SortedMap<CorrelatorRel.Correlation, Integer> mapCorVarToOutputPos)
    {
        RelNode resultRel = null;

        Map<RelNode, List<Integer>> mapNewInputRelToOutputPos =
            new HashMap<RelNode, List<Integer>>();

        Map<RelNode, Integer> mapNewInputRelToNewOffset =
            new HashMap<RelNode, Integer>();

        RelNode oldInputRel;
        RelNode newInputRel;
        List<Integer> newLocalOutputPosList;

        // inputRel provides the definition of a correlated variable.
        // Add to map all the referenced positions(relative to each input rel)
        for (CorrelatorRel.Correlation corVar : correlations) {
            int oldCorVarOffset = corVar.getOffset();

            oldInputRel = mapCorVarToCorRel.get(corVar).getInput(0);
            assert (oldInputRel != null);
            newInputRel = mapOldToNewRel.get(oldInputRel);
            assert (newInputRel != null);

            if (!mapNewInputRelToOutputPos.containsKey(newInputRel)) {
                newLocalOutputPosList = new ArrayList<Integer>();
            } else {
                newLocalOutputPosList =
                    mapNewInputRelToOutputPos.get(newInputRel);
            }

            Map<Integer, Integer> mapOldToNewOutputPos =
                mapNewRelToMapOldToNewOutputPos.get(newInputRel);
            assert (mapOldToNewOutputPos != null);

            int newCorVarOffset = mapOldToNewOutputPos.get(oldCorVarOffset);

            // Add all unique positions referenced.
            if (!newLocalOutputPosList.contains(newCorVarOffset)) {
                newLocalOutputPosList.add(newCorVarOffset);
            }
            mapNewInputRelToOutputPos.put(newInputRel, newLocalOutputPosList);
        }

        int offset = 0;

        // Project only the correlated fields out of each inputRel
        // and join the projectRel together.
        // To make sure the plan does not change in terms of join order,
        // join these rels based on their occurance in cor var list which
        // is sorted.
        Set<RelNode> joinedInputRelSet = new HashSet<RelNode>();

        for (CorrelatorRel.Correlation corVar : correlations) {
            oldInputRel = mapCorVarToCorRel.get(corVar).getInput(0);
            assert (oldInputRel != null);
            newInputRel = mapOldToNewRel.get(oldInputRel);
            assert (newInputRel != null);

            if (!joinedInputRelSet.contains(newInputRel)) {
                RelNode projectRel =
                    CalcRel.createProject(
                        newInputRel,
                        mapNewInputRelToOutputPos.get(newInputRel));
                RelNode distinctRel = RelOptUtil.createDistinctRel(projectRel);
                RelOptCluster cluster = distinctRel.getCluster();

                joinedInputRelSet.add(newInputRel);
                mapNewInputRelToNewOffset.put(newInputRel, offset);
                offset += distinctRel.getRowType().getFieldCount();

                if (resultRel == null) {
                    resultRel = distinctRel;
                } else {
                    resultRel =
                        new JoinRel(
                            cluster,
                            resultRel,
                            distinctRel,
                            cluster.getRexBuilder().makeLiteral(true),
                            JoinRelType.INNER,
                            Collections.<String>emptySet());
                }
            }
        }

        // Translate the positions of correlated variables to be relative to
        // the join output, leaving room for valueGenFieldOffset because
        // valueGenerators are joined with the original left input of the rel
        // referencing correlated variables.
        int newOutputPos, newLocalOutputPos;
        for (CorrelatorRel.Correlation corVar : correlations) {
            // The first child of a correlatorRel is always the rel defining
            // the correlated variables.
            newInputRel =
                mapOldToNewRel.get(mapCorVarToCorRel.get(corVar).getInput(0));
            newLocalOutputPosList = mapNewInputRelToOutputPos.get(newInputRel);

            Map<Integer, Integer> mapOldToNewOutputPos =
                mapNewRelToMapOldToNewOutputPos.get(newInputRel);
            assert (mapOldToNewOutputPos != null);

            newLocalOutputPos = mapOldToNewOutputPos.get(corVar.getOffset());

            // newOutputPos is the index of the cor var in the referenced
            // position list plus the offset of referenced postition list of
            // each newInputRel.
            newOutputPos =
                newLocalOutputPosList.indexOf(newLocalOutputPos)
                + mapNewInputRelToNewOffset.get(newInputRel)
                + valueGenFieldOffset;

            if (mapCorVarToOutputPos.containsKey(corVar)) {
                assert (mapCorVarToOutputPos.get(corVar) == newOutputPos);
            }
            mapCorVarToOutputPos.put(corVar, newOutputPos);
        }

        return resultRel;
    }

    private void decorrelateInputWithValueGenerator(
        RelNode rel)
    {
        // currently only handles one child input
        assert (rel.getInputs().size() == 1);
        RelNode oldChildRel = rel.getInput(0);
        RelNode newChildRel = mapOldToNewRel.get(oldChildRel);

        Map<Integer, Integer> childMapOldToNewOutputPos =
            mapNewRelToMapOldToNewOutputPos.get(newChildRel);
        assert (childMapOldToNewOutputPos != null);

        SortedMap<CorrelatorRel.Correlation, Integer> mapCorVarToOutputPos =
            new TreeMap<CorrelatorRel.Correlation, Integer>();

        if (mapNewRelToMapCorVarToOutputPos.containsKey(newChildRel)) {
            mapCorVarToOutputPos.putAll(
                mapNewRelToMapCorVarToOutputPos.get(newChildRel));
        }

        SortedSet<CorrelatorRel.Correlation> corVarList =
            mapRefRelToCorVar.get(rel);

        RelNode newLeftChildRel = newChildRel;

        int leftChildOutputCount = newLeftChildRel.getRowType().getFieldCount();

        // can directly add positions into mapCorVarToOutputPos since join
        // does not change the output ordering from the children.
        RelNode valueGenRel =
            createValueGenerator(
                corVarList,
                leftChildOutputCount,
                mapCorVarToOutputPos);

        final Set<String> variablesStopped = Collections.emptySet();
        RelNode joinRel =
            new JoinRel(
                rel.getCluster(),
                newLeftChildRel,
                valueGenRel,
                rexBuilder.makeLiteral(true),
                JoinRelType.INNER,
                variablesStopped);

        mapOldToNewRel.put(oldChildRel, joinRel);
        mapNewRelToMapCorVarToOutputPos.put(joinRel, mapCorVarToOutputPos);

        // JoinRel or FilterRel does not change the old input ordering. All
        // input fields from newLeftInput(i.e. the original input to the old
        // FilterRel) are in the output and in the same position.
        mapNewRelToMapOldToNewOutputPos.put(joinRel, childMapOldToNewOutputPos);
    }

    /**
     * Rewrite FilterRel.
     *
     * @param rel the filter rel to rewrite
     */
    public void decorrelateRel(FilterRel rel)
    {
        //
        // Rewrite logic:
        //
        // 1. If a FilterRel references a correlated field in its filter
        // condition, rewrite the FilterRel to be
        //   FilterRel
        //     JoinRel(cross product)
        //       OriginalFilterInput
        //       ValueGenerator(produces distinct sets of correlated variables)
        // and rewrite the correlated fieldAccess in the filter condition to
        // reference the JoinRel output.
        //
        // 2. If FilterRel does not reference correlated variables, simply
        // rewrite the filter condition using new input.
        //

        RelNode oldChildRel = rel.getChild();

        RelNode newChildRel = mapOldToNewRel.get(oldChildRel);
        if (newChildRel == null) {
            // If child has not been rewritten, do not rewrite this rel.
            return;
        }

        Map<Integer, Integer> childMapOldToNewOutputPos =
            mapNewRelToMapOldToNewOutputPos.get(newChildRel);
        assert (childMapOldToNewOutputPos != null);

        boolean produceCorVar =
            mapNewRelToMapCorVarToOutputPos.containsKey(newChildRel);

        // If this FilterRel has correlated reference, create value generator
        // and produce the correlated variables in the new output.
        if (mapRefRelToCorVar.containsKey(rel)) {
            decorrelateInputWithValueGenerator(rel);

            // The old child should be mapped to the newly created JoinRel by
            // rewriteInputWithValueGenerator().
            newChildRel = mapOldToNewRel.get(oldChildRel);
            produceCorVar = true;
        }

        // Replace the filter expression to reference output of the join
        // Map filter to the new filter over join
        RelNode newFilterRel =
            CalcRel.createFilter(
                newChildRel,
                decorrelateExpr(rel.getCondition()));

        mapOldToNewRel.put(rel, newFilterRel);

        // Filter does not change the input ordering.
        mapNewRelToMapOldToNewOutputPos.put(
            newFilterRel,
            childMapOldToNewOutputPos);

        if (produceCorVar) {
            // filter rel does not permute the input all corvars produced by
            // filter will have the same output positions in the child rel.
            mapNewRelToMapCorVarToOutputPos.put(
                newFilterRel,
                mapNewRelToMapCorVarToOutputPos.get(newChildRel));
        }
    }

    /**
     * Rewrite CorrelatorRel into a left outer join.
     *
     * @param rel CorrelatorRel
     */
    public void decorrelateRel(CorrelatorRel rel)
    {
        //
        // Rewrite logic:
        //
        // The original left input will be joined with the new right input that
        // has generated correlated variables propagated up. For any generated
        // cor vars that are not used in the join key, pass them along to be
        // joined later with the CorrelatorRels that produce them.
        //

        // the right input to CorrelatorRel should produce correlated variables
        RelNode oldLeftRel = rel.getInputs().get(0);
        RelNode oldRightRel = rel.getInputs().get(1);

        RelNode newLeftRel = mapOldToNewRel.get(oldLeftRel);
        RelNode newRightRel = mapOldToNewRel.get(oldRightRel);

        if ((newLeftRel == null) || (newRightRel == null)) {
            // If any child has not been rewritten, do not rewrite this rel.
            return;
        }

        SortedMap<CorrelatorRel.Correlation, Integer>
            rightChildMapCorVarToOutputPos =
            mapNewRelToMapCorVarToOutputPos.get(newRightRel);

        if (rightChildMapCorVarToOutputPos == null) {
            return;
        }

        Map<Integer, Integer> leftChildMapOldToNewOutputPos =
            mapNewRelToMapOldToNewOutputPos.get(newLeftRel);
        assert (leftChildMapOldToNewOutputPos != null);

        Map<Integer, Integer> rightChildMapOldToNewOutputPos =
            mapNewRelToMapOldToNewOutputPos.get(newRightRel);

        assert (rightChildMapOldToNewOutputPos != null);

        SortedMap<CorrelatorRel.Correlation, Integer> mapCorVarToOutputPos =
            rightChildMapCorVarToOutputPos;

        assert (rel.getCorrelations().size()
            <= rightChildMapCorVarToOutputPos.keySet().size());

        // Change correlator rel into a join.
        // Join all the correlated variables produced by this correlator rel
        // with the values generated and propagated from the right input
        RexNode condition = rel.getCondition();
        final RelDataTypeField [] newLeftOutput =
            newLeftRel.getRowType().getFields();
        int newLeftFieldCount = newLeftOutput.length;

        final RelDataTypeField [] newRightOutput =
            newRightRel.getRowType().getFields();

        int newLeftPos, newRightPos;
        for (CorrelatorRel.Correlation corVar : rel.getCorrelations()) {
            newLeftPos = leftChildMapOldToNewOutputPos.get(corVar.getOffset());
            newRightPos = rightChildMapCorVarToOutputPos.get(corVar);
            RexNode equi =
                rexBuilder.makeCall(
                    SqlStdOperatorTable.equalsOperator,
                    new RexInputRef(
                        newLeftPos,
                        newLeftOutput[newLeftPos].getType()),
                    new RexInputRef(
                        newLeftFieldCount + newRightPos,
                        newRightOutput[newRightPos].getType()));
            if (condition == rexBuilder.makeLiteral(true)) {
                condition = equi;
            } else {
                condition =
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.andOperator,
                        condition,
                        equi);
            }

            // remove this cor var from output position mapping
            mapCorVarToOutputPos.remove(corVar);
        }

        // Update the output position for the cor vars: only pass on the cor
        // vars that are not used in the join key.
        for (CorrelatorRel.Correlation corVar : mapCorVarToOutputPos.keySet()) {
            int newPos = mapCorVarToOutputPos.get(corVar) + newLeftFieldCount;
            mapCorVarToOutputPos.put(corVar, newPos);
        }

        // then add any cor var from the left input. Do not need to change
        // output positions.
        if (mapNewRelToMapCorVarToOutputPos.containsKey(newLeftRel)) {
            mapCorVarToOutputPos.putAll(
                mapNewRelToMapCorVarToOutputPos.get(newLeftRel));
        }

        // Create the mapping between the output of the old correlation rel
        // and the new join rel
        Map<Integer, Integer> mapOldToNewOutputPos =
            new HashMap<Integer, Integer>();

        int oldLeftFieldCount = oldLeftRel.getRowType().getFieldCount();

        int oldRightFieldCount = oldRightRel.getRowType().getFieldCount();
        assert (rel.getRowType().getFieldCount()
            == (oldLeftFieldCount + oldRightFieldCount));

        // Left input positions are not changed.
        mapOldToNewOutputPos.putAll(leftChildMapOldToNewOutputPos);

        // Right input positions are shifted by newLeftFieldCount.
        for (int i = 0; i < oldRightFieldCount; i++) {
            mapOldToNewOutputPos.put(
                i + oldLeftFieldCount,
                rightChildMapOldToNewOutputPos.get(i) + newLeftFieldCount);
        }

        final Set<String> variablesStopped = Collections.emptySet();
        RelNode newRel =
            new JoinRel(
                rel.getCluster(),
                newLeftRel,
                newRightRel,
                condition,
                rel.getJoinType(),
                variablesStopped);

        mapOldToNewRel.put(rel, newRel);
        mapNewRelToMapOldToNewOutputPos.put(newRel, mapOldToNewOutputPos);

        if (!mapCorVarToOutputPos.isEmpty()) {
            mapNewRelToMapCorVarToOutputPos.put(newRel, mapCorVarToOutputPos);
        }
    }

    /**
     * Rewrite JoinRel.
     *
     * @param rel JoinRel
     */
    public void decorrelateRel(JoinRel rel)
    {
        //
        // Rewrite logic:
        //
        // 1. rewrite join condition.
        // 2. map output positions and produce cor vars if any.
        //

        RelNode oldLeftRel = rel.getInputs().get(0);
        RelNode oldRightRel = rel.getInputs().get(1);

        RelNode newLeftRel = mapOldToNewRel.get(oldLeftRel);
        RelNode newRightRel = mapOldToNewRel.get(oldRightRel);

        if ((newLeftRel == null) || (newRightRel == null)) {
            // If any child has not been rewritten, do not rewrite this rel.
            return;
        }

        Map<Integer, Integer> leftChildMapOldToNewOutputPos =
            mapNewRelToMapOldToNewOutputPos.get(newLeftRel);
        assert (leftChildMapOldToNewOutputPos != null);

        Map<Integer, Integer> rightChildMapOldToNewOutputPos =
            mapNewRelToMapOldToNewOutputPos.get(newRightRel);
        assert (rightChildMapOldToNewOutputPos != null);

        SortedMap<CorrelatorRel.Correlation, Integer> mapCorVarToOutputPos =
            new TreeMap<CorrelatorRel.Correlation, Integer>();

        final Set<String> variablesStopped = Collections.emptySet();
        RelNode newRel =
            new JoinRel(
                rel.getCluster(),
                newLeftRel,
                newRightRel,
                decorrelateExpr(rel.getCondition()),
                rel.getJoinType(),
                variablesStopped);

        // Create the mapping between the output of the old correlation rel
        // and the new join rel
        Map<Integer, Integer> mapOldToNewOutputPos =
            new HashMap<Integer, Integer>();

        int oldLeftFieldCount = oldLeftRel.getRowType().getFieldCount();
        int newLeftFieldCount = newLeftRel.getRowType().getFieldCount();

        int oldRightFieldCount = oldRightRel.getRowType().getFieldCount();
        assert (rel.getRowType().getFieldCount()
            == (oldLeftFieldCount + oldRightFieldCount));

        // Left input positions are not changed.
        mapOldToNewOutputPos.putAll(leftChildMapOldToNewOutputPos);

        // Right input positions are shifted by newLeftFieldCount.
        for (int i = 0; i < oldRightFieldCount; i++) {
            mapOldToNewOutputPos.put(
                i + oldLeftFieldCount,
                rightChildMapOldToNewOutputPos.get(i) + newLeftFieldCount);
        }

        if (mapNewRelToMapCorVarToOutputPos.containsKey(newLeftRel)) {
            mapCorVarToOutputPos.putAll(
                mapNewRelToMapCorVarToOutputPos.get(newLeftRel));
        }

        // Right input positions are shifted by newLeftFieldCount.
        int oldRightPos;
        if (mapNewRelToMapCorVarToOutputPos.containsKey(newRightRel)) {
            SortedMap<CorrelatorRel.Correlation, Integer>
                rightChildMapCorVarToOutputPos =
                mapNewRelToMapCorVarToOutputPos.get(newRightRel);
            for (
                CorrelatorRel.Correlation corVar
                : rightChildMapCorVarToOutputPos.keySet())
            {
                oldRightPos = rightChildMapCorVarToOutputPos.get(corVar);
                mapCorVarToOutputPos.put(
                    corVar,
                    oldRightPos + newLeftFieldCount);
            }
        }
        mapOldToNewRel.put(rel, newRel);
        mapNewRelToMapOldToNewOutputPos.put(newRel, mapOldToNewOutputPos);

        if (!mapCorVarToOutputPos.isEmpty()) {
            mapNewRelToMapCorVarToOutputPos.put(newRel, mapCorVarToOutputPos);
        }
    }

    private RexInputRef getNewForOldInputRef(RexInputRef oldInputRef)
    {
        assert (currentRel != null);

        int oldOrdinal = oldInputRef.getIndex();
        int newOrdinal = 0;

        // determine which input rel oldOrdinal references, and adjust
        // oldOrdinal to be relative to that input rel
        List<RelNode> oldInputRels = currentRel.getInputs();
        RelNode oldInputRel = null;

        for (int i = 0; i < oldInputRels.size(); ++i) {
            RelDataType oldInputType = oldInputRels.get(i).getRowType();
            int n = oldInputType.getFieldCount();
            if (oldOrdinal < n) {
                oldInputRel = oldInputRels.get(i);
                break;
            }
            RelNode newInput = mapOldToNewRel.get(oldInputRels.get(i));
            newOrdinal += newInput.getRowType().getFieldCount();
            oldOrdinal -= n;
        }

        assert (oldInputRel != null);

        RelNode newInputRel = mapOldToNewRel.get(oldInputRel);
        assert (newInputRel != null);

        // now oldOrdinal is relative to oldInputRel
        int oldLocalOrdinal = oldOrdinal;

        // figure out the newLocalOrdinal, relative to the newInputRel.
        int newLocalOrdinal = oldLocalOrdinal;

        Map<Integer, Integer> mapOldToNewOutputPos =
            mapNewRelToMapOldToNewOutputPos.get(newInputRel);

        if (mapOldToNewOutputPos != null) {
            newLocalOrdinal = mapOldToNewOutputPos.get(oldLocalOrdinal);
        }

        newOrdinal += newLocalOrdinal;

        return new RexInputRef(
            newOrdinal,
            newInputRel.getRowType().getFields()[newLocalOrdinal].getType());
    }

    /**
     * Pull projRel above the joinRel from its RHS input. Enforce nullability
     * for join output.
     *
     * @param joinRel Join
     * @param projRel the orginal projRel as the RHS input of the join.
     * @param nullIndicatorPos Position of null indicator
     *
     * @return the subtree with the new ProjectRel at the root
     */
    private RelNode projectJoinOutputWithNullability(
        JoinRel joinRel,
        ProjectRel projRel,
        int nullIndicatorPos)
    {
        RelDataTypeFactory typeFactory = joinRel.getCluster().getTypeFactory();
        RelNode leftInputRel = joinRel.getLeft();
        JoinRelType joinType = joinRel.getJoinType();

        RexInputRef nullIndicator =
            new RexInputRef(
                nullIndicatorPos,
                typeFactory.createTypeWithNullability(
                    (joinRel.getRowType().getFields()[nullIndicatorPos])
                    .getType(),
                    true));

        // now create the new project
        List<String> newFieldNames = new ArrayList<String>();
        List<RexNode> newProjExprs = new ArrayList<RexNode>();

        // project everything from the LHS and then those from the original
        // projRel
        RelDataTypeField [] leftInputFields =
            leftInputRel.getRowType().getFields();

        for (int i = 0; i < leftInputFields.length; i++) {
            RelDataType newType = leftInputFields[i].getType();
            newProjExprs.add(new RexInputRef(i, newType));
            newFieldNames.add(leftInputFields[i].getName());
        }

        // Marked where the projected expr is coming from so that the types will
        // become nullable for the original projections which are now coming out
        // of the nullable side of the OJ.
        boolean projectPulledAboveLeftCorrelator =
            joinType.generatesNullsOnRight();

        RexNode [] projExprs = projRel.getProjectExps();
        for (int i = 0; i < projExprs.length; i++) {
            RexNode projExpr = projExprs[i];
            RexNode newProjExpr =
                removeCorrelationExpr(
                    projExpr,
                    projectPulledAboveLeftCorrelator,
                    nullIndicator);

            newProjExprs.add(newProjExpr);
            newFieldNames.add((projRel.getRowType().getFields()[i]).getName());
        }

        RelNode newProjRel =
            CalcRel.createProject(joinRel, newProjExprs, newFieldNames);

        return newProjRel;
    }

    /**
     * Pulls projRel above the joinRel from its RHS input. Enforces nullability
     * for join output.
     *
     * @param corRel Correlator
     * @param projRel the orginal ProjectRel as the RHS input of the join
     * @param isCount Positions which are calls to the <code>COUNT</code>
     * aggregation function
     *
     * @return the subtree with the new ProjectRel at the root
     */
    private RelNode aggregateCorrelatorOutput(
        CorrelatorRel corRel,
        ProjectRel projRel,
        Set<Integer> isCount)
    {
        RelNode leftInputRel = corRel.getLeft();
        JoinRelType joinType = corRel.getJoinType();

        // now create the new project
        List<String> newFieldNames = new ArrayList<String>();
        List<RexNode> newProjExprs = new ArrayList<RexNode>();

        // project everything from the LHS and then those from the original
        // projRel
        RelDataTypeField [] leftInputFields =
            leftInputRel.getRowType().getFields();

        for (int i = 0; i < leftInputFields.length; i++) {
            RelDataType newType = leftInputFields[i].getType();
            newProjExprs.add(new RexInputRef(i, newType));
            newFieldNames.add(leftInputFields[i].getName());
        }

        // Marked where the projected expr is coming from so that the types will
        // become nullable for the original projections which are now coming out
        // of the nullable side of the OJ.
        boolean projectPulledAboveLeftCorrelator =
            joinType.generatesNullsOnRight();

        RexNode [] projExprs = projRel.getProjectExps();
        for (int i = 0; i < projExprs.length; i++) {
            RexNode projExpr = projExprs[i];
            RexNode newProjExpr =
                removeCorrelationExpr(
                    projExpr,
                    projectPulledAboveLeftCorrelator,
                    isCount);

            newProjExprs.add(newProjExpr);
            newFieldNames.add((projRel.getRowType().getFields()[i]).getName());
        }

        RelNode newProjRel =
            CalcRel.createProject(corRel, newProjExprs, newFieldNames);

        return newProjRel;
    }

    /**
     * Checks whether the correlations in projRel and filterRel are related to
     * the correlated variables provided by corRel.
     *
     * @param corRel Correlator
     * @param projRel the orginal ProjectRel as the RHS input of the join
     * @param filterRel Filter
     * @param correlatedJoinKeys Correlated join keys
     *
     * @return true if filter and proj only references corVar provided by corRel
     */
    private boolean checkCorVars(
        CorrelatorRel corRel,
        ProjectRel projRel,
        FilterRel filterRel,
        List<RexFieldAccess> correlatedJoinKeys)
    {
        if (filterRel != null) {
            assert (correlatedJoinKeys != null);

            // check that all correlated refs in the filter condition are
            // used in the join(as field access).
            HashSet<CorrelatorRel.Correlation> corVarInFilter =
                new HashSet<CorrelatorRel.Correlation>();
            corVarInFilter.addAll(mapRefRelToCorVar.get(filterRel));

            for (RexFieldAccess correlatedJoinKey : correlatedJoinKeys) {
                corVarInFilter.remove(
                    mapFieldAccessToCorVar.get(correlatedJoinKey));
            }

            if (!corVarInFilter.isEmpty()) {
                return false;
            }

            // Check that the correlated variables referenced in these
            // comparisons do come from the correlatorRel.
            corVarInFilter.addAll(mapRefRelToCorVar.get(filterRel));

            for (CorrelatorRel.Correlation corVar : corVarInFilter) {
                if (mapCorVarToCorRel.get(corVar) != corRel) {
                    return false;
                }
            }
        }

        // if projRel has any correlated reference, make sure they are also
        // provided by the current corRel. They will be projected out of the LHS
        // of the corRel.
        if ((projRel != null) && mapRefRelToCorVar.containsKey(projRel)) {
            SortedSet<CorrelatorRel.Correlation> corVarInProj =
                mapRefRelToCorVar.get(projRel);
            for (CorrelatorRel.Correlation corVar : corVarInProj) {
                if (mapCorVarToCorRel.get(corVar) != corRel) {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Remove correlated variables from the tree at root corRel
     *
     * @param corRel Correlator
     */
    private void removeCorVarFromTree(
        CorrelatorRel corRel)
    {
        HashSet<CorrelatorRel.Correlation> corVarSet =
            new HashSet<CorrelatorRel.Correlation>();
        corVarSet.addAll(mapCorVarToCorRel.keySet());

        for (CorrelatorRel.Correlation corVar : corVarSet) {
            if (mapCorVarToCorRel.get(corVar) == corRel) {
                mapCorVarToCorRel.remove(corVar);
            }
        }
    }

    /**
     * Project all childRel output fields plus the additional expressions.
     *
     * @param childRel Child relational expression
     * @param additionalExprs Additional expressions
     * @param additionalExprNames Names of additional expressions
     *
     * @return the new ProjectRel
     */
    private RelNode createProjectWithAdditionalExprs(
        RelNode childRel,
        RexNode [] additionalExprs,
        String [] additionalExprNames)
    {
        boolean useNewNames = false;

        if (additionalExprNames != null) {
            useNewNames = true;
        }

        RelDataType childFieldType = childRel.getRowType();
        int childFieldCount = childFieldType.getFieldCount();

        RexNode [] exprs =
            new RexNode[childFieldCount + additionalExprs.length];

        int i = 0;
        for (; i < childFieldCount; i++) {
            exprs[i] =
                rexBuilder.makeInputRef(
                    childFieldType.getFields()[i].getType(),
                    i);
        }

        for (; i < exprs.length; i++) {
            exprs[i] = additionalExprs[i - childFieldCount];
        }

        String [] exprNames = null;

        if (useNewNames) {
            exprNames = new String[childFieldCount + additionalExprs.length];
            i = 0;
            for (; i < childFieldCount; i++) {
                exprNames[i] = childFieldType.getFields()[i].getName();
            }

            for (; i < exprs.length; i++) {
                exprNames[i] = additionalExprNames[i - childFieldCount];
            }
        }
        return CalcRel.createProject(childRel, exprs, exprNames);
    }

    //~ Inner Classes ----------------------------------------------------------

    private class DecorrelateRelVisitor
        extends RelVisitor
    {
        private final ReflectiveVisitDispatcher<RelDecorrelator, RelNode>
            dispatcher =
                ReflectUtil.createDispatcher(
                    RelDecorrelator.class,
                    RelNode.class);

        // implement RelVisitor
        public void visit(RelNode p, int ordinal, RelNode parent)
        {
            // rewrite children first  (from left to right)
            super.visit(p, ordinal, parent);

            currentRel = p;

            final String visitMethodName = "decorrelateRel";
            boolean found =
                dispatcher.invokeVisitor(
                    RelDecorrelator.this,
                    currentRel,
                    visitMethodName);
            currentRel = null;

            if (!found) {
                decorrelateRelGeneric(p);
            }
            // else no rewrite will occur. This will terminate the bottom-up
            // rewrite. If root node of a RelNode tree is not rewritten, the
            // original tree will be returned. See decorrelate() method.
        }
    }

    private class DecorrelateRexShuttle
        extends RexShuttle
    {
        // override RexShuttle
        public RexNode visitFieldAccess(RexFieldAccess fieldAccess)
        {
            int newInputRelOutputOffset = 0;
            RelNode oldInputRel;
            RelNode newInputRel;
            Integer newInputPos;

            List<RelNode> inputs = currentRel.getInputs();
            for (int i = 0; i < inputs.size(); i++) {
                oldInputRel = inputs.get(i);
                newInputRel = mapOldToNewRel.get(oldInputRel);

                if ((newInputRel != null)
                    && mapNewRelToMapCorVarToOutputPos.containsKey(
                        newInputRel))
                {
                    SortedMap<CorrelatorRel.Correlation, Integer>
                        childMapCorVarToOutputPos =
                        mapNewRelToMapCorVarToOutputPos.get(newInputRel);

                    if (childMapCorVarToOutputPos != null) {
                        // try to find in this input rel the position of cor var
                        CorrelatorRel.Correlation corVar =
                            mapFieldAccessToCorVar.get(fieldAccess);

                        if (corVar != null) {
                            newInputPos = childMapCorVarToOutputPos.get(corVar);
                            if (newInputPos != null) {
                                // this input rel does produce the cor var
                                // referenced
                                newInputPos += newInputRelOutputOffset;

                                // fieldAccess is assumed to have the correct
                                // type info.
                                RexInputRef newInput =
                                    new RexInputRef(
                                        newInputPos,
                                        fieldAccess.getType());
                                return newInput;
                            }
                        }
                    }

                    // this input rel does not produce the cor var needed
                    newInputRelOutputOffset +=
                        newInputRel.getRowType().getFieldCount();
                } else {
                    // this input rel is not rewritten
                    newInputRelOutputOffset +=
                        oldInputRel.getRowType().getFieldCount();
                }
            }
            return fieldAccess;
        }

        // override RexShuttle
        public RexNode visitInputRef(RexInputRef inputRef)
        {
            RexInputRef newInputRef = getNewForOldInputRef(inputRef);
            return newInputRef;
        }
    }

    private class RemoveCorrelationRexShuttle
        extends RexShuttle
    {
        RexBuilder rexBuilder;
        RelDataTypeFactory typeFactory;
        boolean projectPulledAboveLeftCorrelator;
        RexInputRef nullIndicator;
        Set<Integer> isCount;

        public RemoveCorrelationRexShuttle(
            RexBuilder rexBuilder,
            boolean projectPulledAboveLeftCorrelator)
        {
            this(
                rexBuilder,
                projectPulledAboveLeftCorrelator,
                null, null);
        }

        public RemoveCorrelationRexShuttle(
            RexBuilder rexBuilder,
            boolean projectPulledAboveLeftCorrelator,
            RexInputRef nullIndicator)
        {
            this(
                rexBuilder,
                projectPulledAboveLeftCorrelator,
                nullIndicator,
                null);
        }

        public RemoveCorrelationRexShuttle(
            RexBuilder rexBuilder,
            boolean projectPulledAboveLeftCorrelator,
            Set<Integer> isCount)
        {
            this(
                rexBuilder,
                projectPulledAboveLeftCorrelator,
                null, isCount);
        }

        public RemoveCorrelationRexShuttle(
            RexBuilder rexBuilder,
            boolean projectPulledAboveLeftCorrelator,
            RexInputRef nullIndicator,
            Set<Integer> isCount)
        {
            this.projectPulledAboveLeftCorrelator =
                projectPulledAboveLeftCorrelator;
            this.nullIndicator = nullIndicator;
            this.isCount = isCount;
            this.rexBuilder = rexBuilder;
            this.typeFactory = rexBuilder.getTypeFactory();
        }

        private RexNode createCaseExpression(
            RexInputRef nullInputRef,
            RexLiteral lit,
            RexNode rexNode)
        {
            RexNode [] caseOperands = new RexNode[3];

            // Construct a CASE expression to handle the null indicator.
            //
            // This also covers the case where a left correlated subquery
            // projects fields from outer relation. Since LOJ cannot produce
            // nulls on the LHS, the projection now need to make a nullable LHS
            // reference using a nullability indicator. If this this indicator
            // is null, it means the subquery does not produce any value. As a
            // result, any RHS ref by this usbquery needs to produce null value.

            // WHEN indicator IS NULL
            caseOperands[0] =
                rexBuilder.makeCall(
                    SqlStdOperatorTable.isNullOperator,
                    new RexInputRef(
                        nullInputRef.getIndex(),
                        typeFactory.createTypeWithNullability(
                            nullInputRef.getType(),
                            true)));

            // THEN CAST(NULL AS newInputTypeNullable)
            caseOperands[1] =
                rexBuilder.makeCast(
                    typeFactory.createTypeWithNullability(
                        rexNode.getType(),
                        true),
                    lit);

            // ELSE cast (newInput AS newInputTypeNullable) END
            caseOperands[2] =
                rexBuilder.makeCast(
                    typeFactory.createTypeWithNullability(
                        rexNode.getType(),
                        true),
                    rexNode);

            return rexBuilder.makeCall(
                SqlStdOperatorTable.caseOperator,
                caseOperands);
        }

        // override RexShuttle
        public RexNode visitFieldAccess(RexFieldAccess fieldAccess)
        {
            if (mapFieldAccessToCorVar.containsKey(fieldAccess)) {
                // if it is a corVar, change it to be input ref.
                CorrelatorRel.Correlation corVar =
                    mapFieldAccessToCorVar.get(fieldAccess);

                // corVar offset shuold point to the leftInput of currentRel,
                // which is the CorrelatorRel.
                RexNode newRexNode =
                    new RexInputRef(corVar.getOffset(), fieldAccess.getType());

                if (projectPulledAboveLeftCorrelator
                    && (nullIndicator != null))
                {
                    // need to enforce nullability by applying an additional
                    // cast operator over the transformed expression.
                    newRexNode =
                        createCaseExpression(
                            nullIndicator,
                            rexBuilder.constantNull(),
                            newRexNode);
                }
                return newRexNode;
            }
            return fieldAccess;
        }

        // override RexShuttle
        public RexNode visitInputRef(RexInputRef inputRef)
        {
            if ((currentRel != null) && (currentRel instanceof CorrelatorRel)) {
                // if this rel references corVar
                // and now it needs to be rewritten
                // it must have been pulled above the CorrelatorRel
                // replace the input ref to account for the LHS of the
                // CorrelatorRel
                int leftInputFieldCount =
                    ((CorrelatorRel) currentRel).getLeft().getRowType()
                    .getFieldCount();
                RelDataType newType = inputRef.getType();

                if (projectPulledAboveLeftCorrelator) {
                    newType =
                        typeFactory.createTypeWithNullability(newType, true);
                }

                int pos = inputRef.getIndex();
                RexInputRef newInputRef =
                    new RexInputRef(leftInputFieldCount + pos, newType);

                if ((isCount != null) && isCount.contains(pos)) {
                    return createCaseExpression(
                        newInputRef,
                        rexBuilder.makeExactLiteral(BigDecimal.ZERO),
                        newInputRef);
                } else {
                    return newInputRef;
                }
            }
            return inputRef;
        }

        // override RexLiteral
        public RexNode visitLiteral(RexLiteral literal)
        {
            // Use nullIndicator to decide whether to project null.
            // Do nothing if the literal is null.
            if (!RexUtil.isNull(literal)
                && projectPulledAboveLeftCorrelator
                && (nullIndicator != null))
            {
                return createCaseExpression(
                    nullIndicator,
                    rexBuilder.constantNull(),
                    literal);
            }
            return literal;
        }

        public RexNode visitCall(final RexCall call)
        {
            RexNode newCall;

            boolean [] update = { false };
            RexNode [] clonedOperands = visitArray(call.operands, update);
            if (update[0]) {
                SqlOperator operator = call.getOperator();

                boolean isSpecialCast = false;
                if (operator instanceof SqlFunction) {
                    SqlFunction function = (SqlFunction) operator;
                    if (function.getKind() == SqlKind.CAST) {
                        if (call.operands.length < 2) {
                            isSpecialCast = true;
                        }
                    }
                }

                if (!isSpecialCast) {
                    // TODO: ideally this only needs to be called if the result
                    // type will also change. However, since that requires
                    // suport from type inference rules to tell whether a rule
                    // decides return type based on input types, for now all
                    // operators will be recreated with new type if any operand
                    // changed, unless the operator has "built-in" type.
                    newCall = rexBuilder.makeCall(operator, clonedOperands);
                } else {
                    // Use the current return type when creating a new call, for
                    // operators with return type built into the operator
                    // definition, and with no type inference rules, such as
                    // cast function with less than 2 operands.

                    // TODO: Comments in RexShuttle.visitCall() mention other
                    // types in this catagory. Need to resolve those together
                    // and preferrably in the base class RexShuttle.
                    newCall =
                        rexBuilder.makeCall(
                            call.getType(),
                            operator,
                            clonedOperands);
                }
            } else {
                newCall = call;
            }

            if (projectPulledAboveLeftCorrelator && (nullIndicator != null)) {
                return createCaseExpression(
                    nullIndicator,
                    rexBuilder.constantNull(),
                    newCall);
            }
            return newCall;
        }
    }

    /**
     * Rule to remove single_value rel. For cases like
     *
     * <blockquote>AggRel single_value proj/filter/agg/ join on unique LHS key
     * AggRel single group</blockquote>
     */
    private final class RemoveSingleAggregateRule
        extends RelOptRule
    {
        public RemoveSingleAggregateRule()
        {
            super(
                some(
                    AggregateRel.class,
                    some(
                        ProjectRel.class,
                        any(AggregateRel.class))));
        }

        public void onMatch(RelOptRuleCall call)
        {
            AggregateRel singleAggRel = call.rel(0);
            ProjectRel projRel = call.rel(1);
            AggregateRel aggRel = call.rel(2);

            // check singleAggRel is single_value agg
            if ((!singleAggRel.getGroupSet().isEmpty())
                || (singleAggRel.getAggCallList().size() != 1)
                || !(singleAggRel.getAggCallList().get(0).getAggregation()
                    instanceof SqlSingleValueAggFunction))
            {
                return;
            }

            // check projRel only projects one expression
            // check this project only projects one expression, i.e. scalar
            // subqueries.
            RexNode [] projExprs = projRel.getProjectExps();
            if (projExprs.length != 1) {
                return;
            }

            // check the input to projRel is an aggregate on the entire input
            if (!aggRel.getGroupSet().isEmpty()) {
                return;
            }

            // singleAggRel produces a nullable type, so create the new
            // projection that casts proj expr to a nullable type.
            RexNode [] newProjExprs = new RexNode[1];

            newProjExprs[0] =
                rexBuilder.makeCast(
                    projRel.getCluster().getTypeFactory()
                           .createTypeWithNullability(
                               projExprs[0].getType(),
                               true),
                    projExprs[0]);

            RelNode newProjRel =
                CalcRel.createProject(aggRel, newProjExprs, null);
            call.transformTo(newProjRel);
        }
    }

    private final class RemoveCorrelationForScalarProjectRule
        extends RelOptRule
    {
        public RemoveCorrelationForScalarProjectRule()
        {
            super(
                some(
                    CorrelatorRel.class,
                    any(RelNode.class),
                    some(
                        AggregateRel.class,
                        some(
                            ProjectRel.class,
                            any(RelNode.class)))));
        }

      public void onMatch(RelOptRuleCall call)
        {
            CorrelatorRel corRel = call.rel(0);
            RelNode leftInputRel = call.rel(1);
            AggregateRel aggRel = call.rel(2);
            ProjectRel projRel = call.rel(3);
            RelNode rightInputRel = call.rel(4);
            RelOptCluster cluster = corRel.getCluster();

            currentRel = corRel;

            // Check for this pattern.
            // The pattern matching could be simplified if rules can be applied
            // during decorrelation.
            //
            // CorrelateRel(left correlation, condition = true)
            //   LeftInputRel
            //   AggregateRel (groupby (0) single_value())
            //     ProjectRel-A (may reference coVar)
            //       RightInputRel
            JoinRelType joinType = corRel.getJoinType();
            RexNode joinCond = corRel.getCondition();
            if ((joinType != JoinRelType.LEFT)
                || (joinCond != rexBuilder.makeLiteral(true)))
            {
                return;
            }

            // check that the agg is of the following type:
            // doing a single_value() on the entire input
            if ((!aggRel.getGroupSet().isEmpty())
                || (aggRel.getAggCallList().size() != 1)
                || !(aggRel.getAggCallList().get(0).getAggregation()
                    instanceof SqlSingleValueAggFunction))
            {
                return;
            }

            // check this project only projects one expression, i.e. scalar
            // subqueries.
            if (projRel.getProjectExps().length != 1) {
                return;
            }

            int nullIndicatorPos;

            if ((rightInputRel instanceof FilterRel)
                && mapRefRelToCorVar.containsKey(rightInputRel))
            {
                // rightInputRel has this shape:
                //
                //       FilterRel (references corvar)
                //         FilterInputRel

                // If rightInputRel is a filter and contains correlated
                // reference, make sure the correlated keys in the filter
                // condition forms a unique key of the RHS.

                FilterRel filterRel = (FilterRel) rightInputRel;
                rightInputRel = filterRel.getChild();

                assert (rightInputRel instanceof HepRelVertex);
                rightInputRel = ((HepRelVertex) rightInputRel).getCurrentRel();

                // check filter input contains no correlation
                if (RelOptUtil.getVariablesUsed(rightInputRel).size() > 0) {
                    return;
                }

                // extract the correlation out of the filterRel

                // First breaking up the filter conditions into equality
                // comparisons between rightJoinKeys(from the original
                // filterInputRel) and correlatedJoinKeys. correlatedJoinKeys
                // can be expressions, while rightJoinKeys need to be input
                // refs. These comparisons are AND'ed together.
                List<RexNode> tmpRightJoinKeys = new ArrayList<RexNode>();
                List<RexNode> correlatedJoinKeys = new ArrayList<RexNode>();
                RelOptUtil.splitCorrelatedFilterCondition(
                    filterRel,
                    tmpRightJoinKeys,
                    correlatedJoinKeys,
                    false);

                // check that the columns referenced in these comparisons form
                // an unique key of the filterInputRel
                List<RexInputRef> rightJoinKeys = new ArrayList<RexInputRef>();
                for (int i = 0; i < tmpRightJoinKeys.size(); i++) {
                    assert (tmpRightJoinKeys.get(i) instanceof RexInputRef);
                    rightJoinKeys.add((RexInputRef) tmpRightJoinKeys.get(i));
                }

                // check that the columns referenced in rightJoinKeys form an
                // unique key of the filterInputRel
                if (rightJoinKeys.isEmpty()) {
                    return;
                }

                // The join filters out the nulls.  So, it's ok if there are
                // nulls in the join keys.
                if (!RelMdUtil.areColumnsDefinitelyUniqueWhenNullsFiltered(
                        rightInputRel,
                        rightJoinKeys))
                {
                    sqlToRelTracer.fine(
                        rightJoinKeys.toString()
                        + "are not unique keys for "
                        + rightInputRel.toString());
                    return;
                }

                RexUtil.FieldAccessFinder visitor =
                    new RexUtil.FieldAccessFinder();
                RexUtil.apply(visitor, correlatedJoinKeys, null);
                List<RexFieldAccess> correlatedKeyList =
                    visitor.getFieldAccessList();

                if (!checkCorVars(
                        corRel,
                        projRel,
                        filterRel,
                        correlatedKeyList))
                {
                    return;
                }

                // Change the plan to this structure.
                // Note that the aggregateRel is removed.
                //
                // ProjectRel-A' (replace corvar to input ref from the JoinRel)
                //   JoinRel (replace corvar to input ref from LeftInputRel)
                //     LeftInputRel
                //     RightInputRel(oreviously FilterInputRel)

                // Change the filter condition into a join condition
                joinCond =
                    removeCorrelationExpr(filterRel.getCondition(), false);

                nullIndicatorPos =
                    leftInputRel.getRowType().getFieldCount()
                    + rightJoinKeys.get(0).getIndex();
            } else if (mapRefRelToCorVar.containsKey(projRel)) {
                // check filter input contains no correlation
                if (RelOptUtil.getVariablesUsed(rightInputRel).size() > 0) {
                    return;
                }

                if (!checkCorVars(corRel, projRel, null, null)) {
                    return;
                }

                // Change the plan to this structure.
                //
                // ProjectRel-A' (replace corvar to input ref from JoinRel)
                //   JoinRel (left, condition = true)
                //     LeftInputRel
                //     AggregateRel(groupby(0), single_value(0), s_v(1)....)
                //       ProjectRel-B (everything from input plus literal true)
                //         ProjInputRel

                // make the new projRel to provide a null indicator
                rightInputRel =
                    createProjectWithAdditionalExprs(
                        rightInputRel,
                        new RexNode[] { rexBuilder.makeLiteral(true) },
                        new String[] { "nullIndicator" });

                // make the new aggRel
                rightInputRel =
                    RelOptUtil.createSingleValueAggRel(cluster, rightInputRel);

                // The last field:
                //     single_value(true)
                // is the nullIndicator
                nullIndicatorPos =
                    leftInputRel.getRowType().getFieldCount()
                    + rightInputRel.getRowType().getFieldCount() - 1;
            } else {
                return;
            }

            // make the new join rel
            final Set<String> variablesStopped = Collections.emptySet();
            JoinRel joinRel =
                new JoinRel(
                    corRel.getCluster(),
                    leftInputRel,
                    rightInputRel,
                    joinCond,
                    joinType,
                    variablesStopped);

            RelNode newProjRel =
                projectJoinOutputWithNullability(
                    joinRel,
                    projRel,
                    nullIndicatorPos);

            call.transformTo(newProjRel);

            removeCorVarFromTree(corRel);
        }
    }

    private final class RemoveCorrelationForScalarAggregateRule
        extends RelOptRule
    {
        public RemoveCorrelationForScalarAggregateRule()
        {
            super(
                some(
                    CorrelatorRel.class,
                    any(RelNode.class),
                    some(
                        ProjectRel.class,
                        some(
                            AggregateRel.class,
                            some(
                                ProjectRel.class,
                                any(RelNode.class))))));
        }

        public void onMatch(RelOptRuleCall call)
        {
            CorrelatorRel corRel = call.rel(0);
            RelNode leftInputRel = call.rel(1);
            ProjectRel aggOutputProjRel = call.rel(2);
            AggregateRel aggRel = call.rel(3);
            ProjectRel aggInputProjRel = call.rel(4);
            RelNode rightInputRel = call.rel(5);
            RelOptCluster cluster = corRel.getCluster();

            currentRel = corRel;

            // check for this pattern
            // The pattern matching could be simplified if rules can be applied
            // during decorrelation,
            //
            // CorrelateRel(left correlation, condition = true)
            //   LeftInputRel
            //   ProjectRel-A (a RexNode)
            //     AggregateRel (groupby (0), agg0(), agg1()...)
            //       ProjectRel-B (references coVar)
            //         rightInputRel

            // check aggOutputProj projects only one expression
            RexNode [] aggOutputProjExprs = aggOutputProjRel.getProjectExps();
            if (aggOutputProjExprs.length != 1) {
                return;
            }

            JoinRelType joinType = corRel.getJoinType();
            RexNode joinCond = corRel.getCondition();
            if ((joinType != JoinRelType.LEFT)
                || (joinCond != rexBuilder.makeLiteral(true)))
            {
                return;
            }

            // check that the agg is on the entire input
            if (!aggRel.getGroupSet().isEmpty()) {
                return;
            }

            RexNode [] aggInputProjExprs = aggInputProjRel.getProjectExps();

            List<AggregateCall> aggCalls = aggRel.getAggCallList();
            Set<Integer> isCountStar = new HashSet<Integer>();

            // mark if agg produces count(*) which needs to reference the
            // nullIndicator after the transformation.
            int k = -1;
            for (AggregateCall aggCall : aggCalls) {
                ++k;
                if ((aggCall.getAggregation() instanceof SqlCountAggFunction)
                    && (aggCall.getArgList().size() == 0))
                {
                    isCountStar.add(k);
                }
            }

            if ((rightInputRel instanceof FilterRel)
                && mapRefRelToCorVar.containsKey(rightInputRel))
            {
                // rightInputRel has this shape:
                //
                //       FilterRel (references corvar)
                //         FilterInputRel
                FilterRel filterRel = (FilterRel) rightInputRel;
                rightInputRel = filterRel.getChild();

                assert (rightInputRel instanceof HepRelVertex);
                rightInputRel = ((HepRelVertex) rightInputRel).getCurrentRel();

                // check filter input contains no correlation
                if (RelOptUtil.getVariablesUsed(rightInputRel).size() > 0) {
                    return;
                }

                // check filter condition type First extract the correlation out
                // of the filterRel

                // First breaking up the filter conditions into equality
                // comparisons between rightJoinKeys(from the original
                // filterInputRel) and correlatedJoinKeys. correlatedJoinKeys
                // can only be RexFieldAccess, while rightJoinKeys can be
                // expressions. These comparisons are AND'ed together.
                List<RexNode> rightJoinKeys = new ArrayList<RexNode>();
                List<RexNode> tmpCorrelatedJoinKeys = new ArrayList<RexNode>();
                RelOptUtil.splitCorrelatedFilterCondition(
                    filterRel,
                    rightJoinKeys,
                    tmpCorrelatedJoinKeys,
                    true);

                // make sure the correlated reference forms a unique key check
                // that the columns referenced in these comparisons form an
                // unique key of the leftInputRel
                List<RexFieldAccess> correlatedJoinKeys =
                    new ArrayList<RexFieldAccess>();
                List<RexInputRef> correlatedInputRefJoinKeys =
                    new ArrayList<RexInputRef>();
                for (RexNode joinKey : tmpCorrelatedJoinKeys) {
                    assert joinKey instanceof RexFieldAccess;
                    correlatedJoinKeys.add((RexFieldAccess) joinKey);
                    RexNode correlatedInputRef =
                        removeCorrelationExpr(
                            joinKey,
                            false);
                    assert (correlatedInputRef instanceof RexInputRef);
                    correlatedInputRefJoinKeys.add(
                        (RexInputRef) correlatedInputRef);
                }

                // check that the columns referenced in rightJoinKeys form an
                // unique key of the filterInputRel
                if (correlatedInputRefJoinKeys.isEmpty()) {
                    return;
                }

                // The join filters out the nulls.  So, it's ok if there are
                // nulls in the join keys.
                if (!RelMdUtil.areColumnsDefinitelyUniqueWhenNullsFiltered(
                        leftInputRel,
                        correlatedInputRefJoinKeys))
                {
                    sqlToRelTracer.fine(
                        correlatedJoinKeys.toString()
                        + "are not unique keys for "
                        + leftInputRel.toString());
                    return;
                }

                // check cor var references are valid
                if (!checkCorVars(
                        corRel,
                        aggInputProjRel,
                        filterRel,
                        correlatedJoinKeys))
                {
                    return;
                }

                // Rewrite the above plan:
                //
                // CorrelateRel(left correlation, condition = true)
                //   LeftInputRel
                //   ProjectRel-A (a RexNode)
                //     AggregateRel (groupby(0), agg0(),agg1()...)
                //       ProjectRel-B (may reference coVar)
                //         FilterRel (references corVar)
                //           RightInputRel (no correlated reference)
                //

                // to this plan:
                //
                // ProjectRel-A' (all gby keys + rewritten nullable ProjExpr)
                //   AggregateRel (groupby(all left input refs)
                //                 agg0(rewritten expression),
                //                 agg1()...)
                //     ProjectRel-B' (rewriten original projected exprs)
                //       JoinRel(replace corvar w/ input ref from LeftInputRel)
                //         LeftInputRel
                //         RightInputRel
                //

                // In the case where agg is count(*) or count($corVar), it is
                // changed to count(nullIndicator).
                // Note:  any non-nullable field from the RHS can be used as
                // the indicator however a "true" field is added to the
                // projection list from the RHS for simplicity to avoid
                // searching for non-null fields.
                //
                // ProjectRel-A' (all gby keys + rewritten nullable ProjExpr)
                //   AggregateRel (groupby(all left input refs),
                //                 count(nullIndicator), other aggs...)
                //     ProjectRel-B' (all left input refs plus
                //                    the rewritten original projected exprs)
                //       JoinRel(replace corvar to input ref from LeftInputRel)
                //         LeftInputRel
                //         ProjectRel (everything from RightInputRel plus
                //                     the nullIndicator "true")
                //           RightInputRel
                //

                // first change the filter condition into a join condition
                joinCond =
                    removeCorrelationExpr(filterRel.getCondition(), false);
            } else if (mapRefRelToCorVar.containsKey(aggInputProjRel)) {
                // check rightInputRel contains no correlation
                if (RelOptUtil.getVariablesUsed(rightInputRel).size() > 0) {
                    return;
                }

                // check cor var references are valid
                if (!checkCorVars(corRel, aggInputProjRel, null, null)) {
                    return;
                }

                int nFields = leftInputRel.getRowType().getFieldCount();
                BitSet allCols = Util.bitSetBetween(0, nFields);

                // leftInputRel contains unique keys
                // i.e. each row is distinct and can group by on all the left
                // fields
                if (!RelMdUtil.areColumnsDefinitelyUnique(
                        leftInputRel,
                        allCols))
                {
                    sqlToRelTracer.fine(
                        "There are no unique keys for "
                        + leftInputRel.toString());
                    return;
                }
                //
                // Rewrite the above plan:
                //
                // CorrelateRel(left correlation, condition = true)
                //   LeftInputRel
                //   ProjectRel-A (a RexNode)
                //     AggregateRel (groupby(0), agg0(), agg1()...)
                //       ProjectRel-B (references coVar)
                //         RightInputRel (no correlated reference)
                //

                // to this plan:
                //
                // ProjectRel-A' (all gby keys + rewritten nullable ProjExpr)
                //   AggregateRel (groupby(all left input refs)
                //                 agg0(rewritten expression),
                //                 agg1()...)
                //     ProjectRel-B' (rewriten original projected exprs)
                //       JoinRel (LOJ cond = true)
                //         LeftInputRel
                //         RightInputRel
                //

                // In the case where agg is count($corVar), it is changed to
                // count(nullIndicator).
                // Note:  any non-nullable field from the RHS can be used as
                // the indicator however a "true" field is added to the
                // projection list from the RHS for simplicity to avoid
                // searching for non-null fields.
                //
                // ProjectRel-A' (all gby keys + rewritten nullable ProjExpr)
                //   AggregateRel (groupby(all left input refs),
                //                 count(nullIndicator), other aggs...)
                //     ProjectRel-B' (all left input refs plus
                //                    the rewritten original projected exprs)
                //       JoinRel(replace corvar to input ref from LeftInputRel)
                //         LeftInputRel
                //         ProjectRel (everything from RightInputRel plus
                //                     the nullIndicator "true")
                //           RightInputRel
            } else {
                return;
            }

            RelDataType leftInputFieldType = leftInputRel.getRowType();
            int leftInputFieldCount = leftInputFieldType.getFieldCount();
            int joinOutputProjExprCount =
                leftInputFieldCount + aggInputProjExprs.length + 1;

            rightInputRel =
                createProjectWithAdditionalExprs(
                    rightInputRel,
                    new RexNode[] { rexBuilder.makeLiteral(true) },
                    new String[] { "nullIndicator" });

            final Set<String> variablesStopped = Collections.emptySet();
            JoinRel joinRel =
                new JoinRel(
                    cluster,
                    leftInputRel,
                    rightInputRel,
                    joinCond,
                    joinType,
                    variablesStopped);

            // To the consumer of joinOutputProjRel, nullIndicator is located
            // at the end
            int nullIndicatorPos = joinRel.getRowType().getFieldCount() - 1;

            RexInputRef nullIndicator =
                new RexInputRef(
                    nullIndicatorPos,
                    cluster.getTypeFactory().createTypeWithNullability(
                        (joinRel.getRowType().getFields()[nullIndicatorPos])
                        .getType(),
                        true));

            // first project all the groupby keys plus the transformed agg input
            RexNode [] joinOutputProjExprs =
                new RexNode[joinOutputProjExprCount];

            // LOJ Join preserves LHS types
            for (int i = 0; i < leftInputFieldCount; i++) {
                joinOutputProjExprs[i] =
                    rexBuilder.makeInputRef(
                        leftInputFieldType.getFields()[i].getType(),
                        i);
            }

            for (int i = 0; i < aggInputProjExprs.length; i++) {
                joinOutputProjExprs[i + leftInputFieldCount] =
                    removeCorrelationExpr(
                        aggInputProjExprs[i],
                        joinType.generatesNullsOnRight(),
                        nullIndicator);
            }

            joinOutputProjExprs[joinOutputProjExprCount - 1] =
                rexBuilder.makeInputRef(
                    joinRel.getRowType().getFields()[nullIndicatorPos]
                    .getType(),
                    nullIndicatorPos);

            RelNode joinOutputProjRel =
                CalcRel.createProject(
                    joinRel,
                    joinOutputProjExprs,
                    null);

            // nullIndicator is now at a different location in the output of
            // the join
            nullIndicatorPos = joinOutputProjExprCount - 1;

            final int groupCount = leftInputFieldCount;

            List<AggregateCall> newAggCalls = new ArrayList<AggregateCall>();
            k = -1;
            for (AggregateCall aggCall : aggCalls) {
                ++k;
                final List<Integer> aggArgs = aggCall.getArgList();
                final List<Integer> newAggArgs;

                if (isCountStar.contains(k)) {
                    // this is a count(*), transform it to count(nullIndicator)
                    // the null indicator is located at the end
                    newAggArgs = Collections.singletonList(nullIndicatorPos);
                } else {
                    newAggArgs = new ArrayList<Integer>();

                    for (Integer aggArg : aggArgs) {
                        newAggArgs.add(aggArg + groupCount);
                    }
                }

                newAggCalls.add(
                    new AggregateCall(
                        aggCall.getAggregation(),
                        aggCall.isDistinct(),
                        newAggArgs,
                        aggCall.getType(),
                        aggCall.getName()));
            }

            BitSet groupSet =
                Util.bitSetBetween(0, groupCount);
            AggregateRel newAggRel =
                new AggregateRel(
                    cluster,
                    joinOutputProjRel,
                    groupSet,
                    newAggCalls);

            List<RexNode> newAggOutputProjExprList = new ArrayList<RexNode>();
            for (int i : Util.toIter(groupSet)) {
                newAggOutputProjExprList.add(
                    rexBuilder.makeInputRef(
                        newAggRel.getRowType().getFields()[i].getType(),
                        i));
            }

            RexNode newAggOutputProjExpr =
                removeCorrelationExpr(aggOutputProjExprs[0], false);
            newAggOutputProjExprList.add(
                rexBuilder.makeCast(
                    cluster.getTypeFactory().createTypeWithNullability(
                        newAggOutputProjExpr.getType(),
                        true),
                    newAggOutputProjExpr));

            RelNode newAggOutputProjRel =
                CalcRel.createProject(
                    newAggRel,
                    newAggOutputProjExprList,
                    null);

            call.transformTo(newAggOutputProjRel);

            removeCorVarFromTree(corRel);
        }
    }

    // REVIEW jhyde 29-Oct-2007: This rule is non-static, depends on the state
    // of members in RelDecorrelator, and has side-effects in the decorrelator.
    // This breaks the contract of a planner rule, and the rule will not be
    // reusable in other planners.

    // REVIEW jvs 29-Oct-2007:  Shouldn't it also be incorporating
    // the flavor attribute into the description?

    private final class AdjustProjectForCountAggregateRule
        extends RelOptRule
    {
        final boolean flavor;

        public AdjustProjectForCountAggregateRule(boolean flavor)
        {
            super(
                flavor
                ? some(
                    CorrelatorRel.class,
                    any(RelNode.class),
                    some(
                        ProjectRel.class,
                        any(AggregateRel.class)))
                : some(
                    CorrelatorRel.class,
                    any(RelNode.class),
                    any(AggregateRel.class)));
            this.flavor = flavor;
        }

        public void onMatch(RelOptRuleCall call)
        {
            CorrelatorRel corRel = call.rel(0);
            RelNode leftInputRel = call.rel(1);
            ProjectRel aggOutputProjRel;
            AggregateRel aggRel;
            if (flavor) {
                aggOutputProjRel = call.rel(2);
                aggRel = call.rel(3);
            } else {
                aggRel = call.rel(2);

                // Create identity projection
                List<RexNode> exprList = new ArrayList<RexNode>();
                List<String> fieldNameList = new ArrayList<String>();
                for (
                    RelDataTypeField field : aggRel.getRowType().getFieldList())
                {
                    exprList.add(
                        new RexInputRef(exprList.size(), field.getType()));
                    fieldNameList.add(field.getName());
                }
                aggOutputProjRel =
                    new ProjectRel(
                        corRel.getCluster(),
                        aggRel,
                        exprList.toArray(new RexNode[exprList.size()]),
                        fieldNameList.toArray(new String[fieldNameList.size()]),
                        ProjectRel.Flags.Boxed);
            }
            onMatch2(call, corRel, leftInputRel, aggOutputProjRel, aggRel);
        }

        private void onMatch2(
            RelOptRuleCall call,
            CorrelatorRel corRel,
            RelNode leftInputRel,
            ProjectRel aggOutputProjRel,
            AggregateRel aggRel)
        {
            RelOptCluster cluster = corRel.getCluster();

            if (generatedCorRels.contains(corRel)) {
                // This correlator was generated by a previous invocation of
                // this rule. No further work to do.
                return;
            }

            currentRel = corRel;

            // check for this pattern
            // The pattern matching could be simplified if rules can be applied
            // during decorrelation,
            //
            // CorrelateRel(left correlation, condition = true)
            //   LeftInputRel
            //   ProjectRel-A (a RexNode)
            //     AggregateRel (groupby (0), agg0(), agg1()...)

            // check aggOutputProj projects only one expression
            RexNode [] aggOutputProjExprs = aggOutputProjRel.getProjectExps();
            if (aggOutputProjExprs.length != 1) {
                return;
            }

            JoinRelType joinType = corRel.getJoinType();
            RexNode joinCond = corRel.getCondition();
            if ((joinType != JoinRelType.LEFT)
                || (joinCond != rexBuilder.makeLiteral(true)))
            {
                return;
            }

            // check that the agg is on the entire input
            if (!aggRel.getGroupSet().isEmpty()) {
                return;
            }

            List<AggregateCall> aggCalls = aggRel.getAggCallList();
            Set<Integer> isCount = new HashSet<Integer>();

            // remember the count() positions
            int i = -1;
            for (AggregateCall aggCall : aggCalls) {
                ++i;
                if (aggCall.getAggregation() instanceof SqlCountAggFunction) {
                    isCount.add(i);
                }
            }

            // now rewrite the plan to
            //
            // Project-A' (all LHS plus transformed original projections,
            //             replacing references to count() with case statement)
            //   CorrelatorRel(left correlation, condition = true)
            //     LeftInputRel
            //     AggregateRel (groupby (0), agg0(), agg1()...)
            //
            CorrelatorRel newCorRel =
                new CorrelatorRel(
                    cluster,
                    leftInputRel,
                    aggRel,
                    corRel.getCorrelations(),
                    corRel.getJoinType());

            // remember this rel so we don't fire rule on it again
            // REVIEW jhyde 29-Oct-2007: rules should not save state; rule
            // should recognize patterns where it does or does not need to do
            // work
            generatedCorRels.add(newCorRel);

            // need to update the mapCorVarToCorRel Update the output position
            // for the cor vars: only pass on the cor vars that are not used in
            // the join key.
            Set<CorrelatorRel.Correlation> corVars =
                new HashSet<CorrelatorRel.Correlation>();
            corVars.addAll(mapCorVarToCorRel.keySet());
            for (CorrelatorRel.Correlation corVar : corVars) {
                if (mapCorVarToCorRel.get(corVar) == corRel) {
                    mapCorVarToCorRel.put(corVar, newCorRel);
                }
            }

            RelNode newOutputRel =
                aggregateCorrelatorOutput(newCorRel, aggOutputProjRel, isCount);

            call.transformTo(newOutputRel);
        }
    }
}

// End RelDecorrelator.java
