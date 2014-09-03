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
package org.eigenbase.sql2rel;

import java.math.*;
import java.util.*;
import java.util.logging.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.Correlation;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.rel.rules.PushFilterPastJoinRule;
import org.eigenbase.relopt.*;
import org.eigenbase.relopt.hep.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.trace.*;
import org.eigenbase.util.*;
import org.eigenbase.util.mapping.Mappings;

import net.hydromatic.linq4j.Ord;
import net.hydromatic.linq4j.function.Function2;

import net.hydromatic.optiq.util.BitSets;

import com.google.common.base.Supplier;
import com.google.common.collect.*;

/**
 * RelDecorrelator replaces all correlated expressions (corExp) in a relational
 * expression (RelNode) tree with non-correlated expressions that are produced
 * from joining the RelNode that produces the corExp with the RelNode that
 * references it.
 *
 * <p>TODO:</p>
 * <ul>
 *   <li>replace {@code CorelMap} constructor parameter with a RelNode
 *   <li>make {@link #currentRel} immutable (would require a fresh
 *      RelDecorrelator for each node being decorrelated)</li>
 *   <li>make fields of {@link CorelMap} immutable</li>
 *   <li>make sub-class rules static, and have them create their own
 *   de-correlator</li>
 * </ul>
 */
public class RelDecorrelator implements ReflectiveVisitor {
  //~ Static fields/initializers ---------------------------------------------

  private static final Logger SQL2REL_LOGGER =
      EigenbaseTrace.getSqlToRelTracer();

  //~ Instance fields --------------------------------------------------------

  // map built during translation
  private CorelMap cm;

  private final DecorrelateRelVisitor decorrelateVisitor;

  private final RexBuilder rexBuilder;

  // The rel which is being visited
  private RelNode currentRel;

  private final Context context;

  // maps built during decorrelation
  private final Map<RelNode, RelNode> mapOldToNewRel = Maps.newHashMap();

  // map rel to all the newly created correlated variables in its output
  private final Map<RelNode, SortedMap<Correlation, Integer>>
  mapNewRelToMapCorVarToOutputPos = Maps.newHashMap();

  // another map to map old input positions to new input positions
  // this is from the view point of the parent rel of a new rel.
  private final Map<RelNode, Map<Integer, Integer>>
  mapNewRelToMapOldToNewOutputPos = Maps.newHashMap();

  private final HashSet<CorrelatorRel> generatedCorRels = Sets.newHashSet();

  //~ Constructors -----------------------------------------------------------

  private RelDecorrelator(
      RexBuilder rexBuilder,
      CorelMap cm,
      Context context) {
    this.cm = cm;
    this.rexBuilder = rexBuilder;
    this.context = context;

    decorrelateVisitor = new DecorrelateRelVisitor();
  }

  //~ Methods ----------------------------------------------------------------

  /** Decorrelates a query.
   *
   * <p>This is the main entry point to {@code RelDecorrelator}.
   *
   * @param rootRel Root node of the query
   * @return Equivalent query with all {@link CorrelatorRel} instances removed
   */
  public static RelNode decorrelateQuery(RelNode rootRel) {
    final CorelMap corelMap = CorelMap.build(rootRel);
    if (!corelMap.hasCorrelation()) {
      return rootRel;
    }

    final RelOptCluster cluster = rootRel.getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    final RelDecorrelator decorrelator =
        new RelDecorrelator(rexBuilder, corelMap,
            cluster.getPlanner().getContext());


    RelNode newRootRel = decorrelator.removeCorrelationViaRule(rootRel);

    if (SQL2REL_LOGGER.isLoggable(Level.FINE)) {
      SQL2REL_LOGGER.fine(
          RelOptUtil.dumpPlan(
              "Plan after removing CorrelatorRel",
              newRootRel,
              false,
              SqlExplainLevel.EXPPLAN_ATTRIBUTES));
    }

    if (!decorrelator.cm.mapCorVarToCorRel.isEmpty()) {
      newRootRel = decorrelator.decorrelate(newRootRel);
    }

    return newRootRel;
  }

  private void setCurrent(RelNode root, CorrelatorRel corRel) {
    currentRel = corRel;
    if (corRel != null) {
      cm = CorelMap.build(Util.first(root, corRel));
    }
  }

  private RelNode decorrelate(RelNode root) {
    // first adjust count() expression if any
    HepProgram program = HepProgram.builder()
        .addRuleInstance(new AdjustProjectForCountAggregateRule(false))
        .addRuleInstance(new AdjustProjectForCountAggregateRule(true))
        .addRuleInstance(PushFilterPastJoinRule.FILTER_ON_JOIN)
        .build();

    HepPlanner planner = createPlanner(program);

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

  private Function2<RelNode, RelNode, Void> createCopyHook() {
    return new Function2<RelNode, RelNode, Void>() {
      public Void apply(RelNode oldNode, RelNode newNode) {
        if (cm.mapRefRelToCorVar.containsKey(oldNode)) {
          cm.mapRefRelToCorVar.putAll(newNode,
              cm.mapRefRelToCorVar.get(oldNode));
        }
        if (oldNode instanceof CorrelatorRel
            && newNode instanceof CorrelatorRel) {
          CorrelatorRel oldCor = (CorrelatorRel) oldNode;
          for (Correlation c : oldCor.getCorrelations()) {
            if (cm.mapCorVarToCorRel.get(c) == oldNode) {
              cm.mapCorVarToCorRel.put(c, (CorrelatorRel) newNode);
            }
          }

          if (generatedCorRels.contains(oldNode)) {
            generatedCorRels.add((CorrelatorRel) newNode);
          }
        }
        return null;
      }
    };
  }

  private HepPlanner createPlanner(HepProgram program) {
    // Create a planner with a hook to update the mapping tables when a
    // node is copied when it is registered.
    return new HepPlanner(
        program,
        context,
        true,
        createCopyHook(),
        RelOptCostImpl.FACTORY);
  }

  public RelNode removeCorrelationViaRule(RelNode root) {
    HepProgram program = HepProgram.builder()
        .addRuleInstance(new RemoveSingleAggregateRule())
        .addRuleInstance(new RemoveCorrelationForScalarProjectRule())
        .addRuleInstance(new RemoveCorrelationForScalarAggregateRule())
        .build();

    HepPlanner planner = createPlanner(program);

    planner.setRoot(root);
    RelNode newRootRel = planner.findBestExp();

    return newRootRel;
  }

  protected RexNode decorrelateExpr(RexNode exp) {
    DecorrelateRexShuttle shuttle = new DecorrelateRexShuttle();
    return exp.accept(shuttle);
  }

  protected RexNode removeCorrelationExpr(
      RexNode exp,
      boolean projectPulledAboveLeftCorrelator) {
    RemoveCorrelationRexShuttle shuttle =
        new RemoveCorrelationRexShuttle(
            rexBuilder,
            projectPulledAboveLeftCorrelator);
    return exp.accept(shuttle);
  }

  protected RexNode removeCorrelationExpr(
      RexNode exp,
      boolean projectPulledAboveLeftCorrelator,
      RexInputRef nullIndicator) {
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
      Set<Integer> isCount) {
    RemoveCorrelationRexShuttle shuttle =
        new RemoveCorrelationRexShuttle(
            rexBuilder,
            projectPulledAboveLeftCorrelator,
            isCount);
    return exp.accept(shuttle);
  }

  public void decorrelateRelGeneric(RelNode rel) {
    RelNode newRel = rel.copy(rel.getTraitSet(), rel.getInputs());

    if (rel.getInputs().size() > 0) {
      List<RelNode> oldInputs = rel.getInputs();
      List<RelNode> newInputs = Lists.newArrayList();
      for (int i = 0; i < oldInputs.size(); ++i) {
        RelNode newInputRel = mapOldToNewRel.get(oldInputs.get(i));
        if ((newInputRel == null)
            || mapNewRelToMapCorVarToOutputPos.containsKey(newInputRel)) {
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

    // the output position should not change since there are no corVars
    // coming from below.
    Map<Integer, Integer> mapOldToNewOutputPos = Maps.newHashMap();
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
  public void decorrelateRel(SortRel rel) {
    //
    // Rewrite logic:
    //
    // 1. change the collations field to reference the new input.
    //

    // SortRel itself should not reference cor vars.
    assert !cm.mapRefRelToCorVar.containsKey(rel);

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
            rel.getCluster().traitSetOf(Convention.NONE).plus(newCollation),
            newChildRel,
            newCollation,
            rel.offset,
            rel.fetch);

    mapOldToNewRel.put(rel, newRel);

    // SortRel does not change input ordering
    mapNewRelToMapOldToNewOutputPos.put(newRel, childMapOldToNewOutputPos);
  }

  /**
   * Rewrite AggregateRel.
   *
   * @param rel the project rel to rewrite
   */
  public void decorrelateRel(AggregateRel rel) {
    //
    // Rewrite logic:
    //
    // 1. Permute the group by keys to the front.
    // 2. If the child of an AggregateRel produces correlated variables,
    //    add them to the group list.
    // 3. Change aggCalls to reference the new ProjectRel.
    //

    // AggregaterRel itself should not reference cor vars.
    assert !cm.mapRefRelToCorVar.containsKey(rel);

    RelNode oldChildRel = rel.getChild();

    RelNode newChildRel = mapOldToNewRel.get(oldChildRel);
    if (newChildRel == null) {
      // If child has not been rewritten, do not rewrite this rel.
      return;
    }

    Map<Integer, Integer> childMapOldToNewOutputPos =
        mapNewRelToMapOldToNewOutputPos.get(newChildRel);
    assert childMapOldToNewOutputPos != null;

    // map from newChildRel
    Map<Integer, Integer> mapNewChildToProjOutputPos = Maps.newHashMap();
    final int oldGroupKeyCount = rel.getGroupSet().cardinality();

    // ProjectRel projects the original expressions,
    // plus any correlated variables the child wants to pass along.
    final List<Pair<RexNode, String>> projects = Lists.newArrayList();

    List<RelDataTypeField> newChildOutput =
        newChildRel.getRowType().getFieldList();

    int newPos;

    // oldChildRel has the original group by keys in the front.
    for (newPos = 0; newPos < oldGroupKeyCount; newPos++) {
      int newChildPos = childMapOldToNewOutputPos.get(newPos);
      projects.add(RexInputRef.of2(newChildPos, newChildOutput));
      mapNewChildToProjOutputPos.put(newChildPos, newPos);
    }

    SortedMap<Correlation, Integer> mapCorVarToOutputPos = Maps.newTreeMap();

    boolean produceCorVar =
        mapNewRelToMapCorVarToOutputPos.containsKey(newChildRel);
    if (produceCorVar) {
      // If child produces correlated variables, move them to the front,
      // right after any existing groupby fields.

      SortedMap<Correlation, Integer> childMapCorVarToOutputPos =
          mapNewRelToMapCorVarToOutputPos.get(newChildRel);

      // Now add the corVars from the child, starting from
      // position oldGroupKeyCount.
      for (Correlation corVar
          : childMapCorVarToOutputPos.keySet()) {
        int newChildPos = childMapCorVarToOutputPos.get(corVar);
        projects.add(RexInputRef.of2(newChildPos, newChildOutput));

        mapCorVarToOutputPos.put(corVar, newPos);
        mapNewChildToProjOutputPos.put(newChildPos, newPos);
        newPos++;
      }
    }

    // add the remaining fields
    final int newGroupKeyCount = newPos;
    for (int i = 0; i < newChildOutput.size(); i++) {
      if (!mapNewChildToProjOutputPos.containsKey(i)) {
        projects.add(RexInputRef.of2(i, newChildOutput));
        mapNewChildToProjOutputPos.put(i, newPos);
        newPos++;
      }
    }

    assert newPos == newChildOutput.size();

    // This ProjectRel will be what the old child maps to,
    // replacing any previous mapping from old child).
    RelNode newProjectRel =
        RelOptUtil.createProject(newChildRel, projects, false);

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
    Map<Integer, Integer> combinedMap = Maps.newHashMap();

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
    List<AggregateCall> newAggCalls = Lists.newArrayList();
    List<AggregateCall> oldAggCalls = rel.getAggCallList();

    // AggregateRel.Call oldAggCall;
    int oldChildOutputFieldCount = oldChildRel.getRowType().getFieldCount();
    int newChildOutputFieldCount =
        newProjectRel.getRowType().getFieldCount();

    int i = -1;
    for (AggregateCall oldAggCall : oldAggCalls) {
      ++i;
      List<Integer> oldAggArgs = oldAggCall.getArgList();

      List<Integer> aggArgs = Lists.newArrayList();

      // Adjust the aggregator argument positions.
      // Note aggregator does not change input ordering, so the child
      // output position mapping can be used to derive the new positions
      // for the argument.
      for (int oldPos : oldAggArgs) {
        aggArgs.add(combinedMap.get(oldPos));
      }

      newAggCalls.add(
          oldAggCall.adaptTo(newProjectRel, aggArgs,
              oldGroupKeyCount, newGroupKeyCount));

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
            BitSets.range(newGroupKeyCount),
            newAggCalls);

    mapOldToNewRel.put(rel, newAggregateRel);

    mapNewRelToMapOldToNewOutputPos.put(newAggregateRel, combinedMap);

    if (produceCorVar) {
      // AggregateRel does not change input ordering so corVars will be
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
  public void decorrelateRel(ProjectRel rel) {
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
    List<RexNode> oldProj = rel.getProjects();
    List<RelDataTypeField> relOutput = rel.getRowType().getFieldList();

    Map<Integer, Integer> childMapOldToNewOutputPos =
        mapNewRelToMapOldToNewOutputPos.get(newChildRel);
    assert childMapOldToNewOutputPos != null;

    Map<Integer, Integer> mapOldToNewOutputPos = Maps.newHashMap();

    boolean produceCorVar =
        mapNewRelToMapCorVarToOutputPos.containsKey(newChildRel);

    // ProjectRel projects the original expressions,
    // plus any correlated variables the child wants to pass along.
    final List<Pair<RexNode, String>> projects = Lists.newArrayList();

    // If this ProjectRel has correlated reference, create value generator
    // and produce the correlated variables in the new output.
    if (cm.mapRefRelToCorVar.containsKey(rel)) {
      decorrelateInputWithValueGenerator(rel);

      // The old child should be mapped to the JoinRel created by
      // rewriteInputWithValueGenerator().
      newChildRel = mapOldToNewRel.get(oldChildRel);
      produceCorVar = true;
    }

    // ProjectRel projects the original expressions
    int newPos;
    for (newPos = 0; newPos < oldProj.size(); newPos++) {
      projects.add(
          newPos,
          Pair.of(
              decorrelateExpr(oldProj.get(newPos)),
              relOutput.get(newPos).getName()));
      mapOldToNewOutputPos.put(newPos, newPos);
    }

    SortedMap<Correlation, Integer> mapCorVarToOutputPos = Maps.newTreeMap();

    // Project any correlated variables the child wants to pass along.
    if (produceCorVar) {
      SortedMap<Correlation, Integer> childMapCorVarToOutputPos =
          mapNewRelToMapCorVarToOutputPos.get(newChildRel);

      // propagate cor vars from the new child
      List<RelDataTypeField> newChildOutput =
          newChildRel.getRowType().getFieldList();
      for (Correlation corVar
          : childMapCorVarToOutputPos.keySet()) {
        int corVarPos = childMapCorVarToOutputPos.get(corVar);
        projects.add(RexInputRef.of2(corVarPos, newChildOutput));
        mapCorVarToOutputPos.put(corVar, newPos);
        newPos++;
      }
    }

    RelNode newProjectRel =
        RelOptUtil.createProject(newChildRel, projects, false);

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
   * @param correlations         correlated variables to generate
   * @param valueGenFieldOffset  offset in the output that generated columns
   *                             will start
   * @param mapCorVarToOutputPos output positions for the correlated variables
   *                             generated
   * @return RelNode the root of the resultant RelNode tree
   */
  private RelNode createValueGenerator(
      Iterable<Correlation> correlations,
      int valueGenFieldOffset,
      SortedMap<Correlation, Integer> mapCorVarToOutputPos) {
    RelNode resultRel = null;

    Map<RelNode, List<Integer>> mapNewInputRelToOutputPos = Maps.newHashMap();

    Map<RelNode, Integer> mapNewInputRelToNewOffset = Maps.newHashMap();

    RelNode oldInputRel;
    RelNode newInputRel;
    List<Integer> newLocalOutputPosList;

    // inputRel provides the definition of a correlated variable.
    // Add to map all the referenced positions(relative to each input rel)
    for (Correlation corVar : correlations) {
      int oldCorVarOffset = corVar.getOffset();

      oldInputRel = cm.mapCorVarToCorRel.get(corVar).getInput(0);
      assert oldInputRel != null;
      newInputRel = mapOldToNewRel.get(oldInputRel);
      assert newInputRel != null;

      if (!mapNewInputRelToOutputPos.containsKey(newInputRel)) {
        newLocalOutputPosList = Lists.newArrayList();
      } else {
        newLocalOutputPosList =
            mapNewInputRelToOutputPos.get(newInputRel);
      }

      Map<Integer, Integer> mapOldToNewOutputPos =
          mapNewRelToMapOldToNewOutputPos.get(newInputRel);
      assert mapOldToNewOutputPos != null;

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
    // join these rels based on their occurrence in cor var list which
    // is sorted.
    Set<RelNode> joinedInputRelSet = Sets.newHashSet();

    for (Correlation corVar : correlations) {
      oldInputRel = cm.mapCorVarToCorRel.get(corVar).getInput(0);
      assert oldInputRel != null;
      newInputRel = mapOldToNewRel.get(oldInputRel);
      assert newInputRel != null;

      if (!joinedInputRelSet.contains(newInputRel)) {
        RelNode projectRel =
            RelOptUtil.createProject(
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
    int newOutputPos;
    int newLocalOutputPos;
    for (Correlation corVar : correlations) {
      // The first child of a correlatorRel is always the rel defining
      // the correlated variables.
      newInputRel =
          mapOldToNewRel.get(cm.mapCorVarToCorRel.get(corVar).getInput(0));
      newLocalOutputPosList = mapNewInputRelToOutputPos.get(newInputRel);

      Map<Integer, Integer> mapOldToNewOutputPos =
          mapNewRelToMapOldToNewOutputPos.get(newInputRel);
      assert mapOldToNewOutputPos != null;

      newLocalOutputPos = mapOldToNewOutputPos.get(corVar.getOffset());

      // newOutputPos is the index of the cor var in the referenced
      // position list plus the offset of referenced postition list of
      // each newInputRel.
      newOutputPos =
          newLocalOutputPosList.indexOf(newLocalOutputPos)
              + mapNewInputRelToNewOffset.get(newInputRel)
              + valueGenFieldOffset;

      if (mapCorVarToOutputPos.containsKey(corVar)) {
        assert mapCorVarToOutputPos.get(corVar) == newOutputPos;
      }
      mapCorVarToOutputPos.put(corVar, newOutputPos);
    }

    return resultRel;
  }

  private void decorrelateInputWithValueGenerator(
      RelNode rel) {
    // currently only handles one child input
    assert rel.getInputs().size() == 1;
    RelNode oldChildRel = rel.getInput(0);
    RelNode newChildRel = mapOldToNewRel.get(oldChildRel);

    Map<Integer, Integer> childMapOldToNewOutputPos =
        mapNewRelToMapOldToNewOutputPos.get(newChildRel);
    assert childMapOldToNewOutputPos != null;

    SortedMap<Correlation, Integer> mapCorVarToOutputPos = Maps.newTreeMap();

    if (mapNewRelToMapCorVarToOutputPos.containsKey(newChildRel)) {
      mapCorVarToOutputPos.putAll(
          mapNewRelToMapCorVarToOutputPos.get(newChildRel));
    }

    final Collection<Correlation> corVarList = cm.mapRefRelToCorVar.get(rel);

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
  public void decorrelateRel(FilterRel rel) {
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
    assert childMapOldToNewOutputPos != null;

    boolean produceCorVar =
        mapNewRelToMapCorVarToOutputPos.containsKey(newChildRel);

    // If this FilterRel has correlated reference, create value generator
    // and produce the correlated variables in the new output.
    if (cm.mapRefRelToCorVar.containsKey(rel)) {
      decorrelateInputWithValueGenerator(rel);

      // The old child should be mapped to the newly created JoinRel by
      // rewriteInputWithValueGenerator().
      newChildRel = mapOldToNewRel.get(oldChildRel);
      produceCorVar = true;
    }

    // Replace the filter expression to reference output of the join
    // Map filter to the new filter over join
    RelNode newFilterRel =
        RelOptUtil.createFilter(
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
  public void decorrelateRel(CorrelatorRel rel) {
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

    SortedMap<Correlation, Integer> rightChildMapCorVarToOutputPos =
        mapNewRelToMapCorVarToOutputPos.get(newRightRel);

    if (rightChildMapCorVarToOutputPos == null) {
      return;
    }

    Map<Integer, Integer> leftChildMapOldToNewOutputPos =
        mapNewRelToMapOldToNewOutputPos.get(newLeftRel);
    assert leftChildMapOldToNewOutputPos != null;

    Map<Integer, Integer> rightChildMapOldToNewOutputPos =
        mapNewRelToMapOldToNewOutputPos.get(newRightRel);

    assert rightChildMapOldToNewOutputPos != null;

    SortedMap<Correlation, Integer> mapCorVarToOutputPos =
        rightChildMapCorVarToOutputPos;

    assert rel.getCorrelations().size()
        <= rightChildMapCorVarToOutputPos.keySet().size();

    // Change correlator rel into a join.
    // Join all the correlated variables produced by this correlator rel
    // with the values generated and propagated from the right input
    RexNode condition = rel.getCondition();
    final List<RelDataTypeField> newLeftOutput =
        newLeftRel.getRowType().getFieldList();
    int newLeftFieldCount = newLeftOutput.size();

    final List<RelDataTypeField> newRightOutput =
        newRightRel.getRowType().getFieldList();

    int newLeftPos;
    int newRightPos;
    for (Correlation corVar : rel.getCorrelations()) {
      newLeftPos = leftChildMapOldToNewOutputPos.get(corVar.getOffset());
      newRightPos = rightChildMapCorVarToOutputPos.get(corVar);
      RexNode equi =
          rexBuilder.makeCall(
              SqlStdOperatorTable.EQUALS,
              RexInputRef.of(newLeftPos, newLeftOutput),
              new RexInputRef(
                  newLeftFieldCount + newRightPos,
                  newRightOutput.get(newRightPos).getType()));
      if (condition == rexBuilder.makeLiteral(true)) {
        condition = equi;
      } else {
        condition =
            rexBuilder.makeCall(
                SqlStdOperatorTable.AND,
                condition,
                equi);
      }

      // remove this cor var from output position mapping
      mapCorVarToOutputPos.remove(corVar);
    }

    // Update the output position for the cor vars: only pass on the cor
    // vars that are not used in the join key.
    for (Correlation corVar : mapCorVarToOutputPos.keySet()) {
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
    Map<Integer, Integer> mapOldToNewOutputPos = Maps.newHashMap();

    int oldLeftFieldCount = oldLeftRel.getRowType().getFieldCount();

    int oldRightFieldCount = oldRightRel.getRowType().getFieldCount();
    assert rel.getRowType().getFieldCount()
        == oldLeftFieldCount + oldRightFieldCount;

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
  public void decorrelateRel(JoinRel rel) {
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
    assert leftChildMapOldToNewOutputPos != null;

    Map<Integer, Integer> rightChildMapOldToNewOutputPos =
        mapNewRelToMapOldToNewOutputPos.get(newRightRel);
    assert rightChildMapOldToNewOutputPos != null;

    SortedMap<Correlation, Integer> mapCorVarToOutputPos = Maps.newTreeMap();

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
    Map<Integer, Integer> mapOldToNewOutputPos = Maps.newHashMap();

    int oldLeftFieldCount = oldLeftRel.getRowType().getFieldCount();
    int newLeftFieldCount = newLeftRel.getRowType().getFieldCount();

    int oldRightFieldCount = oldRightRel.getRowType().getFieldCount();
    assert rel.getRowType().getFieldCount()
        == oldLeftFieldCount + oldRightFieldCount;

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
      SortedMap<Correlation, Integer> rightChildMapCorVarToOutputPos =
          mapNewRelToMapCorVarToOutputPos.get(newRightRel);
      for (Correlation corVar : rightChildMapCorVarToOutputPos.keySet()) {
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

  private RexInputRef getNewForOldInputRef(RexInputRef oldInputRef) {
    assert currentRel != null;

    int oldOrdinal = oldInputRef.getIndex();
    int newOrdinal = 0;

    // determine which input rel oldOrdinal references, and adjust
    // oldOrdinal to be relative to that input rel
    List<RelNode> oldInputRels = currentRel.getInputs();
    RelNode oldInputRel = null;

    for (RelNode oldInputRel0 : oldInputRels) {
      RelDataType oldInputType = oldInputRel0.getRowType();
      int n = oldInputType.getFieldCount();
      if (oldOrdinal < n) {
        oldInputRel = oldInputRel0;
        break;
      }
      RelNode newInput = mapOldToNewRel.get(oldInputRel0);
      newOrdinal += newInput.getRowType().getFieldCount();
      oldOrdinal -= n;
    }

    assert oldInputRel != null;

    RelNode newInputRel = mapOldToNewRel.get(oldInputRel);
    assert newInputRel != null;

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

    return new RexInputRef(newOrdinal,
        newInputRel.getRowType().getFieldList().get(newLocalOrdinal).getType());
  }

  /**
   * Pull projRel above the joinRel from its RHS input. Enforce nullability
   * for join output.
   *
   * @param joinRel          Join
   * @param projRel          the original projRel as the RHS input of the join.
   * @param nullIndicatorPos Position of null indicator
   * @return the subtree with the new ProjectRel at the root
   */
  private RelNode projectJoinOutputWithNullability(
      JoinRel joinRel,
      ProjectRel projRel,
      int nullIndicatorPos) {
    RelDataTypeFactory typeFactory = joinRel.getCluster().getTypeFactory();
    RelNode leftInputRel = joinRel.getLeft();
    JoinRelType joinType = joinRel.getJoinType();

    RexInputRef nullIndicator =
        new RexInputRef(
            nullIndicatorPos,
            typeFactory.createTypeWithNullability(
                joinRel.getRowType().getFieldList().get(nullIndicatorPos)
                    .getType(),
                true));

    // now create the new project
    List<Pair<RexNode, String>> newProjExprs = Lists.newArrayList();

    // project everything from the LHS and then those from the original
    // projRel
    List<RelDataTypeField> leftInputFields =
        leftInputRel.getRowType().getFieldList();

    for (int i = 0; i < leftInputFields.size(); i++) {
      newProjExprs.add(RexInputRef.of2(i, leftInputFields));
    }

    // Marked where the projected expr is coming from so that the types will
    // become nullable for the original projections which are now coming out
    // of the nullable side of the OJ.
    boolean projectPulledAboveLeftCorrelator =
        joinType.generatesNullsOnRight();

    for (Pair<RexNode, String> pair : projRel.getNamedProjects()) {
      RexNode newProjExpr =
          removeCorrelationExpr(
              pair.left,
              projectPulledAboveLeftCorrelator,
              nullIndicator);

      newProjExprs.add(Pair.of(newProjExpr, pair.right));
    }

    RelNode newProjRel =
        RelOptUtil.createProject(joinRel, newProjExprs, false);

    return newProjRel;
  }

  /**
   * Pulls projRel above the joinRel from its RHS input. Enforces nullability
   * for join output.
   *
   * @param corRel  Correlator
   * @param projRel the original ProjectRel as the RHS input of the join
   * @param isCount Positions which are calls to the <code>COUNT</code>
   *                aggregation function
   * @return the subtree with the new ProjectRel at the root
   */
  private RelNode aggregateCorrelatorOutput(
      CorrelatorRel corRel,
      ProjectRel projRel,
      Set<Integer> isCount) {
    RelNode leftInputRel = corRel.getLeft();
    JoinRelType joinType = corRel.getJoinType();

    // now create the new project
    List<Pair<RexNode, String>> newProjects = Lists.newArrayList();

    // project everything from the LHS and then those from the original
    // projRel
    List<RelDataTypeField> leftInputFields =
        leftInputRel.getRowType().getFieldList();

    for (int i = 0; i < leftInputFields.size(); i++) {
      newProjects.add(RexInputRef.of2(i, leftInputFields));
    }

    // Marked where the projected expr is coming from so that the types will
    // become nullable for the original projections which are now coming out
    // of the nullable side of the OJ.
    boolean projectPulledAboveLeftCorrelator =
        joinType.generatesNullsOnRight();

    for (Pair<RexNode, String> pair : projRel.getNamedProjects()) {
      RexNode newProjExpr =
          removeCorrelationExpr(
              pair.left,
              projectPulledAboveLeftCorrelator,
              isCount);
      newProjects.add(Pair.of(newProjExpr, pair.right));
    }

    return RelOptUtil.createProject(corRel, newProjects, false);
  }

  /**
   * Checks whether the correlations in projRel and filterRel are related to
   * the correlated variables provided by corRel.
   *
   * @param corRel             Correlator
   * @param projRel            the original ProjectRel as the RHS input of the
   *                           join
   * @param filterRel          Filter
   * @param correlatedJoinKeys Correlated join keys
   * @return true if filter and proj only references corVar provided by corRel
   */
  private boolean checkCorVars(
      CorrelatorRel corRel,
      ProjectRel projRel,
      FilterRel filterRel,
      List<RexFieldAccess> correlatedJoinKeys) {
    if (filterRel != null) {
      assert correlatedJoinKeys != null;

      // check that all correlated refs in the filter condition are
      // used in the join(as field access).
      Set<Correlation> corVarInFilter =
          Sets.newHashSet(cm.mapRefRelToCorVar.get(filterRel));

      for (RexFieldAccess correlatedJoinKey : correlatedJoinKeys) {
        corVarInFilter.remove(
            cm.mapFieldAccessToCorVar.get(correlatedJoinKey));
      }

      if (!corVarInFilter.isEmpty()) {
        return false;
      }

      // Check that the correlated variables referenced in these
      // comparisons do come from the correlatorRel.
      corVarInFilter.addAll(cm.mapRefRelToCorVar.get(filterRel));

      for (Correlation corVar : corVarInFilter) {
        if (cm.mapCorVarToCorRel.get(corVar) != corRel) {
          return false;
        }
      }
    }

    // if projRel has any correlated reference, make sure they are also
    // provided by the current corRel. They will be projected out of the LHS
    // of the corRel.
    if ((projRel != null) && cm.mapRefRelToCorVar.containsKey(projRel)) {
      for (Correlation corVar : cm.mapRefRelToCorVar.get(projRel)) {
        if (cm.mapCorVarToCorRel.get(corVar) != corRel) {
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
  private void removeCorVarFromTree(CorrelatorRel corRel) {
    for (Correlation c : Lists.newArrayList(cm.mapCorVarToCorRel.keySet())) {
      if (cm.mapCorVarToCorRel.get(c) == corRel) {
        cm.mapCorVarToCorRel.remove(c);
      }
    }
  }

  /**
   * Project all childRel output fields plus the additional expressions.
   *
   * @param childRel        Child relational expression
   * @param additionalExprs Additional expressions and names
   * @return the new ProjectRel
   */
  private RelNode createProjectWithAdditionalExprs(
      RelNode childRel,
      List<Pair<RexNode, String>> additionalExprs) {
    final List<RelDataTypeField> fieldList =
        childRel.getRowType().getFieldList();
    List<Pair<RexNode, String>> projects = Lists.newArrayList();
    for (Ord<RelDataTypeField> field : Ord.zip(fieldList)) {
      projects.add(
          Pair.of(
              (RexNode) rexBuilder.makeInputRef(
                  field.e.getType(), field.i),
              field.e.getName()));
    }
    projects.addAll(additionalExprs);
    return RelOptUtil.createProject(childRel, projects, false);
  }

  //~ Inner Classes ----------------------------------------------------------

  private class DecorrelateRelVisitor extends RelVisitor {
    private final ReflectiveVisitDispatcher<RelDecorrelator, RelNode>
    dispatcher =
        ReflectUtil.createDispatcher(
            RelDecorrelator.class,
            RelNode.class);

    // implement RelVisitor
    public void visit(RelNode p, int ordinal, RelNode parent) {
      // rewrite children first  (from left to right)
      super.visit(p, ordinal, parent);

      currentRel = p;

      final String visitMethodName = "decorrelateRel";
      boolean found =
          dispatcher.invokeVisitor(
              RelDecorrelator.this,
              currentRel,
              visitMethodName);
      setCurrent(null, null);

      if (!found) {
        decorrelateRelGeneric(p);
      }
      // else no rewrite will occur. This will terminate the bottom-up
      // rewrite. If root node of a RelNode tree is not rewritten, the
      // original tree will be returned. See decorrelate() method.
    }
  }

  private class DecorrelateRexShuttle extends RexShuttle {
    // override RexShuttle
    public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
      int newInputRelOutputOffset = 0;
      RelNode oldInputRel;
      RelNode newInputRel;
      Integer newInputPos;

      List<RelNode> inputs = currentRel.getInputs();
      for (int i = 0; i < inputs.size(); i++) {
        oldInputRel = inputs.get(i);
        newInputRel = mapOldToNewRel.get(oldInputRel);

        if ((newInputRel != null)
            && mapNewRelToMapCorVarToOutputPos.containsKey(newInputRel)) {
          SortedMap<Correlation, Integer> childMapCorVarToOutputPos =
              mapNewRelToMapCorVarToOutputPos.get(newInputRel);

          if (childMapCorVarToOutputPos != null) {
            // try to find in this input rel the position of cor var
            Correlation corVar = cm.mapFieldAccessToCorVar.get(fieldAccess);

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
    public RexNode visitInputRef(RexInputRef inputRef) {
      RexInputRef newInputRef = getNewForOldInputRef(inputRef);
      return newInputRef;
    }
  }

  private class RemoveCorrelationRexShuttle extends RexShuttle {
    RexBuilder rexBuilder;
    RelDataTypeFactory typeFactory;
    boolean projectPulledAboveLeftCorrelator;
    RexInputRef nullIndicator;
    Set<Integer> isCount;

    public RemoveCorrelationRexShuttle(
        RexBuilder rexBuilder,
        boolean projectPulledAboveLeftCorrelator) {
      this(
          rexBuilder,
          projectPulledAboveLeftCorrelator,
          null, null);
    }

    public RemoveCorrelationRexShuttle(
        RexBuilder rexBuilder,
        boolean projectPulledAboveLeftCorrelator,
        RexInputRef nullIndicator) {
      this(
          rexBuilder,
          projectPulledAboveLeftCorrelator,
          nullIndicator,
          null);
    }

    public RemoveCorrelationRexShuttle(
        RexBuilder rexBuilder,
        boolean projectPulledAboveLeftCorrelator,
        Set<Integer> isCount) {
      this(
          rexBuilder,
          projectPulledAboveLeftCorrelator,
          null, isCount);
    }

    public RemoveCorrelationRexShuttle(
        RexBuilder rexBuilder,
        boolean projectPulledAboveLeftCorrelator,
        RexInputRef nullIndicator,
        Set<Integer> isCount) {
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
        RexNode rexNode) {
      RexNode[] caseOperands = new RexNode[3];

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
              SqlStdOperatorTable.IS_NULL,
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
          SqlStdOperatorTable.CASE,
          caseOperands);
    }

    // override RexShuttle
    public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
      if (cm.mapFieldAccessToCorVar.containsKey(fieldAccess)) {
        // if it is a corVar, change it to be input ref.
        Correlation corVar = cm.mapFieldAccessToCorVar.get(fieldAccess);

        // corVar offset should point to the leftInput of currentRel,
        // which is the CorrelatorRel.
        RexNode newRexNode =
            new RexInputRef(corVar.getOffset(), fieldAccess.getType());

        if (projectPulledAboveLeftCorrelator
            && (nullIndicator != null)) {
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
    public RexNode visitInputRef(RexInputRef inputRef) {
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
    public RexNode visitLiteral(RexLiteral literal) {
      // Use nullIndicator to decide whether to project null.
      // Do nothing if the literal is null.
      if (!RexUtil.isNull(literal)
          && projectPulledAboveLeftCorrelator
          && (nullIndicator != null)) {
        return createCaseExpression(
            nullIndicator,
            rexBuilder.constantNull(),
            literal);
      }
      return literal;
    }

    public RexNode visitCall(final RexCall call) {
      RexNode newCall;

      boolean[] update = {false};
      List<RexNode> clonedOperands = visitList(call.operands, update);
      if (update[0]) {
        SqlOperator operator = call.getOperator();

        boolean isSpecialCast = false;
        if (operator instanceof SqlFunction) {
          SqlFunction function = (SqlFunction) operator;
          if (function.getKind() == SqlKind.CAST) {
            if (call.operands.size() < 2) {
              isSpecialCast = true;
            }
          }
        }

        final RelDataType newType;
        if (!isSpecialCast) {
          // TODO: ideally this only needs to be called if the result
          // type will also change. However, since that requires
          // support from type inference rules to tell whether a rule
          // decides return type based on input types, for now all
          // operators will be recreated with new type if any operand
          // changed, unless the operator has "built-in" type.
          newType = rexBuilder.deriveReturnType(operator, clonedOperands);
        } else {
          // Use the current return type when creating a new call, for
          // operators with return type built into the operator
          // definition, and with no type inference rules, such as
          // cast function with less than 2 operands.

          // TODO: Comments in RexShuttle.visitCall() mention other
          // types in this category. Need to resolve those together
          // and preferably in the base class RexShuttle.
          newType = call.getType();
        }
        newCall =
            rexBuilder.makeCall(
                newType,
                operator,
                clonedOperands);
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
  private final class RemoveSingleAggregateRule extends RelOptRule {
    public RemoveSingleAggregateRule() {
      super(
          operand(
              AggregateRel.class,
              operand(
                  ProjectRel.class,
                  operand(AggregateRel.class, any()))));
    }

    public void onMatch(RelOptRuleCall call) {
      AggregateRel singleAggRel = call.rel(0);
      ProjectRel projRel = call.rel(1);
      AggregateRel aggRel = call.rel(2);

      // check singleAggRel is single_value agg
      if ((!singleAggRel.getGroupSet().isEmpty())
          || (singleAggRel.getAggCallList().size() != 1)
          || !(singleAggRel.getAggCallList().get(0).getAggregation()
          instanceof SqlSingleValueAggFunction)) {
        return;
      }

      // check projRel only projects one expression
      // check this project only projects one expression, i.e. scalar
      // subqueries.
      List<RexNode> projExprs = projRel.getProjects();
      if (projExprs.size() != 1) {
        return;
      }

      // check the input to projRel is an aggregate on the entire input
      if (!aggRel.getGroupSet().isEmpty()) {
        return;
      }

      // singleAggRel produces a nullable type, so create the new
      // projection that casts proj expr to a nullable type.
      final RelOptCluster cluster = projRel.getCluster();
      RelNode newProjRel =
          RelOptUtil.createProject(aggRel,
              ImmutableList.of(
                  rexBuilder.makeCast(
                      cluster.getTypeFactory().createTypeWithNullability(
                          projExprs.get(0).getType(),
                          true),
                      projExprs.get(0))),
              null);
      call.transformTo(newProjRel);
    }
  }

  private final class RemoveCorrelationForScalarProjectRule extends RelOptRule {
    public RemoveCorrelationForScalarProjectRule() {
      super(
          operand(
              CorrelatorRel.class,
              operand(RelNode.class, any()),
              operand(
                  AggregateRel.class,
                  operand(
                      ProjectRel.class,
                      operand(RelNode.class, any())))));
    }

    public void onMatch(RelOptRuleCall call) {
      CorrelatorRel corRel = call.rel(0);
      RelNode leftInputRel = call.rel(1);
      AggregateRel aggRel = call.rel(2);
      ProjectRel projRel = call.rel(3);
      RelNode rightInputRel = call.rel(4);
      RelOptCluster cluster = corRel.getCluster();

      setCurrent(call.getPlanner().getRoot(), corRel);

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
          || (joinCond != rexBuilder.makeLiteral(true))) {
        return;
      }

      // check that the agg is of the following type:
      // doing a single_value() on the entire input
      if ((!aggRel.getGroupSet().isEmpty())
          || (aggRel.getAggCallList().size() != 1)
          || !(aggRel.getAggCallList().get(0).getAggregation()
          instanceof SqlSingleValueAggFunction)) {
        return;
      }

      // check this project only projects one expression, i.e. scalar
      // subqueries.
      if (projRel.getProjects().size() != 1) {
        return;
      }

      int nullIndicatorPos;

      if ((rightInputRel instanceof FilterRel)
          && cm.mapRefRelToCorVar.containsKey(rightInputRel)) {
        // rightInputRel has this shape:
        //
        //       FilterRel (references corvar)
        //         FilterInputRel

        // If rightInputRel is a filter and contains correlated
        // reference, make sure the correlated keys in the filter
        // condition forms a unique key of the RHS.

        FilterRel filterRel = (FilterRel) rightInputRel;
        rightInputRel = filterRel.getChild();

        assert rightInputRel instanceof HepRelVertex;
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
        List<RexNode> tmpRightJoinKeys = Lists.newArrayList();
        List<RexNode> correlatedJoinKeys = Lists.newArrayList();
        RelOptUtil.splitCorrelatedFilterCondition(
            filterRel,
            tmpRightJoinKeys,
            correlatedJoinKeys,
            false);

        // check that the columns referenced in these comparisons form
        // an unique key of the filterInputRel
        List<RexInputRef> rightJoinKeys = new ArrayList<RexInputRef>();
        for (RexNode key : tmpRightJoinKeys) {
          assert key instanceof RexInputRef;
          rightJoinKeys.add((RexInputRef) key);
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
            rightJoinKeys)) {
          SQL2REL_LOGGER.fine(
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

        if (!checkCorVars(corRel, projRel, filterRel, correlatedKeyList)) {
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
      } else if (cm.mapRefRelToCorVar.containsKey(projRel)) {
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
            createProjectWithAdditionalExprs(rightInputRel,
                ImmutableList.of(
                    Pair.<RexNode, String>of(
                        rexBuilder.makeLiteral(true), "nullIndicator")));

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
      JoinRel joinRel =
          new JoinRel(
              corRel.getCluster(),
              leftInputRel,
              rightInputRel,
              joinCond,
              joinType,
              ImmutableSet.<String>of());

      RelNode newProjRel =
          projectJoinOutputWithNullability(joinRel, projRel, nullIndicatorPos);

      call.transformTo(newProjRel);

      removeCorVarFromTree(corRel);
    }
  }

  private final class RemoveCorrelationForScalarAggregateRule
      extends RelOptRule {
    public RemoveCorrelationForScalarAggregateRule() {
      super(
          operand(
              CorrelatorRel.class,
              operand(RelNode.class, any()),
              operand(
                  ProjectRel.class,
                  operand(
                      AggregateRel.class,
                      operand(
                          ProjectRel.class,
                          operand(RelNode.class, any()))))));
    }

    public void onMatch(RelOptRuleCall call) {
      CorrelatorRel corRel = call.rel(0);
      RelNode leftInputRel = call.rel(1);
      ProjectRel aggOutputProjRel = call.rel(2);
      AggregateRel aggRel = call.rel(3);
      ProjectRel aggInputProjRel = call.rel(4);
      RelNode rightInputRel = call.rel(5);
      RelOptCluster cluster = corRel.getCluster();

      setCurrent(call.getPlanner().getRoot(), corRel);

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
      List<RexNode> aggOutputProjExprs = aggOutputProjRel.getProjects();
      if (aggOutputProjExprs.size() != 1) {
        return;
      }

      JoinRelType joinType = corRel.getJoinType();
      RexNode joinCond = corRel.getCondition();
      if ((joinType != JoinRelType.LEFT)
          || (joinCond != rexBuilder.makeLiteral(true))) {
        return;
      }

      // check that the agg is on the entire input
      if (!aggRel.getGroupSet().isEmpty()) {
        return;
      }

      List<RexNode> aggInputProjExprs = aggInputProjRel.getProjects();

      List<AggregateCall> aggCalls = aggRel.getAggCallList();
      Set<Integer> isCountStar = Sets.newHashSet();

      // mark if agg produces count(*) which needs to reference the
      // nullIndicator after the transformation.
      int k = -1;
      for (AggregateCall aggCall : aggCalls) {
        ++k;
        if ((aggCall.getAggregation() instanceof SqlCountAggFunction)
            && (aggCall.getArgList().size() == 0)) {
          isCountStar.add(k);
        }
      }

      if ((rightInputRel instanceof FilterRel)
          && cm.mapRefRelToCorVar.containsKey(rightInputRel)) {
        // rightInputRel has this shape:
        //
        //       FilterRel (references corvar)
        //         FilterInputRel
        FilterRel filterRel = (FilterRel) rightInputRel;
        rightInputRel = filterRel.getChild();

        assert rightInputRel instanceof HepRelVertex;
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
        List<RexNode> rightJoinKeys = Lists.newArrayList();
        List<RexNode> tmpCorrelatedJoinKeys = Lists.newArrayList();
        RelOptUtil.splitCorrelatedFilterCondition(
            filterRel,
            rightJoinKeys,
            tmpCorrelatedJoinKeys,
            true);

        // make sure the correlated reference forms a unique key check
        // that the columns referenced in these comparisons form an
        // unique key of the leftInputRel
        List<RexFieldAccess> correlatedJoinKeys = Lists.newArrayList();
        List<RexInputRef> correlatedInputRefJoinKeys = Lists.newArrayList();
        for (RexNode joinKey : tmpCorrelatedJoinKeys) {
          assert joinKey instanceof RexFieldAccess;
          correlatedJoinKeys.add((RexFieldAccess) joinKey);
          RexNode correlatedInputRef =
              removeCorrelationExpr(joinKey, false);
          assert correlatedInputRef instanceof RexInputRef;
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
            correlatedInputRefJoinKeys)) {
          SQL2REL_LOGGER.fine(
              correlatedJoinKeys.toString()
                  + "are not unique keys for "
                  + leftInputRel.toString());
          return;
        }

        // check cor var references are valid
        if (!checkCorVars(corRel,
            aggInputProjRel,
            filterRel,
            correlatedJoinKeys)) {
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
      } else if (cm.mapRefRelToCorVar.containsKey(aggInputProjRel)) {
        // check rightInputRel contains no correlation
        if (RelOptUtil.getVariablesUsed(rightInputRel).size() > 0) {
          return;
        }

        // check cor var references are valid
        if (!checkCorVars(corRel, aggInputProjRel, null, null)) {
          return;
        }

        int nFields = leftInputRel.getRowType().getFieldCount();
        BitSet allCols = BitSets.range(nFields);

        // leftInputRel contains unique keys
        // i.e. each row is distinct and can group by on all the left
        // fields
        if (!RelMdUtil.areColumnsDefinitelyUnique(
            leftInputRel,
            allCols)) {
          SQL2REL_LOGGER.fine("There are no unique keys for " + leftInputRel);
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
          leftInputFieldCount + aggInputProjExprs.size() + 1;

      rightInputRel =
          createProjectWithAdditionalExprs(rightInputRel,
              ImmutableList.of(Pair.<RexNode, String>of(rexBuilder.makeLiteral(
                      true), "nullIndicator")));

      JoinRel joinRel =
          new JoinRel(
              cluster,
              leftInputRel,
              rightInputRel,
              joinCond,
              joinType,
              ImmutableSet.<String>of());

      // To the consumer of joinOutputProjRel, nullIndicator is located
      // at the end
      int nullIndicatorPos = joinRel.getRowType().getFieldCount() - 1;

      RexInputRef nullIndicator =
          new RexInputRef(
              nullIndicatorPos,
              cluster.getTypeFactory().createTypeWithNullability(
                  joinRel.getRowType().getFieldList()
                      .get(nullIndicatorPos).getType(),
                  true));

      // first project all group-by keys plus the transformed agg input
      List<RexNode> joinOutputProjExprs = Lists.newArrayList();

      // LOJ Join preserves LHS types
      for (int i = 0; i < leftInputFieldCount; i++) {
        joinOutputProjExprs.add(
            rexBuilder.makeInputRef(
                leftInputFieldType.getFieldList().get(i).getType(), i));
      }

      for (RexNode aggInputProjExpr : aggInputProjExprs) {
        joinOutputProjExprs.add(
            removeCorrelationExpr(aggInputProjExpr,
                joinType.generatesNullsOnRight(),
                nullIndicator));
      }

      joinOutputProjExprs.add(
          rexBuilder.makeInputRef(joinRel, nullIndicatorPos));

      RelNode joinOutputProjRel =
          RelOptUtil.createProject(
              joinRel,
              joinOutputProjExprs,
              null);

      // nullIndicator is now at a different location in the output of
      // the join
      nullIndicatorPos = joinOutputProjExprCount - 1;

      final int groupCount = leftInputFieldCount;

      List<AggregateCall> newAggCalls = Lists.newArrayList();
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
          newAggArgs = Lists.newArrayList();

          for (Integer aggArg : aggArgs) {
            newAggArgs.add(aggArg + groupCount);
          }
        }

        newAggCalls.add(aggCall.adaptTo(joinOutputProjRel, newAggArgs,
                aggRel.getGroupCount(), groupCount));
      }

      BitSet groupSet =
          BitSets.range(groupCount);
      AggregateRel newAggRel =
          new AggregateRel(
              cluster,
              joinOutputProjRel,
              groupSet,
              newAggCalls);

      List<RexNode> newAggOutputProjExprList = Lists.newArrayList();
      for (int i : BitSets.toIter(groupSet)) {
        newAggOutputProjExprList.add(
            rexBuilder.makeInputRef(newAggRel, i));
      }

      RexNode newAggOutputProjExpr =
          removeCorrelationExpr(aggOutputProjExprs.get(0), false);
      newAggOutputProjExprList.add(
          rexBuilder.makeCast(
              cluster.getTypeFactory().createTypeWithNullability(
                  newAggOutputProjExpr.getType(),
                  true),
              newAggOutputProjExpr));

      RelNode newAggOutputProjRel =
          RelOptUtil.createProject(
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

  private final class AdjustProjectForCountAggregateRule extends RelOptRule {
    final boolean flavor;

    public AdjustProjectForCountAggregateRule(boolean flavor) {
      super(
          flavor
              ? operand(CorrelatorRel.class,
                  operand(RelNode.class, any()),
                      operand(ProjectRel.class,
                          operand(AggregateRel.class, any())))
              : operand(CorrelatorRel.class,
                  operand(RelNode.class, any()),
                      operand(AggregateRel.class, any())));
      this.flavor = flavor;
    }

    public void onMatch(RelOptRuleCall call) {
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
        List<Pair<RexNode, String>> projects = Lists.newArrayList();
        final List<RelDataTypeField> fields =
            aggRel.getRowType().getFieldList();
        for (int i = 0; i < fields.size(); i++) {
          projects.add(RexInputRef.of2(projects.size(), fields));
        }
        aggOutputProjRel =
            (ProjectRel) RelOptUtil.createProject(
                aggRel,
                projects,
                false);
      }
      onMatch2(call, corRel, leftInputRel, aggOutputProjRel, aggRel);
    }

    private void onMatch2(
        RelOptRuleCall call,
        CorrelatorRel corRel,
        RelNode leftInputRel,
        ProjectRel aggOutputProjRel,
        AggregateRel aggRel) {
      RelOptCluster cluster = corRel.getCluster();

      if (generatedCorRels.contains(corRel)) {
        // This correlator was generated by a previous invocation of
        // this rule. No further work to do.
        return;
      }

      setCurrent(call.getPlanner().getRoot(), corRel);

      // check for this pattern
      // The pattern matching could be simplified if rules can be applied
      // during decorrelation,
      //
      // CorrelateRel(left correlation, condition = true)
      //   LeftInputRel
      //   ProjectRel-A (a RexNode)
      //     AggregateRel (groupby (0), agg0(), agg1()...)

      // check aggOutputProj projects only one expression
      List<RexNode> aggOutputProjExprs = aggOutputProjRel.getProjects();
      if (aggOutputProjExprs.size() != 1) {
        return;
      }

      JoinRelType joinType = corRel.getJoinType();
      RexNode joinCond = corRel.getCondition();
      if ((joinType != JoinRelType.LEFT)
          || (joinCond != rexBuilder.makeLiteral(true))) {
        return;
      }

      // check that the agg is on the entire input
      if (!aggRel.getGroupSet().isEmpty()) {
        return;
      }

      List<AggregateCall> aggCalls = aggRel.getAggCallList();
      Set<Integer> isCount = Sets.newHashSet();

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
      for (Correlation c : Lists.newArrayList(cm.mapCorVarToCorRel.keySet())) {
        if (cm.mapCorVarToCorRel.get(c) == corRel) {
          cm.mapCorVarToCorRel.put(c, newCorRel);
        }
      }

      RelNode newOutputRel =
          aggregateCorrelatorOutput(newCorRel, aggOutputProjRel, isCount);

      call.transformTo(newOutputRel);
    }
  }

  /** A map of the locations of {@link org.eigenbase.rel.CorrelatorRel} in
   * a tree of {@link RelNode}s.
   *
   * <p>It is used to drive the decorrelation process.
   * Treat it as immutable; rebuild if you modify the tree.
   *
   * <p>There are three maps:<ol>
   *
   * <li>mapRefRelToCorVars map a rel node to the correlated variables it
   * references;
   *
   * <li>mapCorVarToCorRel maps a correlated variable to the correlatorRel
   * providing it;
   *
   * <li>mapFieldAccessToCorVar maps a rex field access to
   * the cor var it represents. Because typeFlattener does not clone or
   * modify a correlated field access this map does not need to be
   * updated.
   *
   * </ol> */
  private static class CorelMap {
    private final Multimap<RelNode, Correlation> mapRefRelToCorVar;
    private final SortedMap<Correlation, CorrelatorRel> mapCorVarToCorRel;
    private final Map<RexFieldAccess, Correlation> mapFieldAccessToCorVar;

    // TODO: create immutable copies of all maps
    private CorelMap(Multimap<RelNode, Correlation> mapRefRelToCorVar,
        SortedMap<Correlation, CorrelatorRel> mapCorVarToCorRel,
        Map<RexFieldAccess, Correlation> mapFieldAccessToCorVar) {
      this.mapRefRelToCorVar = mapRefRelToCorVar;
      this.mapCorVarToCorRel = mapCorVarToCorRel;
      this.mapFieldAccessToCorVar = mapFieldAccessToCorVar;
    }

    @Override
    public String toString() {
      return "mapRefRelToCorVar=" + mapRefRelToCorVar
          + "\nmapCorVarToCorRel=" + mapCorVarToCorRel
          + "\nmapFieldAccessToCorVar=" + mapFieldAccessToCorVar
          + "\n";
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof CorelMap
          && mapRefRelToCorVar.equals(((CorelMap) obj).mapRefRelToCorVar)
          && mapCorVarToCorRel.equals(((CorelMap) obj).mapCorVarToCorRel)
          && mapFieldAccessToCorVar.equals(
              ((CorelMap) obj).mapFieldAccessToCorVar);
    }

    @Override public int hashCode() {
      return Util.hashV(mapRefRelToCorVar, mapCorVarToCorRel,
          mapFieldAccessToCorVar);
    }

    /** Creates a CorelMap with given contents. */
    public static CorelMap of(
        SortedSetMultimap<RelNode, Correlation> mapRefRelToCorVar,
        SortedMap<Correlation, CorrelatorRel> mapCorVarToCorRel,
        Map<RexFieldAccess, Correlation> mapFieldAccessToCorVar) {
      return new CorelMap(mapRefRelToCorVar, mapCorVarToCorRel,
          mapFieldAccessToCorVar);
    }

    /** Creates a CorelMap by iterating over a {@link RelNode} tree. */
    public static CorelMap build(RelNode rel) {
      final SortedMap<Correlation, CorrelatorRel> mapCorVarToCorRel =
          new TreeMap<Correlation, CorrelatorRel>();

      final SortedSetMultimap<RelNode, Correlation> mapRefRelToCorVar =
          Multimaps.newSortedSetMultimap(
              Maps.<RelNode, Collection<Correlation>>newHashMap(),
              new Supplier<TreeSet<Correlation>>() {
                public TreeSet<Correlation> get() {
                  Bug.upgrade("use MultimapBuilder when we're on Guava-16");
                  return Sets.newTreeSet();
                }
              });

      final Map<RexFieldAccess, Correlation> mapFieldAccessToCorVar =
          new HashMap<RexFieldAccess, Correlation>();

      final Map<String, Correlation> mapNameToCorVar = Maps.newHashMap();

      final Holder<Integer> offset = Holder.of(0);
      final RelShuttleImpl shuttle = new RelShuttleImpl() {
        @Override
        public RelNode visit(JoinRel join) {
          return visitJoin(join);
        }

        @Override
        protected RelNode visitChild(RelNode parent, int i, RelNode child) {
          return super.visitChild(parent, i, stripHep(child));
        }

        @Override
        public RelNode visit(CorrelatorRel correlator) {
          for (Correlation c : correlator.getCorrelations()) {
            mapNameToCorVar.put("$cor" + c.getId(), c);
            mapCorVarToCorRel.put(c, correlator);
          }
          return visitJoin(correlator);
        }

        private JoinRelBase visitJoin(JoinRelBase join) {
          join.getCondition().accept(rexVisitor(join));
          final int x = offset.get();
          visitChild(join, 0, join.getLeft());
          offset.set(x + join.getLeft().getRowType().getFieldCount());
          visitChild(join, 0, join.getRight());
          offset.set(x);
          return join;
        }

        @Override
        public RelNode visit(final FilterRel filter) {
          filter.getCondition().accept(rexVisitor(filter));
          return super.visit(filter);
        }

        @Override
        public RelNode visit(ProjectRel project) {
          for (RexNode node : project.getProjects()) {
            node.accept(rexVisitor(project));
          }
          return super.visit(project);
        }

        private RexVisitorImpl<Void> rexVisitor(final RelNode rel) {
          return new RexVisitorImpl<Void>(true) {
            @Override
            public Void visitFieldAccess(RexFieldAccess fieldAccess) {
              final RexNode ref = fieldAccess.getReferenceExpr();
              if (ref instanceof RexCorrelVariable) {
                final RexCorrelVariable var = (RexCorrelVariable) ref;
                final Correlation correlation =
                    mapNameToCorVar.get(var.getName());
                mapFieldAccessToCorVar.put(fieldAccess, correlation);
                mapRefRelToCorVar.put(rel, correlation);
              }
              return super.visitFieldAccess(fieldAccess);
            }
          };
        }
      };
      stripHep(rel).accept(shuttle);
      return new CorelMap(mapRefRelToCorVar, mapCorVarToCorRel,
          mapFieldAccessToCorVar);
    }

    private static RelNode stripHep(RelNode rel) {
      if (rel instanceof HepRelVertex) {
        HepRelVertex hepRelVertex = (HepRelVertex) rel;
        rel = hepRelVertex.getCurrentRel();
      }
      return rel;
    }

    /**
     * Returns whether there are any correlating variables in this statement.
     *
     * @return whether there are any correlating variables
     */
    public boolean hasCorrelation() {
      return !mapCorVarToCorRel.isEmpty();
    }
  }
}

// End RelDecorrelator.java
