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
package org.apache.calcite.rel.rules.materialize;

import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.AggregateProjectPullUpConstantsRule;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexTableInputRef.RelTableRef;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilder.AggCall;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Materialized view rewriting for aggregate */
public abstract class MaterializedViewAggregateRule extends MaterializedViewRule {

  protected static final ImmutableList<TimeUnitRange> SUPPORTED_DATE_TIME_ROLLUP_UNITS =
      ImmutableList.of(TimeUnitRange.YEAR, TimeUnitRange.QUARTER, TimeUnitRange.MONTH,
          TimeUnitRange.DAY, TimeUnitRange.HOUR, TimeUnitRange.MINUTE,
          TimeUnitRange.SECOND, TimeUnitRange.MILLISECOND, TimeUnitRange.MICROSECOND);

  //~ Instance fields --------------------------------------------------------

  /** Instance of rule to push filter through project. */
  protected final RelOptRule filterProjectTransposeRule;

  /** Instance of rule to push filter through aggregate. */
  protected final RelOptRule filterAggregateTransposeRule;

  /** Instance of rule to pull up constants into aggregate. */
  protected final RelOptRule aggregateProjectPullUpConstantsRule;

  /** Instance of rule to merge project operators. */
  protected final RelOptRule projectMergeRule;


  /** Creates a MaterializedViewAggregateRule. */
  protected MaterializedViewAggregateRule(RelOptRuleOperand operand,
      RelBuilderFactory relBuilderFactory, String description,
      boolean generateUnionRewriting, HepProgram unionRewritingPullProgram) {
    this(operand, relBuilderFactory, description, generateUnionRewriting,
        unionRewritingPullProgram,
        new FilterProjectTransposeRule(
            Filter.class, Project.class, true, true, relBuilderFactory),
        new FilterAggregateTransposeRule(
            Filter.class, relBuilderFactory, Aggregate.class),
        new AggregateProjectPullUpConstantsRule(
            Aggregate.class, Filter.class, relBuilderFactory, "AggFilterPullUpConstants"),
        new ProjectMergeRule(true, ProjectMergeRule.DEFAULT_BLOAT,
            relBuilderFactory));
  }

  /** Creates a MaterializedViewAggregateRule. */
  protected MaterializedViewAggregateRule(RelOptRuleOperand operand,
      RelBuilderFactory relBuilderFactory, String description,
      boolean generateUnionRewriting, HepProgram unionRewritingPullProgram,
      RelOptRule filterProjectTransposeRule,
      RelOptRule filterAggregateTransposeRule,
      RelOptRule aggregateProjectPullUpConstantsRule,
      RelOptRule projectMergeRule) {
    super(operand, relBuilderFactory, description, generateUnionRewriting,
        unionRewritingPullProgram, false);
    this.filterProjectTransposeRule = filterProjectTransposeRule;
    this.filterAggregateTransposeRule = filterAggregateTransposeRule;
    this.aggregateProjectPullUpConstantsRule = aggregateProjectPullUpConstantsRule;
    this.projectMergeRule = projectMergeRule;
  }

  @Override protected boolean isValidPlan(Project topProject, RelNode node,
      RelMetadataQuery mq) {
    if (!(node instanceof Aggregate)) {
      return false;
    }
    Aggregate aggregate = (Aggregate) node;
    if (aggregate.getGroupType() != Aggregate.Group.SIMPLE) {
      // TODO: Rewriting with grouping sets not supported yet
      return false;
    }
    return isValidRelNodePlan(aggregate.getInput(), mq);
  }

  @Override protected ViewPartialRewriting compensateViewPartial(
      RelBuilder relBuilder,
      RexBuilder rexBuilder,
      RelMetadataQuery mq,
      RelNode input,
      Project topProject,
      RelNode node,
      Set<RelTableRef> queryTableRefs,
      EquivalenceClasses queryEC,
      Project topViewProject,
      RelNode viewNode,
      Set<RelTableRef> viewTableRefs) {
    // Modify view to join with missing tables and add Project on top to reorder columns.
    // In turn, modify view plan to join with missing tables before Aggregate operator,
    // change Aggregate operator to group by previous grouping columns and columns in
    // attached tables, and add a final Project on top.
    // We only need to add the missing tables on top of the view and view plan using
    // a cartesian product.
    // Then the rest of the rewriting algorithm can be executed in the same
    // fashion, and if there are predicates between the existing and missing
    // tables, the rewriting algorithm will enforce them.
    final Set<RelTableRef> extraTableRefs = new HashSet<>();
    for (RelTableRef tRef : queryTableRefs) {
      if (!viewTableRefs.contains(tRef)) {
        // Add to extra tables if table is not part of the view
        extraTableRefs.add(tRef);
      }
    }
    Collection<RelNode> tableScanNodes = mq.getNodeTypes(node).get(TableScan.class);
    List<RelNode> newRels = new ArrayList<>();
    for (RelTableRef tRef : extraTableRefs) {
      int i = 0;
      for (RelNode relNode : tableScanNodes) {
        if (tRef.getQualifiedName().equals(relNode.getTable().getQualifiedName())) {
          if (tRef.getEntityNumber() == i++) {
            newRels.add(relNode);
            break;
          }
        }
      }
    }
    assert extraTableRefs.size() == newRels.size();

    relBuilder.push(input);
    for (RelNode newRel : newRels) {
      // Add to the view
      relBuilder.push(newRel);
      relBuilder.join(JoinRelType.INNER, rexBuilder.makeLiteral(true));
    }
    final RelNode newView = relBuilder.build();

    final Aggregate aggregateViewNode = (Aggregate) viewNode;
    relBuilder.push(aggregateViewNode.getInput());
    int offset = 0;
    for (RelNode newRel : newRels) {
      // Add to the view plan
      relBuilder.push(newRel);
      relBuilder.join(JoinRelType.INNER, rexBuilder.makeLiteral(true));
      offset += newRel.getRowType().getFieldCount();
    }
    // Modify aggregate: add grouping columns
    ImmutableBitSet.Builder groupSet = ImmutableBitSet.builder();
    groupSet.addAll(aggregateViewNode.getGroupSet());
    groupSet.addAll(
        ImmutableBitSet.range(
            aggregateViewNode.getInput().getRowType().getFieldCount(),
            aggregateViewNode.getInput().getRowType().getFieldCount() + offset));
    final Aggregate newViewNode = aggregateViewNode.copy(
        aggregateViewNode.getTraitSet(), relBuilder.build(),
        groupSet.build(), null, aggregateViewNode.getAggCallList());

    relBuilder.push(newViewNode);
    List<RexNode> nodes = new ArrayList<>();
    List<String> fieldNames = new ArrayList<>();
    if (topViewProject != null) {
      // Insert existing expressions (and shift aggregation arguments),
      // then append rest of columns
      Mappings.TargetMapping shiftMapping = Mappings.createShiftMapping(
          newViewNode.getRowType().getFieldCount(),
          0, 0, aggregateViewNode.getGroupCount(),
          newViewNode.getGroupCount(), aggregateViewNode.getGroupCount(),
          aggregateViewNode.getAggCallList().size());
      for (int i = 0; i < topViewProject.getChildExps().size(); i++) {
        nodes.add(
            topViewProject.getChildExps().get(i).accept(
                new RexPermuteInputsShuttle(shiftMapping, newViewNode)));
        fieldNames.add(topViewProject.getRowType().getFieldNames().get(i));
      }
      for (int i = aggregateViewNode.getRowType().getFieldCount();
           i < newViewNode.getRowType().getFieldCount(); i++) {
        int idx = i - aggregateViewNode.getAggCallList().size();
        nodes.add(rexBuilder.makeInputRef(newViewNode, idx));
        fieldNames.add(newViewNode.getRowType().getFieldNames().get(idx));
      }
    } else {
      // Original grouping columns, aggregation columns, then new grouping columns
      for (int i = 0; i < newViewNode.getRowType().getFieldCount(); i++) {
        int idx;
        if (i < aggregateViewNode.getGroupCount()) {
          idx = i;
        } else if (i < aggregateViewNode.getRowType().getFieldCount()) {
          idx = i + offset;
        } else {
          idx = i - aggregateViewNode.getAggCallList().size();
        }
        nodes.add(rexBuilder.makeInputRef(newViewNode, idx));
        fieldNames.add(newViewNode.getRowType().getFieldNames().get(idx));
      }
    }
    relBuilder.project(nodes, fieldNames, true);
    final Project newTopViewProject = (Project) relBuilder.build();

    return ViewPartialRewriting.of(newView, newTopViewProject, newViewNode);
  }

  @Override protected RelNode rewriteQuery(
      RelBuilder relBuilder,
      RexBuilder rexBuilder,
      RexSimplify simplify,
      RelMetadataQuery mq,
      RexNode compensationColumnsEquiPred,
      RexNode otherCompensationPred,
      Project topProject,
      RelNode node,
      BiMap<RelTableRef, RelTableRef> queryToViewTableMapping,
      EquivalenceClasses viewEC, EquivalenceClasses queryEC) {
    Aggregate aggregate = (Aggregate) node;

    // Our target node is the node below the root, which should have the maximum
    // number of available expressions in the tree in order to maximize our
    // number of rewritings.
    // If the program is available, we execute it to maximize rewriting opportunities.
    // For instance, a program might pull up all the expressions that are below the
    // aggregate so we can introduce compensation filters easily. This is important
    // depending on the planner strategy.
    RelNode newAggregateInput = aggregate.getInput(0);
    RelNode target = aggregate.getInput(0);
    if (unionRewritingPullProgram != null) {
      final HepPlanner tmpPlanner = new HepPlanner(unionRewritingPullProgram);
      tmpPlanner.setRoot(newAggregateInput);
      newAggregateInput = tmpPlanner.findBestExp();
      target = newAggregateInput.getInput(0);
    }

    // We need to check that all columns required by compensating predicates
    // are contained in the query.
    List<RexNode> queryExprs = extractReferences(rexBuilder, target);
    if (!compensationColumnsEquiPred.isAlwaysTrue()) {
      compensationColumnsEquiPred = rewriteExpression(rexBuilder, mq,
          target, target, queryExprs, queryToViewTableMapping, queryEC, false,
          compensationColumnsEquiPred);
      if (compensationColumnsEquiPred == null) {
        // Skip it
        return null;
      }
    }
    // For the rest, we use the query equivalence classes
    if (!otherCompensationPred.isAlwaysTrue()) {
      otherCompensationPred = rewriteExpression(rexBuilder, mq,
          target, target, queryExprs, queryToViewTableMapping, viewEC, true,
          otherCompensationPred);
      if (otherCompensationPred == null) {
        // Skip it
        return null;
      }
    }
    final RexNode queryCompensationPred = RexUtil.not(
        RexUtil.composeConjunction(rexBuilder,
            ImmutableList.of(compensationColumnsEquiPred,
                otherCompensationPred)));

    // Generate query rewriting.
    RelNode rewrittenPlan = relBuilder
        .push(target)
        .filter(simplify.simplifyUnknownAsFalse(queryCompensationPred))
        .build();
    if (unionRewritingPullProgram != null) {
      return aggregate.copy(aggregate.getTraitSet(),
          ImmutableList.of(
              newAggregateInput.copy(newAggregateInput.getTraitSet(),
                  ImmutableList.of(rewrittenPlan))));
    }
    return aggregate.copy(aggregate.getTraitSet(), ImmutableList.of(rewrittenPlan));
  }

  @Override protected RelNode createUnion(RelBuilder relBuilder, RexBuilder rexBuilder,
      RelNode topProject, RelNode unionInputQuery, RelNode unionInputView) {
    // Union
    relBuilder.push(unionInputQuery);
    relBuilder.push(unionInputView);
    relBuilder.union(true);
    List<RexNode> exprList = new ArrayList<>(relBuilder.peek().getRowType().getFieldCount());
    List<String> nameList = new ArrayList<>(relBuilder.peek().getRowType().getFieldCount());
    for (int i = 0; i < relBuilder.peek().getRowType().getFieldCount(); i++) {
      // We can take unionInputQuery as it is query based.
      RelDataTypeField field = unionInputQuery.getRowType().getFieldList().get(i);
      exprList.add(
          rexBuilder.ensureType(
              field.getType(),
              rexBuilder.makeInputRef(relBuilder.peek(), i),
              true));
      nameList.add(field.getName());
    }
    relBuilder.project(exprList, nameList);
    // Rollup aggregate
    Aggregate aggregate = (Aggregate) unionInputQuery;
    final ImmutableBitSet groupSet = ImmutableBitSet.range(aggregate.getGroupCount());
    final List<AggCall> aggregateCalls = new ArrayList<>();
    for (int i = 0; i < aggregate.getAggCallList().size(); i++) {
      AggregateCall aggCall = aggregate.getAggCallList().get(i);
      if (aggCall.isDistinct()) {
        // Cannot ROLLUP distinct
        return null;
      }
      SqlAggFunction rollupAgg =
          getRollup(aggCall.getAggregation());
      if (rollupAgg == null) {
        // Cannot rollup this aggregate, bail out
        return null;
      }
      final RexInputRef operand =
          rexBuilder.makeInputRef(relBuilder.peek(),
              aggregate.getGroupCount() + i);
      aggregateCalls.add(
          relBuilder.aggregateCall(rollupAgg, operand)
              .distinct(aggCall.isDistinct())
              .approximate(aggCall.isApproximate())
              .as(aggCall.name));
    }
    RelNode prevNode = relBuilder.peek();
    RelNode result = relBuilder
        .aggregate(relBuilder.groupKey(groupSet), aggregateCalls)
        .build();
    if (prevNode == result && groupSet.cardinality() != result.getRowType().getFieldCount()) {
      // Aggregate was not inserted but we need to prune columns
      result = relBuilder
          .push(result)
          .project(relBuilder.fields(groupSet))
          .build();
    }
    if (topProject != null) {
      // Top project
      return topProject.copy(topProject.getTraitSet(), ImmutableList.of(result));
    }
    // Result
    return result;
  }

  @Override protected RelNode rewriteView(
      RelBuilder relBuilder,
      RexBuilder rexBuilder,
      RexSimplify simplify,
      RelMetadataQuery mq,
      MatchModality matchModality,
      boolean unionRewriting,
      RelNode input,
      Project topProject,
      RelNode node,
      Project topViewProject,
      RelNode viewNode,
      BiMap<RelTableRef, RelTableRef> queryToViewTableMapping,
      EquivalenceClasses queryEC) {
    final Aggregate queryAggregate = (Aggregate) node;
    final Aggregate viewAggregate = (Aggregate) viewNode;
    // Get group by references and aggregate call input references needed
    ImmutableBitSet.Builder indexes = ImmutableBitSet.builder();
    ImmutableBitSet references = null;
    if (topProject != null && !unionRewriting) {
      // We have a Project on top, gather only what is needed
      final RelOptUtil.InputFinder inputFinder =
          new RelOptUtil.InputFinder(new LinkedHashSet<>());
      inputFinder.visitEach(topProject.getChildExps());
      references = inputFinder.build();
      for (int i = 0; i < queryAggregate.getGroupCount(); i++) {
        indexes.set(queryAggregate.getGroupSet().nth(i));
      }
      for (int i = 0; i < queryAggregate.getAggCallList().size(); i++) {
        if (references.get(queryAggregate.getGroupCount() + i)) {
          for (int inputIdx : queryAggregate.getAggCallList().get(i).getArgList()) {
            indexes.set(inputIdx);
          }
        }
      }
    } else {
      // No project on top, all of them are needed
      for (int i = 0; i < queryAggregate.getGroupCount(); i++) {
        indexes.set(queryAggregate.getGroupSet().nth(i));
      }
      for (AggregateCall queryAggCall : queryAggregate.getAggCallList()) {
        for (int inputIdx : queryAggCall.getArgList()) {
          indexes.set(inputIdx);
        }
      }
    }

    // Create mapping from query columns to view columns
    List<RexNode> rollupNodes = new ArrayList<>();
    Multimap<Integer, Integer> m = generateMapping(rexBuilder, simplify, mq,
        queryAggregate.getInput(), viewAggregate.getInput(), indexes.build(),
        queryToViewTableMapping, queryEC, rollupNodes);
    if (m == null) {
      // Bail out
      return null;
    }

    // We could map all expressions. Create aggregate mapping.
    int viewAggregateAdditionalFieldCount = rollupNodes.size();
    int viewInputFieldCount = viewAggregate.getInput().getRowType().getFieldCount();
    int viewInputDifferenceViewFieldCount =
        viewAggregate.getRowType().getFieldCount() - viewInputFieldCount;
    int viewAggregateTotalFieldCount =
        viewAggregate.getRowType().getFieldCount() + rollupNodes.size();
    boolean forceRollup = false;
    Mapping aggregateMapping = Mappings.create(MappingType.FUNCTION,
        queryAggregate.getRowType().getFieldCount(), viewAggregateTotalFieldCount);
    for (int i = 0; i < queryAggregate.getGroupCount(); i++) {
      Collection<Integer> c = m.get(queryAggregate.getGroupSet().nth(i));
      for (int j : c) {
        if (j >= viewAggregate.getInput().getRowType().getFieldCount()) {
          // This is one of the rollup columns
          aggregateMapping.set(i, j + viewInputDifferenceViewFieldCount);
          forceRollup = true;
        } else {
          int targetIdx = viewAggregate.getGroupSet().indexOf(j);
          if (targetIdx == -1) {
            continue;
          }
          aggregateMapping.set(i, targetIdx);
        }
        break;
      }
      if (aggregateMapping.getTargetOpt(i) == -1) {
        // It is not part of group by, we bail out
        return null;
      }
    }
    boolean containsDistinctAgg = false;
    for (int idx = 0; idx < queryAggregate.getAggCallList().size(); idx++) {
      if (references != null && !references.get(queryAggregate.getGroupCount() + idx)) {
        // Ignore
        continue;
      }
      AggregateCall queryAggCall = queryAggregate.getAggCallList().get(idx);
      if (queryAggCall.filterArg >= 0) {
        // Not supported currently
        return null;
      }
      List<Integer> queryAggCallIndexes = new ArrayList<>();
      for (int aggCallIdx : queryAggCall.getArgList()) {
        queryAggCallIndexes.add(m.get(aggCallIdx).iterator().next());
      }
      for (int j = 0; j < viewAggregate.getAggCallList().size(); j++) {
        AggregateCall viewAggCall = viewAggregate.getAggCallList().get(j);
        if (queryAggCall.getAggregation().getKind() != viewAggCall.getAggregation().getKind()
            || queryAggCall.isDistinct() != viewAggCall.isDistinct()
            || queryAggCall.getArgList().size() != viewAggCall.getArgList().size()
            || queryAggCall.getType() != viewAggCall.getType()
            || viewAggCall.filterArg >= 0) {
          // Continue
          continue;
        }
        if (!queryAggCallIndexes.equals(viewAggCall.getArgList())) {
          // Continue
          continue;
        }
        aggregateMapping.set(queryAggregate.getGroupCount() + idx,
            viewAggregate.getGroupCount() + j);
        if (queryAggCall.isDistinct()) {
          containsDistinctAgg = true;
        }
        break;
      }
    }

    // If we reach here, to simplify things, we create an identity topViewProject
    // if not present
    if (topViewProject == null) {
      topViewProject = (Project) relBuilder.push(viewNode)
          .project(relBuilder.fields(), ImmutableList.of(), true).build();
    }

    // Generate result rewriting
    final List<RexNode> additionalViewExprs = new ArrayList<>();

    // Multimap is required since a column in the materialized view's project
    // could map to multiple columns in the target query
    ImmutableMultimap<Integer, Integer> rewritingMapping = null;
    RelNode result = relBuilder.push(input).build();
    // We create view expressions that will be used in a Project on top of the
    // view in case we need to rollup the expression
    final List<RexNode> inputViewExprs = new ArrayList<>();
    inputViewExprs.addAll(relBuilder.push(result).fields());
    relBuilder.clear();
    if (forceRollup || queryAggregate.getGroupCount() != viewAggregate.getGroupCount()
        || matchModality == MatchModality.VIEW_PARTIAL) {
      if (containsDistinctAgg) {
        // Cannot rollup DISTINCT aggregate
        return null;
      }
      // Target is coarser level of aggregation. Generate an aggregate.
      final ImmutableMultimap.Builder<Integer, Integer> rewritingMappingB =
          ImmutableMultimap.builder();
      final ImmutableBitSet.Builder groupSetB = ImmutableBitSet.builder();
      for (int i = 0; i < queryAggregate.getGroupCount(); i++) {
        int targetIdx = aggregateMapping.getTargetOpt(i);
        if (targetIdx == -1) {
          // No matching group by column, we bail out
          return null;
        }
        boolean added = false;
        if (targetIdx >= viewAggregate.getRowType().getFieldCount()) {
          RexNode targetNode = rollupNodes.get(
              targetIdx - viewInputFieldCount - viewInputDifferenceViewFieldCount);
          // We need to rollup this expression
          final Multimap<RexNode, Integer> exprsLineage = ArrayListMultimap.create();
          final ImmutableBitSet refs = RelOptUtil.InputFinder.bits(targetNode);
          for (int childTargetIdx : refs) {
            added = false;
            for (int k = 0; k < topViewProject.getChildExps().size() && !added; k++) {
              RexNode n = topViewProject.getChildExps().get(k);
              if (!n.isA(SqlKind.INPUT_REF)) {
                continue;
              }
              final int ref = ((RexInputRef) n).getIndex();
              if (ref == childTargetIdx) {
                exprsLineage.put(
                    new RexInputRef(ref, targetNode.getType()), k);
                added = true;
              }
            }
            if (!added) {
              // No matching column needed for computed expression, bail out
              return null;
            }
          }
          // We create the new node pointing to the index
          groupSetB.set(inputViewExprs.size());
          rewritingMappingB.put(inputViewExprs.size(), i);
          additionalViewExprs.add(
              new RexInputRef(targetIdx, targetNode.getType()));
          // We need to create the rollup expression
          inputViewExprs.add(
              shuttleReferences(rexBuilder, targetNode, exprsLineage));
          added = true;
        } else {
          // This expression should be referenced directly
          for (int k = 0; k < topViewProject.getChildExps().size() && !added; k++) {
            RexNode n = topViewProject.getChildExps().get(k);
            if (!n.isA(SqlKind.INPUT_REF)) {
              continue;
            }
            int ref = ((RexInputRef) n).getIndex();
            if (ref == targetIdx) {
              groupSetB.set(k);
              rewritingMappingB.put(k, i);
              added = true;
            }
          }
        }
        if (!added) {
          // No matching group by column, we bail out
          return null;
        }
      }
      final ImmutableBitSet groupSet = groupSetB.build();
      final List<AggCall> aggregateCalls = new ArrayList<>();
      for (int i = 0; i < queryAggregate.getAggCallList().size(); i++) {
        if (references != null && !references.get(queryAggregate.getGroupCount() + i)) {
          // Ignore
          continue;
        }
        int sourceIdx = queryAggregate.getGroupCount() + i;
        int targetIdx =
            aggregateMapping.getTargetOpt(sourceIdx);
        if (targetIdx < 0) {
          // No matching aggregation column, we bail out
          return null;
        }
        AggregateCall queryAggCall = queryAggregate.getAggCallList().get(i);
        boolean added = false;
        for (int k = 0; k < topViewProject.getChildExps().size() && !added; k++) {
          RexNode n = topViewProject.getChildExps().get(k);
          if (!n.isA(SqlKind.INPUT_REF)) {
            continue;
          }
          int ref = ((RexInputRef) n).getIndex();
          if (ref == targetIdx) {
            SqlAggFunction rollupAgg =
                getRollup(queryAggCall.getAggregation());
            if (rollupAgg == null) {
              // Cannot rollup this aggregate, bail out
              return null;
            }
            rewritingMappingB.put(k, queryAggregate.getGroupCount() + aggregateCalls.size());
            final RexInputRef operand = rexBuilder.makeInputRef(input, k);
            aggregateCalls.add(
                relBuilder.aggregateCall(rollupAgg, operand)
                    .approximate(queryAggCall.isApproximate())
                    .distinct(queryAggCall.isDistinct())
                    .as(queryAggCall.name));
            added = true;
          }
        }
        if (!added) {
          // No matching aggregation column, we bail out
          return null;
        }
      }
      // Create aggregate on top of input
      RelNode prevNode = result;
      relBuilder.push(result);
      if (inputViewExprs.size() != result.getRowType().getFieldCount()) {
        relBuilder.project(inputViewExprs);
      }
      result = relBuilder
          .aggregate(relBuilder.groupKey(groupSet), aggregateCalls)
          .build();
      if (prevNode == result && groupSet.cardinality() != result.getRowType().getFieldCount()) {
        // Aggregate was not inserted but we need to prune columns
        result = relBuilder
            .push(result)
            .project(relBuilder.fields(groupSet))
            .build();
      }
      // We introduce a project on top, as group by columns order is lost
      rewritingMapping = rewritingMappingB.build();
      final ImmutableMultimap<Integer, Integer> inverseMapping = rewritingMapping.inverse();
      final List<RexNode> projects = new ArrayList<>();

      final ImmutableBitSet.Builder addedProjects = ImmutableBitSet.builder();
      for (int i = 0; i < queryAggregate.getGroupCount(); i++) {
        int pos = groupSet.indexOf(inverseMapping.get(i).iterator().next());
        addedProjects.set(pos);
        projects.add(
            rexBuilder.makeInputRef(result, pos));
      }

      ImmutableBitSet projectedCols = addedProjects.build();
      // We add aggregate functions that are present in result to projection list
      for (int i = 0; i < result.getRowType().getFieldCount(); i++) {
        if (!projectedCols.get(i)) {
          projects.add(rexBuilder.makeInputRef(result, i));
        }
      }
      result = relBuilder
          .push(result)
          .project(projects)
          .build();
    } // end if queryAggregate.getGroupCount() != viewAggregate.getGroupCount()

    // Add query expressions on top. We first map query expressions to view
    // expressions. Once we have done that, if the expression is contained
    // and we have introduced already an operator on top of the input node,
    // we use the mapping to resolve the position of the expression in the
    // node.
    final RelDataType topRowType;
    final List<RexNode> topExprs = new ArrayList<>();
    if (topProject != null && !unionRewriting) {
      topExprs.addAll(topProject.getChildExps());
      topRowType = topProject.getRowType();
    } else {
      // Add all
      for (int pos = 0; pos < queryAggregate.getRowType().getFieldCount(); pos++) {
        topExprs.add(rexBuilder.makeInputRef(queryAggregate, pos));
      }
      topRowType = queryAggregate.getRowType();
    }
    // Available in view.
    final Multimap<RexNode, Integer> viewExprs = ArrayListMultimap.create();
    int numberViewExprs = 0;
    for (RexNode viewExpr : topViewProject.getChildExps()) {
      viewExprs.put(viewExpr, numberViewExprs++);
    }
    for (RexNode additionalViewExpr : additionalViewExprs) {
      viewExprs.put(additionalViewExpr, numberViewExprs++);
    }
    final List<RexNode> rewrittenExprs = new ArrayList<>(topExprs.size());
    for (RexNode expr : topExprs) {
      // First map through the aggregate
      RexNode rewrittenExpr = shuttleReferences(rexBuilder, expr, aggregateMapping);
      if (rewrittenExpr == null) {
        // Cannot map expression
        return null;
      }
      // Next map through the last project
      rewrittenExpr =
          shuttleReferences(rexBuilder, rewrittenExpr, viewExprs, result, rewritingMapping);
      if (rewrittenExpr == null) {
        // Cannot map expression
        return null;
      }
      rewrittenExprs.add(rewrittenExpr);
    }
    return relBuilder
        .push(result)
        .project(rewrittenExprs)
        .convert(topRowType, false)
        .build();
  }

  /**
   * Mapping from node expressions to target expressions.
   *
   * <p>If any of the expressions cannot be mapped, we return null.
   */
  protected Multimap<Integer, Integer> generateMapping(
      RexBuilder rexBuilder,
      RexSimplify simplify,
      RelMetadataQuery mq,
      RelNode node,
      RelNode target,
      ImmutableBitSet positions,
      BiMap<RelTableRef, RelTableRef> tableMapping,
      EquivalenceClasses sourceEC,
      List<RexNode> additionalExprs) {
    Preconditions.checkArgument(additionalExprs.isEmpty());
    Multimap<Integer, Integer> m = ArrayListMultimap.create();
    Map<RexTableInputRef, Set<RexTableInputRef>> equivalenceClassesMap =
        sourceEC.getEquivalenceClassesMap();
    Multimap<RexNode, Integer> exprsLineage = ArrayListMultimap.create();
    List<RexNode> timestampExprs = new ArrayList<>();
    for (int i = 0; i < target.getRowType().getFieldCount(); i++) {
      Set<RexNode> s = mq.getExpressionLineage(target, rexBuilder.makeInputRef(target, i));
      if (s == null) {
        // Bail out
        continue;
      }
      // We only support project - filter - join, thus it should map to
      // a single expression
      final RexNode e = Iterables.getOnlyElement(s);
      // Rewrite expr to be expressed on query tables
      final RexNode simplified = simplify.simplifyUnknownAsFalse(e);
      RexNode expr = RexUtil.swapTableColumnReferences(rexBuilder,
          simplified,
          tableMapping.inverse(),
          equivalenceClassesMap);
      exprsLineage.put(expr, i);
      SqlTypeName sqlTypeName = expr.getType().getSqlTypeName();
      if (sqlTypeName == SqlTypeName.TIMESTAMP
          || sqlTypeName == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
        timestampExprs.add(expr);
      }
    }

    // If this is a column of TIMESTAMP (WITH LOCAL TIME ZONE)
    // type, we add the possible rollup columns too.
    // This way we will be able to match FLOOR(ts to HOUR) to
    // FLOOR(ts to DAY) via FLOOR(FLOOR(ts to HOUR) to DAY)
    for (RexNode timestampExpr : timestampExprs) {
      for (TimeUnitRange value : SUPPORTED_DATE_TIME_ROLLUP_UNITS) {
        // CEIL
        RexNode ceilExpr =
            rexBuilder.makeCall(getCeilSqlFunction(value),
                timestampExpr, rexBuilder.makeFlag(value));
        // References self-row
        RexNode rewrittenCeilExpr =
            shuttleReferences(rexBuilder, ceilExpr, exprsLineage);
        if (rewrittenCeilExpr != null) {
          // We add the CEIL expression to the additional expressions, replacing the child
          // expression by the position that it references
          additionalExprs.add(rewrittenCeilExpr);
          // Then we simplify the expression and we add it to the expressions lineage so we
          // can try to find a match
          final RexNode simplified =
              simplify.simplifyUnknownAsFalse(ceilExpr);
          exprsLineage.put(simplified,
              target.getRowType().getFieldCount() + additionalExprs.size() - 1);
        }
        // FLOOR
        RexNode floorExpr =
            rexBuilder.makeCall(getFloorSqlFunction(value),
                timestampExpr, rexBuilder.makeFlag(value));
        // References self-row
        RexNode rewrittenFloorExpr =
            shuttleReferences(rexBuilder, floorExpr, exprsLineage);
        if (rewrittenFloorExpr != null) {
          // We add the FLOOR expression to the additional expressions, replacing the child
          // expression by the position that it references
          additionalExprs.add(rewrittenFloorExpr);
          // Then we simplify the expression and we add it to the expressions lineage so we
          // can try to find a match
          final RexNode simplified =
              simplify.simplifyUnknownAsFalse(floorExpr);
          exprsLineage.put(simplified,
              target.getRowType().getFieldCount() + additionalExprs.size() - 1);
        }
      }
    }

    for (int i : positions) {
      Set<RexNode> s = mq.getExpressionLineage(node, rexBuilder.makeInputRef(node, i));
      if (s == null) {
        // Bail out
        return null;
      }
      // We only support project - filter - join, thus it should map to
      // a single expression
      final RexNode e = Iterables.getOnlyElement(s);
      // Rewrite expr to be expressed on query tables
      final RexNode simplified = simplify.simplifyUnknownAsFalse(e);
      RexNode targetExpr = RexUtil.swapColumnReferences(rexBuilder,
          simplified, equivalenceClassesMap);
      final Collection<Integer> c = exprsLineage.get(targetExpr);
      if (!c.isEmpty()) {
        for (Integer j : c) {
          m.put(i, j);
        }
      } else {
        // If we did not find the expression, try to navigate it
        RexNode rewrittenTargetExpr =
            shuttleReferences(rexBuilder, targetExpr, exprsLineage);
        if (rewrittenTargetExpr == null) {
          // Some expressions were not present
          return null;
        }
        m.put(i, target.getRowType().getFieldCount() + additionalExprs.size());
        additionalExprs.add(rewrittenTargetExpr);
      }
    }
    return m;
  }

  /**
   * Get ceil function datetime.
   */
  protected SqlFunction getCeilSqlFunction(TimeUnitRange flag) {
    return SqlStdOperatorTable.CEIL;
  }

  /**
   * Get floor function datetime.
   */
  protected SqlFunction getFloorSqlFunction(TimeUnitRange flag) {
    return SqlStdOperatorTable.FLOOR;
  }

  /**
   * Get rollup aggregation function.
   */
  protected SqlAggFunction getRollup(SqlAggFunction aggregation) {
    if (aggregation == SqlStdOperatorTable.SUM
        || aggregation == SqlStdOperatorTable.MIN
        || aggregation == SqlStdOperatorTable.MAX
        || aggregation == SqlStdOperatorTable.SUM0
        || aggregation == SqlStdOperatorTable.ANY_VALUE) {
      return aggregation;
    } else if (aggregation == SqlStdOperatorTable.COUNT) {
      return SqlStdOperatorTable.SUM0;
    } else {
      return null;
    }
  }

  @Override public Pair<RelNode, RelNode> pushFilterToOriginalViewPlan(RelBuilder builder,
      RelNode topViewProject, RelNode viewNode, RexNode cond) {
    // We add (and push) the filter to the view plan before triggering the rewriting.
    // This is useful in case some of the columns can be folded to same value after
    // filter is added.
    HepProgramBuilder pushFiltersProgram = new HepProgramBuilder();
    if (topViewProject != null) {
      pushFiltersProgram.addRuleInstance(filterProjectTransposeRule);
    }
    pushFiltersProgram
        .addRuleInstance(this.filterAggregateTransposeRule)
        .addRuleInstance(this.aggregateProjectPullUpConstantsRule)
        .addRuleInstance(this.projectMergeRule);
    final HepPlanner tmpPlanner = new HepPlanner(pushFiltersProgram.build());
    // Now that the planner is created, push the node
    RelNode topNode = builder
        .push(topViewProject != null ? topViewProject : viewNode)
        .filter(cond).build();
    tmpPlanner.setRoot(topNode);
    topNode = tmpPlanner.findBestExp();
    RelNode resultTopViewProject = null;
    RelNode resultViewNode = null;
    while (topNode != null) {
      if (topNode instanceof Project) {
        if (resultTopViewProject != null) {
          // Both projects could not be merged, we will bail out
          return Pair.of(topViewProject, viewNode);
        }
        resultTopViewProject = topNode;
        topNode = topNode.getInput(0);
      } else if (topNode instanceof Aggregate) {
        resultViewNode = topNode;
        topNode = null;
      } else {
        // We move to the child
        topNode = topNode.getInput(0);
      }
    }
    return Pair.of(resultTopViewProject, resultViewNode);
  }
}
