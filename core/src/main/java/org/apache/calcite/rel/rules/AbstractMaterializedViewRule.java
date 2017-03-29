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
package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptMaterializations;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.SubstitutionVisitor;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexTableInputRef.RelTableRef;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilder.AggCall;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.IntPair;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.calcite.util.trace.CalciteLogger;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;

import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Planner rule that converts a {@link org.apache.calcite.rel.core.Project}
 * followed by {@link org.apache.calcite.rel.core.Aggregate} or an
 * {@link org.apache.calcite.rel.core.Aggregate} to a scan (and possibly
 * other operations) over a materialized view.
 */
public abstract class AbstractMaterializedViewRule extends RelOptRule {

  private static final CalciteLogger LOGGER =
      new CalciteLogger(LoggerFactory.getLogger(AbstractMaterializedViewRule.class));

  public static final MaterializedViewProjectJoinRule INSTANCE_PROJECT_JOIN =
      new MaterializedViewProjectJoinRule(RelFactories.LOGICAL_BUILDER);

  public static final MaterializedViewOnlyJoinRule INSTANCE_JOIN =
      new MaterializedViewOnlyJoinRule(RelFactories.LOGICAL_BUILDER);

  public static final MaterializedViewProjectAggregateRule INSTANCE_PROJECT_AGGREGATE =
      new MaterializedViewProjectAggregateRule(RelFactories.LOGICAL_BUILDER);

  public static final MaterializedViewOnlyAggregateRule INSTANCE_AGGREGATE =
      new MaterializedViewOnlyAggregateRule(RelFactories.LOGICAL_BUILDER);

  //~ Constructors -----------------------------------------------------------

  /** Creates a AbstractMaterializedViewRule. */
  protected AbstractMaterializedViewRule(RelOptRuleOperand operand,
      RelBuilderFactory relBuilderFactory, String description) {
    super(operand, relBuilderFactory, description);
  }

  /**
   * Rewriting logic is based on "Optimizing Queries Using Materialized Views:
   * A Practical, Scalable Solution" by Goldstein and Larson.
   *
   * On the query side, rules matches a Project-node chain or node, where node
   * is either an Aggregate or a Join. Subplan rooted at the node operator must
   * be composed of one or more of the following operators: TableScan, Project,
   * Filter, and Join.
   *
   * For each join MV, we need to check the following:
   * 1) The plan rooted at the Join operator in the view produces all rows
   * needed by the plan rooted at the Join operator in the query.
   * 2) All columns required by compensating predicates, i.e., predicates that
   * need to be enforced over the view, are available at the view output.
   * 3) All output expressions can be computed from the output of the view.
   * 4) All output rows occur with the correct duplication factor.
   * TODO: Currently we only allow the same tables in the view and the query,
   * thus we are sure condition 4 is met. This restriction will be lifted in
   * the future.
   *
   * In turn, for each aggregate MV, we need to check the following:
   * 1) The plan rooted at the Aggregate operator in the view produces all rows
   * needed by the plan rooted at the Aggregate operator in the query.
   * 2) All columns required by compensating predicates, i.e., predicates that
   * need to be enforced over the view, are available at the view output.
   * 3) The grouping columns in the query are a subset of the grouping columns
   * in the view.
   * 4) All columns required to perform further grouping are available in the
   * view output.
   * 5) All columns required to compute output expressions are available in the
   * view output.
   */
  protected void perform(RelOptRuleCall call, Project topProject, RelNode node) {
    final RexBuilder rexBuilder = node.getCluster().getRexBuilder();
    final RelMetadataQuery mq = RelMetadataQuery.instance();
    final RelOptPlanner planner = call.getPlanner();
    final RexSimplify simplify =
        new RexSimplify(rexBuilder, true,
            planner.getExecutor() != null ? planner.getExecutor() : RexUtil.EXECUTOR);

    final List<RelOptMaterialization> materializations =
        (planner instanceof VolcanoPlanner)
            ? ((VolcanoPlanner) planner).getMaterializations()
            : ImmutableList.<RelOptMaterialization>of();

    if (!materializations.isEmpty()) {
      // 1. Explore query plan to recognize whether preconditions to
      // try to generate a rewriting are met
      if (!isValidPlan(topProject, node, mq)) {
        return;
      }

      // Obtain applicable (filtered) materializations
      // TODO: Filtering of relevant materializations needs to be
      // improved so we gather only materializations that might
      // actually generate a valid rewriting.
      final List<RelOptMaterialization> applicableMaterializations =
          RelOptMaterializations.getApplicableMaterializations(node, materializations);

      if (!applicableMaterializations.isEmpty()) {
        // 2. Initialize all query related auxiliary data structures
        // that will be used throughout query rewriting process
        final Multiset<RelOptTable> qTableBag = ImmutableMultiset.copyOf(
            RelOptUtil.findAllTables(node));

        // Generate query table references
        final Set<RelTableRef> queryTableRefs = mq.getTableReferences(node);
        if (queryTableRefs == null) {
          // Bail out
          return;
        }

        // Extract query predicates
        final RelOptPredicateList queryPredicateList =
            mq.getAllPredicates(node);
        if (queryPredicateList == null) {
          // Bail out
          return;
        }
        final RexNode[] queryPreds = splitPredicates(
            rexBuilder, queryPredicateList.pulledUpPredicates);

        // Extract query equivalence classes. An equivalence class is a set
        // of columns in the query output that are known to be equal.
        final EquivalenceClasses qEC = new EquivalenceClasses();
        for (RexNode conj : RelOptUtil.conjunctions(queryPreds[0])) {
          assert conj.isA(SqlKind.EQUALS);
          RexCall equiCond = (RexCall) conj;
          qEC.addEquivalenceClass(
              (RexTableInputRef) equiCond.getOperands().get(0),
              (RexTableInputRef) equiCond.getOperands().get(1));
        }

        // 3. We iterate through all applicable materializations trying to
        // rewrite the given query
        for (RelOptMaterialization materialization : applicableMaterializations) {
          final Project topViewProject;
          final RelNode viewNode;
          // 3.1. Check whether it is a valid view
          if (!isViewMatching(materialization.queryRel)) {
            // Skip it
            continue;
          }
          if (materialization.queryRel instanceof Project) {
            topViewProject = (Project) materialization.queryRel;
            viewNode = topViewProject.getInput();
          } else {
            topViewProject = null;
            viewNode = materialization.queryRel;
          }

          // 3.2. View checks before proceeding
          if (!isValidPlan(topViewProject, viewNode, mq)) {
            // Skip it
            continue;
          }

          // 3.3. Initialize all query related auxiliary data structures
          // that will be used throughout query rewriting process
          // Extract view tables
          Multiset<RelOptTable> vTableBag = ImmutableMultiset.copyOf(
              RelOptUtil.findAllTables(viewNode));
          if (!qTableBag.equals(vTableBag)) {
            // Currently we only support rewriting with views that use
            // the same set of tables than the query, thus we skip it
            // TODO: Extend to lift this restriction
            continue;
          }

          // Extract view predicates
          final RelOptPredicateList viewPredicateList =
              mq.getAllPredicates(viewNode);
          if (viewPredicateList == null) {
            // Skip it
            continue;
          }
          final RexNode[] viewPreds = splitPredicates(
              rexBuilder, viewPredicateList.pulledUpPredicates);

          // Extract view table references
          final Set<RelTableRef> viewTableRefs = mq.getTableReferences(viewNode);
          if (viewTableRefs == null) {
            // Bail out
            return;
          }

          // 4. We map every table in the query to a view table with the same qualified
          // name.
          final Multimap<RelTableRef, RelTableRef> multiMapTables = ArrayListMultimap.create();
          for (RelTableRef queryTableRef : queryTableRefs) {
            for (RelTableRef viewTableRef : viewTableRefs) {
              if (queryTableRef.getQualifiedName().equals(
                  viewTableRef.getQualifiedName())) {
                multiMapTables.put(queryTableRef, viewTableRef);
              }
            }
          }

          // If a table is used multiple times, we will create multiple mappings,
          // and we will try to rewrite the query using each of the mappings.
          // Then, we will try to map every source table (query) to a target
          // table (view), and if we are successful, we will try to create
          // compensation predicates to filter the view results further
          // (if needed).
          final List<BiMap<RelTableRef, RelTableRef>> flatListMappings =
              generateTableMappings(multiMapTables);
          for (BiMap<RelTableRef, RelTableRef> tableMapping : flatListMappings) {
            final RexNode compensationColumnsEquiPred;
            final RexNode compensationRangePred;
            final RexNode compensationResidualPred;

            // 4.1. Establish relationship between view and query equivalence classes.
            // If every view equivalence class is not a subset of a query
            // equivalence class, we bail out.
            // To establish relationship, we swap column references of the view predicates
            // to point to query tables. Then, we create the equivalence classes for the
            // view predicates and check that every view equivalence class is a subset of a
            // query equivalence class: if it is not, we bail out.
            final RexNode viewColumnsEquiPred = RexUtil.swapTableReferences(
                rexBuilder, viewPreds[0], tableMapping.inverse());
            final EquivalenceClasses vEC = new EquivalenceClasses();
            for (RexNode conj : RelOptUtil.conjunctions(viewColumnsEquiPred)) {
              assert conj.isA(SqlKind.EQUALS);
              RexCall equiCond = (RexCall) conj;
              vEC.addEquivalenceClass(
                  (RexTableInputRef) equiCond.getOperands().get(0),
                  (RexTableInputRef) equiCond.getOperands().get(1));
            }
            compensationColumnsEquiPred = generateEquivalenceClasses(
                rexBuilder, queryPreds[0], qEC, viewColumnsEquiPred, vEC);
            if (compensationColumnsEquiPred == null) {
              // Skip it
              continue;
            }

            // 4.2. We check that range intervals for the query are contained in the view.
            // Compute compensating predicates.
            final RexNode queryRangePred = RexUtil.swapColumnReferences(
                rexBuilder, queryPreds[1], qEC.getEquivalenceClassesMap());
            final RexNode viewRangePred = RexUtil.swapTableColumnReferences(
                rexBuilder, viewPreds[1], tableMapping.inverse(), qEC.getEquivalenceClassesMap());
            compensationRangePred = SubstitutionVisitor.splitFilter(
                simplify, queryRangePred, viewRangePred);
            if (compensationRangePred == null) {
              // Skip it
              continue;
            }

            // 4.3. Finally, we check that residual predicates of the query are satisfied
            // within the view.
            // Compute compensating predicates.
            final RexNode queryResidualPred = RexUtil.swapColumnReferences(
                rexBuilder, queryPreds[2], qEC.getEquivalenceClassesMap());
            final RexNode viewResidualPred = RexUtil.swapTableColumnReferences(
                rexBuilder, viewPreds[2], tableMapping.inverse(), qEC.getEquivalenceClassesMap());
            compensationResidualPred = SubstitutionVisitor.splitFilter(
                simplify, queryResidualPred, viewResidualPred);
            if (compensationResidualPred == null) {
              // Skip it
              continue;
            }

            // 4.4. Final compensation predicate.
            RexNode compensationPred = RexUtil.composeConjunction(
                rexBuilder,
                ImmutableList.of(
                    compensationColumnsEquiPred,
                    compensationRangePred,
                    compensationResidualPred),
                false);
            if (!compensationPred.isAlwaysTrue()) {
              // All columns required by compensating predicates must be contained
              // in the view output (condition 2).
              List<RexNode> viewExprs = extractExpressions(topViewProject, viewNode,
                  rexBuilder);
              compensationPred = rewriteExpression(rexBuilder, viewNode, viewExprs,
                  compensationPred, tableMapping, qEC.getEquivalenceClassesMap(), mq);
              if (compensationPred == null) {
                // Skip it
                continue;
              }
            }

            // 4.5. Generate final rewriting if possible.
            // First, we add the compensation predicate (if any) on top of the view.
            // Then, we trigger the Aggregate unifying method. This method will either create
            // a Project or an Aggregate operator on top of the view. It will also compute the
            // output expressions for the query.
            RelBuilder builder = call.builder();
            builder.push(materialization.tableRel);
            if (!compensationPred.isAlwaysTrue()) {
              builder.filter(compensationPred);
            }
            RelNode result = unify(rexBuilder, builder, builder.build(),
                topProject, node, topViewProject, viewNode, tableMapping,
                qEC.getEquivalenceClassesMap(), mq);
            if (result == null) {
              // Skip it
              continue;
            }
            call.transformTo(result);
          }
        }
      }
    }
  }

  protected abstract boolean isValidPlan(Project topProject, RelNode node,
      RelMetadataQuery mq);

  protected abstract boolean isViewMatching(RelNode node);

  protected abstract List<RexNode> extractExpressions(Project topProject,
      RelNode node, RexBuilder rexBuilder);

  /* This method is responsible for rewriting the query using the given view query.
   *
   * The input node is a Scan on the view table and possibly a compensation Filter
   * on top. If a rewriting can be produced, we return that rewriting. If it cannot
   * be produced, we will return null. */
  protected abstract RelNode unify(RexBuilder rexBuilder, RelBuilder relBuilder,
      RelNode input, Project topProject, RelNode node,
      Project topViewProject, RelNode viewNode,
      BiMap<RelTableRef, RelTableRef> tableMapping,
      Map<RexTableInputRef, Set<RexTableInputRef>> equivalenceClassesMap,
      RelMetadataQuery mq);

  //~ Instances Join ---------------------------------------------------------

  /** Materialized view rewriting for join */
  private abstract static class MaterializedViewJoinRule
          extends AbstractMaterializedViewRule {
    /** Creates a MaterializedViewJoinRule. */
    protected MaterializedViewJoinRule(RelOptRuleOperand operand,
        RelBuilderFactory relBuilderFactory, String description) {
      super(operand, relBuilderFactory, description);
    }

    @Override protected boolean isValidPlan(Project topProject, RelNode node,
        RelMetadataQuery mq) {
      Join join = (Join) node;
      if (join.getJoinType() != JoinRelType.INNER) {
        // TODO: Rewriting for non-inner joins not supported yet
        return false;
      }
      return isValidRexNodePlan(join, mq);
    }

    @Override protected boolean isViewMatching(RelNode node) {
      if (node instanceof Join) {
        return true;
      }
      if (node instanceof Project && ((Project) node).getInput() instanceof Join) {
        return true;
      }
      return false;
    }

    @Override protected List<RexNode> extractExpressions(Project topProject,
        RelNode node, RexBuilder rexBuilder) {
      Join viewJoin = (Join) node;
      List<RexNode> viewExprs = new ArrayList<>();
      if (topProject != null) {
        for (RexNode e : topProject.getChildExps()) {
          viewExprs.add(e);
        }
      } else {
        for (int i = 0; i < viewJoin.getRowType().getFieldCount(); i++) {
          viewExprs.add(rexBuilder.makeInputRef(viewJoin, i));
        }
      }
      return viewExprs;
    }

    @Override protected RelNode unify(
        RexBuilder rexBuilder,
        RelBuilder relBuilder,
        RelNode input,
        Project topProject,
        RelNode node,
        Project topViewProject,
        RelNode viewNode,
        BiMap<RelTableRef, RelTableRef> tableMapping,
        Map<RexTableInputRef, Set<RexTableInputRef>> equivalenceClassesMap,
        RelMetadataQuery mq) {
      List<RexNode> exprs = extractExpressions(topProject, node, rexBuilder);
      List<RexNode> exprsLineage = new ArrayList<>(exprs.size());
      for (RexNode expr : exprs) {
        Set<RexNode> s = mq.getExpressionLineage(node, expr);
        if (s == null) {
          // Bail out
          return null;
        }
        assert s.size() == 1;
        exprsLineage.add(s.iterator().next());
      }
      List<RexNode> viewExprs = extractExpressions(topViewProject, viewNode, rexBuilder);
      List<RexNode> rewrittenExprs = rewriteExpressions(rexBuilder, viewNode, viewExprs,
          exprsLineage, tableMapping, equivalenceClassesMap, mq);
      if (rewrittenExprs == null) {
        return null;
      }
      return relBuilder
          .push(input)
          .project(rewrittenExprs)
          .build();
    }
  }

  /** Rule that matches Project on Join. */
  public static class MaterializedViewProjectJoinRule extends MaterializedViewJoinRule {
    public MaterializedViewProjectJoinRule(RelBuilderFactory relBuilderFactory) {
      super(
          operand(Project.class,
              operand(Join.class, any())),
          relBuilderFactory,
          "MaterializedViewJoinRule(Project-Join)");
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Project project = call.rel(0);
      final Join join = call.rel(1);
      perform(call, project, join);
    }
  }

  /** Rule that matches Join. */
  public static class MaterializedViewOnlyJoinRule extends MaterializedViewJoinRule {
    public MaterializedViewOnlyJoinRule(RelBuilderFactory relBuilderFactory) {
      super(
          operand(Join.class, any()),
          relBuilderFactory,
          "MaterializedViewJoinRule(Join)");
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Join join = call.rel(0);
      perform(call, null, join);
    }
  }

  //~ Instances Aggregate ----------------------------------------------------

  /** Materialized view rewriting for aggregate */
  private abstract static class MaterializedViewAggregateRule
          extends AbstractMaterializedViewRule {
    /** Creates a MaterializedViewAggregateRule. */
    protected MaterializedViewAggregateRule(RelOptRuleOperand operand,
        RelBuilderFactory relBuilderFactory, String description) {
      super(operand, relBuilderFactory, description);
    }

    @Override protected boolean isValidPlan(Project topProject, RelNode node,
        RelMetadataQuery mq) {
      Aggregate aggregate = (Aggregate) node;
      if (aggregate.getGroupType() != Aggregate.Group.SIMPLE) {
        // TODO: Rewriting with grouping sets not supported yet
        return false;
      }
      return isValidRexNodePlan(aggregate.getInput(), mq);
    }

    @Override protected boolean isViewMatching(RelNode node) {
      if (node instanceof Aggregate) {
        return true;
      }
      if (node instanceof Project && ((Project) node).getInput() instanceof Aggregate) {
        return true;
      }
      return false;
    }

    @Override protected List<RexNode> extractExpressions(Project topProject,
        RelNode node, RexBuilder rexBuilder) {
      Aggregate viewAggregate = (Aggregate) node;
      List<RexNode> viewExprs = new ArrayList<>();
      if (topProject != null) {
        for (RexNode e : topProject.getChildExps()) {
          viewExprs.add(e);
        }
      } else {
        for (int i = 0; i < viewAggregate.getGroupCount(); i++) {
          viewExprs.add(rexBuilder.makeInputRef(viewAggregate, i));
        }
      }
      return viewExprs;
    }

    @Override protected RelNode unify(
        RexBuilder rexBuilder,
        RelBuilder relBuilder,
        RelNode input,
        Project topProject,
        RelNode node,
        Project topViewProject,
        RelNode viewNode,
        BiMap<RelTableRef, RelTableRef> tableMapping,
        Map<RexTableInputRef, Set<RexTableInputRef>> equivalenceClassesMap,
        RelMetadataQuery mq) {
      final Aggregate queryAggregate = (Aggregate) node;
      final Aggregate viewAggregate = (Aggregate) viewNode;
      // Get group by references and aggregate call input references needed
      ImmutableBitSet.Builder indexes = ImmutableBitSet.builder();
      ImmutableBitSet references = null;
      if (topProject != null) {
        // We have a Project on top, gather only what is needed
        final RelOptUtil.InputFinder inputFinder =
            new RelOptUtil.InputFinder(new LinkedHashSet<RelDataTypeField>());
        for (RexNode e : topProject.getChildExps()) {
          e.accept(inputFinder);
        }
        references = inputFinder.inputBitSet.build();
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
      Multimap<Integer, Integer> m = generateMapping(rexBuilder, queryAggregate.getInput(),
          viewAggregate.getInput(), indexes.build(), tableMapping, equivalenceClassesMap, mq);
      if (m == null) {
        // Bail out
        return null;
      }

      // We could map all expressions. Create aggregate mapping.
      Mapping aggregateMapping = Mappings.create(MappingType.FUNCTION,
          queryAggregate.getRowType().getFieldCount(), viewAggregate.getRowType().getFieldCount());
      for (int i = 0; i < queryAggregate.getGroupCount(); i++) {
        Collection<Integer> c = m.get(queryAggregate.getGroupSet().nth(i));
        for (int j : c) {
          int targetIdx = viewAggregate.getGroupSet().indexOf(j);
          if (targetIdx == -1) {
            continue;
          }
          aggregateMapping.set(i, targetIdx);
          break;
        }
        if (aggregateMapping.getTargetOpt(i) == -1) {
          // It is not part of group by, we bail out
          return null;
        }
      }
      for (int idx = 0; idx < queryAggregate.getAggCallList().size(); idx++) {
        if (references != null && !references.get(queryAggregate.getGroupCount() + idx)) {
          // Ignore
          continue;
        }
        AggregateCall queryAggCall = queryAggregate.getAggCallList().get(idx);
        List<Integer> queryAggCallIndexes = queryAggCall.getArgList();
        for (int j = 0; j < viewAggregate.getAggCallList().size(); j++) {
          AggregateCall viewAggCall = viewAggregate.getAggCallList().get(j);
          if (queryAggCall.getAggregation() != viewAggCall.getAggregation()
              || queryAggCall.isDistinct() != viewAggCall.isDistinct()
              || queryAggCall.getArgList().size() != viewAggCall.getArgList().size()
              || queryAggCall.getType() != viewAggCall.getType()) {
            // Continue
            continue;
          }
          List<Integer> viewAggCallIndexes = new ArrayList<>();
          for (int aggCallIdx : viewAggCall.getArgList()) {
            viewAggCallIndexes.add(m.get(aggCallIdx).iterator().next());
          }
          if (!queryAggCallIndexes.equals(viewAggCallIndexes)) {
            // Continue
            continue;
          }
          aggregateMapping.set(queryAggregate.getGroupCount() + idx,
              viewAggregate.getGroupCount() + j);
          break;
        }
      }

      // Generate result rewriting
      Mapping rewritingMapping = null;
      RelNode result = relBuilder
          .push(input)
          .build();
      if (queryAggregate.getGroupCount() != viewAggregate.getGroupCount()) {
        // Target is coarser level of aggregation. Generate an aggregate.
        rewritingMapping = Mappings.create(MappingType.FUNCTION,
            topViewProject != null ? topViewProject.getRowType().getFieldCount()
                : viewAggregate.getRowType().getFieldCount(),
            queryAggregate.getRowType().getFieldCount());
        final ImmutableBitSet.Builder groupSetB = ImmutableBitSet.builder();
        for (int i = 0; i < queryAggregate.getGroupCount(); i++) {
          int targetIdx = aggregateMapping.getTargetOpt(i);
          if (targetIdx == -1) {
            // No matching group by column, we bail out
            return null;
          }
          if (topViewProject != null) {
            boolean added = false;
            for (int k = 0; k < topViewProject.getChildExps().size(); k++) {
              RexNode n = topViewProject.getChildExps().get(k);
              if (!n.isA(SqlKind.INPUT_REF)) {
                continue;
              }
              int ref = ((RexInputRef) n).getIndex();
              if (ref == targetIdx) {
                groupSetB.set(k);
                rewritingMapping.set(k, i);
                added = true;
                break;
              }
            }
            if (!added) {
              // No matching group by column, we bail out
              return null;
            }
          } else {
            groupSetB.set(targetIdx);
            rewritingMapping.set(targetIdx, i);
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
          int targetIdx = aggregateMapping.getTargetOpt(sourceIdx);
          if (targetIdx == -1) {
            // No matching aggregation column, we bail out
            return null;
          }
          AggregateCall queryAggCall = queryAggregate.getAggCallList().get(i);
          if (topViewProject != null) {
            boolean added = false;
            for (int k = 0; k < topViewProject.getChildExps().size(); k++) {
              RexNode n = topViewProject.getChildExps().get(k);
              if (!n.isA(SqlKind.INPUT_REF)) {
                continue;
              }
              int ref = ((RexInputRef) n).getIndex();
              if (ref == targetIdx) {
                aggregateCalls.add(
                    relBuilder.aggregateCall(
                        SubstitutionVisitor.getRollup(queryAggCall.getAggregation()),
                        queryAggCall.isDistinct(),
                        null,
                        queryAggCall.name,
                        ImmutableList.of(rexBuilder.makeInputRef(input, k))));
                rewritingMapping.set(k, sourceIdx);
                added = true;
                break;
              }
            }
            if (!added) {
              // No matching aggregation column, we bail out
              return null;
            }
          } else {
            aggregateCalls.add(
                relBuilder.aggregateCall(
                    SubstitutionVisitor.getRollup(queryAggCall.getAggregation()),
                    queryAggCall.isDistinct(),
                    null,
                    queryAggCall.name,
                    ImmutableList.of(rexBuilder.makeInputRef(input, targetIdx))));
            rewritingMapping.set(targetIdx, sourceIdx);
          }
        }
        result = relBuilder
            .push(result)
            .aggregate(relBuilder.groupKey(groupSet, false, null), aggregateCalls)
            .build();
        // We introduce a project on top, as group by columns order is lost
        List<RexNode> projects = new ArrayList<>();
        Mapping inverseMapping = rewritingMapping.inverse();
        for (int i = 0; i < queryAggregate.getGroupCount(); i++) {
          projects.add(
              rexBuilder.makeInputRef(result,
                  groupSet.indexOf(inverseMapping.getTarget(i))));
        }
        for (int i = 0; i < queryAggregate.getAggCallList().size(); i++) {
          projects.add(
              rexBuilder.makeInputRef(result, queryAggregate.getGroupCount() + i));
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
      final List<RexNode> topExprs = new ArrayList<>();
      if (topProject != null) {
        topExprs.addAll(topProject.getChildExps());
      } else {
        // Add all
        for (int pos = 0; pos < queryAggregate.getRowType().getFieldCount(); pos++) {
          topExprs.add(rexBuilder.makeInputRef(queryAggregate, pos));
        }
      }
      // Available in view.
      final List<String> viewExprs = new ArrayList<>();
      if (topViewProject != null) {
        for (int i = 0; i < topViewProject.getChildExps().size(); i++) {
          viewExprs.add(topViewProject.getChildExps().get(i).toString());
        }
      } else {
        // Add all
        for (int i = 0; i < viewAggregate.getRowType().getFieldCount(); i++) {
          viewExprs.add(rexBuilder.makeInputRef(viewAggregate, i).toString());
        }
      }
      final List<RexNode> rewrittenExprs = new ArrayList<>(topExprs.size());
      for (RexNode expr : topExprs) {
        RexNode rewrittenExpr = shuttleReferences(rexBuilder, expr, aggregateMapping);
        if (rewrittenExpr == null) {
          // Cannot map expression
          return null;
        }
        int pos = viewExprs.indexOf(rewrittenExpr.toString());
        if (pos == -1) {
          // Cannot map expression
          return null;
        }
        if (rewritingMapping != null) {
          pos = rewritingMapping.getTargetOpt(pos);
          if (pos == -1) {
            // Cannot map expression
            return null;
          }
        }
        rewrittenExprs.add(rexBuilder.makeInputRef(result, pos));
      }
      return relBuilder
          .push(result)
          .project(rewrittenExprs)
          .build();
    }
  }

  /** Rule that matches Project on Aggregate. */
  public static class MaterializedViewProjectAggregateRule extends MaterializedViewAggregateRule {
    public MaterializedViewProjectAggregateRule(RelBuilderFactory relBuilderFactory) {
      super(
          operand(Project.class,
              operand(Aggregate.class, any())),
          relBuilderFactory,
          "MaterializedViewAggregateRule(Project-Aggregate)");
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Project project = call.rel(0);
      final Aggregate aggregate = call.rel(1);
      perform(call, project, aggregate);
    }
  }

  /** Rule that matches Aggregate. */
  public static class MaterializedViewOnlyAggregateRule extends MaterializedViewAggregateRule {
    public MaterializedViewOnlyAggregateRule(RelBuilderFactory relBuilderFactory) {
      super(
          operand(Aggregate.class, any()),
          relBuilderFactory,
          "MaterializedViewAggregateRule(Aggregate)");
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Aggregate aggregate = call.rel(0);
      perform(call, null, aggregate);
    }
  }

  //~ Methods ----------------------------------------------------------------

  /* It will flatten a multimap containing table references to table references,
   * producing all possible combinations of mappings. Each of the mappings will
   * be bi-directional. */
  private static List<BiMap<RelTableRef, RelTableRef>> generateTableMappings(
      Multimap<RelTableRef, RelTableRef> multiMapTables) {
    final List<BiMap<RelTableRef, RelTableRef>> result = new ArrayList<>();
    if (multiMapTables.isEmpty()) {
      return result;
    }
    result.add(HashBiMap.<RelTableRef, RelTableRef>create());
    for (Entry<RelTableRef, Collection<RelTableRef>> e : multiMapTables.asMap().entrySet()) {
      boolean added = false;
      for (RelTableRef target : e.getValue()) {
        if (added) {
          for (BiMap<RelTableRef, RelTableRef> m : result) {
            final BiMap<RelTableRef, RelTableRef> newM =
                HashBiMap.<RelTableRef, RelTableRef>create(m);
            newM.put(e.getKey(), target);
            result.add(newM);
          }
        } else {
          for (BiMap<RelTableRef, RelTableRef> m : result) {
            m.put(e.getKey(), target);
          }
          added = true;
        }
      }
      // Mapping needs to exist
      assert added;
    }
    return result;
  }

  /* Currently we only support TableScan - Project - Filter - Join */
  private static boolean isValidRexNodePlan(RelNode node, RelMetadataQuery mq) {
    final Multimap<Class<? extends RelNode>, RelNode> m =
            mq.getNodeTypes(node);
    for (Class<? extends RelNode> c : m.keySet()) {
      if (!TableScan.class.isAssignableFrom(c)
              && !Project.class.isAssignableFrom(c)
              && !Filter.class.isAssignableFrom(c)
              && !Join.class.isAssignableFrom(c)) {
        // Skip it
        return false;
      }
    }
    return true;
  }

  /* Classifies each of the predicates in the list into one of these three
   * categories:
   * - column equality predicates, or
   * - range predicates, comprising <, <=, >, >=, and = between a reference
   * and a constant, or
   * - residual predicates, all the rest
   *
   * For each category, it creates the conjunction of the predicates. The
   * result is an array of three RexNode objects corresponding to each
   * category. */
  private static RexNode[] splitPredicates(
      RexBuilder rexBuilder, ImmutableList<RexNode> predicates) {
    List<RexNode> equiColumnsPreds = new ArrayList<>();
    List<RexNode> rangePreds = new ArrayList<>();
    List<RexNode> residualPreds = new ArrayList<>();
    for (RexNode e : predicates) {
      switch (e.getKind()) {
      case EQUALS:
        RexCall eqCall = (RexCall) e;
        if (RexUtil.isReferenceOrAccess(eqCall.getOperands().get(0), false)
                && RexUtil.isReferenceOrAccess(eqCall.getOperands().get(1), false)) {
          equiColumnsPreds.add(e);
        } else if ((RexUtil.isReferenceOrAccess(eqCall.getOperands().get(0), false)
                && RexUtil.isConstant(eqCall.getOperands().get(1)))
            || (RexUtil.isReferenceOrAccess(eqCall.getOperands().get(1), false)
                && RexUtil.isConstant(eqCall.getOperands().get(0)))) {
          rangePreds.add(e);
        } else {
          residualPreds.add(e);
        }
        break;
      case LESS_THAN:
      case GREATER_THAN:
      case LESS_THAN_OR_EQUAL:
      case GREATER_THAN_OR_EQUAL:
      case NOT_EQUALS:
        RexCall rangeCall = (RexCall) e;
        if ((RexUtil.isReferenceOrAccess(rangeCall.getOperands().get(0), false)
                && RexUtil.isConstant(rangeCall.getOperands().get(1)))
            || (RexUtil.isReferenceOrAccess(rangeCall.getOperands().get(1), false)
                && RexUtil.isConstant(rangeCall.getOperands().get(0)))) {
          rangePreds.add(e);
        } else {
          residualPreds.add(e);
        }
        break;
      default:
        residualPreds.add(e);
      }
    }
    return new RexNode[] {
        RexUtil.composeConjunction(rexBuilder, equiColumnsPreds, false),
        RexUtil.composeConjunction(rexBuilder, rangePreds, false),
        RexUtil.composeConjunction(rexBuilder, residualPreds, false)};
  }

  /* Given the equi-column predicates of the query and the view and the
   * computed equivalence classes, it extracts possible mappings between
   * the equivalence classes.
   *
   * If there is no mapping, it returns null. If there is a exact match,
   * it will return a compensation predicate that evaluates to true.
   * Finally, if a compensation predicate needs to be enforced on top of
   * the view to make the equivalences classes match, it returns that
   * compensation predicate */
  private static RexNode generateEquivalenceClasses(RexBuilder rexBuilder,
      RexNode queryEquiColumnsPreds, EquivalenceClasses qEC,
      RexNode viewEquiColumnsPreds, EquivalenceClasses vEC) {
    if (queryEquiColumnsPreds.isAlwaysTrue() && viewEquiColumnsPreds.isAlwaysTrue()) {
      // No column equality predicates in query and view
      // Empty mapping and compensation predicate
      assert qEC.getEquivalenceClasses().isEmpty() && vEC.getEquivalenceClasses().isEmpty();
      return rexBuilder.makeLiteral(true);
    }
    if (queryEquiColumnsPreds.isAlwaysTrue() || viewEquiColumnsPreds.isAlwaysTrue()) {
      // No column equality predicates in query or view
      assert qEC.getEquivalenceClasses().isEmpty() || vEC.getEquivalenceClasses().isEmpty();
      return null;
    }

    final List<Set<RexTableInputRef>> queryEquivalenceClasses = qEC.getEquivalenceClasses();
    final List<Set<RexTableInputRef>> viewEquivalenceClasses = vEC.getEquivalenceClasses();
    final Mapping mapping = extractPossibleMapping(
        queryEquivalenceClasses, viewEquivalenceClasses);
    if (mapping == null) {
      // Did not find mapping between the equivalence classes,
      // bail out
      return null;
    }

    // Create the compensation predicate
    RexNode compensationPredicate = rexBuilder.makeLiteral(true);
    for (IntPair pair : mapping) {
      Set<RexTableInputRef> difference = new HashSet<>(
          queryEquivalenceClasses.get(pair.target));
      difference.removeAll(viewEquivalenceClasses.get(pair.source));
      for (RexTableInputRef e : difference) {
        RexNode equals = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
            e, viewEquivalenceClasses.get(pair.source).iterator().next());
        compensationPredicate = rexBuilder.makeCall(SqlStdOperatorTable.AND,
            compensationPredicate, equals);
      }
    }

    return compensationPredicate;
  }

  /* Given the query and view equivalence classes, it extracts the possible mappings
   * from each view equivalence class to each query equivalence class.
   *
   * If any of the view equivalence classes cannot be mapped to a query equivalence
   * class, it returns null. */
  private static Mapping extractPossibleMapping(
      List<Set<RexTableInputRef>> queryEquivalenceClasses,
      List<Set<RexTableInputRef>> viewEquivalenceClasses) {
    Mapping mapping = Mappings.create(MappingType.FUNCTION,
        viewEquivalenceClasses.size(), queryEquivalenceClasses.size());
    for (int i = 0; i < viewEquivalenceClasses.size(); i++) {
      boolean foundQueryEquivalenceClass = false;
      final Set<RexTableInputRef> viewEquivalenceClass = viewEquivalenceClasses.get(i);
      for (int j = 0; j < queryEquivalenceClasses.size(); j++) {
        final Set<RexTableInputRef> queryEquivalenceClass = queryEquivalenceClasses.get(j);
        if (queryEquivalenceClass.containsAll(viewEquivalenceClass)) {
          mapping.set(i, j);
          foundQueryEquivalenceClass = true;
          break;
        }
      } // end for

      if (!foundQueryEquivalenceClass) {
        // View equivalence class not found in query equivalence class
        return null;
      }
    } // end for

    return mapping;
  }

  /* Given the input expression that references source expressions in the query,
   * it will rewrite it to refer to the view output.
   *
   * If any of the subexpressions in the input expression cannot be mapped to
   * the query, it will return null.
   */
  private static RexNode rewriteExpression(
      RexBuilder rexBuilder,
      RelNode viewNode,
      List<RexNode> viewExprs,
      RexNode expr,
      BiMap<RelTableRef, RelTableRef> tableMapping,
      Map<RexTableInputRef, Set<RexTableInputRef>> equivalenceClassesMap,
      RelMetadataQuery mq) {
    List<RexNode> rewrittenExprs = rewriteExpressions(rexBuilder, viewNode, viewExprs,
        ImmutableList.of(expr), tableMapping, equivalenceClassesMap, mq);
    if (rewrittenExprs == null) {
      return null;
    }
    assert rewrittenExprs.size() == 1;
    return rewrittenExprs.get(0);
  }

  private static List<RexNode> rewriteExpressions(
      RexBuilder rexBuilder,
      RelNode viewNode,
      List<RexNode> viewExprs,
      List<RexNode> exprs,
      BiMap<RelTableRef, RelTableRef> tableMapping,
      Map<RexTableInputRef, Set<RexTableInputRef>> equivalenceClassesMap,
      RelMetadataQuery mq) {
    Map<String, Integer> exprsLineage = new HashMap<>();
    for (int i = 0; i < viewExprs.size(); i++) {
      final RexNode e = viewExprs.get(i);
      final Set<RexNode> s = mq.getExpressionLineage(viewNode, e);
      if (s == null) {
        // Next expression
        continue;
      }
      // We only support project - filter - join, thus it should map to
      // a single expression
      assert s.size() == 1;
      // Rewrite expr to be expressed on query tables
      exprsLineage.put(
          RexUtil.swapTableColumnReferences(
              rexBuilder,
              s.iterator().next(),
              tableMapping.inverse(),
              equivalenceClassesMap).toString(),
          i);
    }

    List<RexNode> rewrittenExprs = new ArrayList<>(exprs.size());
    for (RexNode expr : exprs) {
      RexNode rewrittenExpr = replaceWithOriginalReferences(rexBuilder, expr, exprsLineage);
      if (RexUtil.containsTableInputRef(rewrittenExpr) != null) {
        // Some expressions were not present in view output
        return null;
      }
      rewrittenExprs.add(rewrittenExpr);
    }
    return rewrittenExprs;
  }

  /* Mapping from node expressions to target expressions.
   *
   * If any of the expressions cannot be mapped, we return null. */
  private static Multimap<Integer, Integer> generateMapping(
      RexBuilder rexBuilder,
      RelNode node,
      RelNode target,
      ImmutableBitSet positions,
      BiMap<RelTableRef, RelTableRef> tableMapping,
      Map<RexTableInputRef, Set<RexTableInputRef>> equivalenceClassesMap,
      RelMetadataQuery mq) {
    Multimap<String, Integer> exprsLineage = ArrayListMultimap.create();
    for (int i = 0; i < target.getRowType().getFieldCount(); i++) {
      Set<RexNode> s = mq.getExpressionLineage(target, rexBuilder.makeInputRef(target, i));
      if (s == null) {
        // Bail out
        continue;
      }
      // We only support project - filter - join, thus it should map to
      // a single expression
      assert s.size() == 1;
      // Rewrite expr to be expressed on query tables
      exprsLineage.put(
          RexUtil.swapTableColumnReferences(
              rexBuilder,
              s.iterator().next(),
              tableMapping.inverse(),
              equivalenceClassesMap).toString(),
          i);
    }

    Multimap<Integer, Integer> m = ArrayListMultimap.create();
    for (int i : positions) {
      Set<RexNode> s = mq.getExpressionLineage(node, rexBuilder.makeInputRef(node, i));
      if (s == null) {
        // Bail out
        return null;
      }
      // We only support project - filter - join, thus it should map to
      // a single expression
      assert s.size() == 1;
      // Rewrite expr to be expressed on query tables
      Collection<Integer> c = exprsLineage.get(
          RexUtil.swapColumnReferences(
              rexBuilder, s.iterator().next(), equivalenceClassesMap).toString());
      if (c == null) {
        // Bail out
        return null;
      }
      for (Integer j : c) {
        m.put(i, j);
      }
    }
    return m;
  }

  /* Given the input expression, it will replace (sub)expressions when possible
   * using the content of the mapping. In particular, the mapping contains the
   * digest of the expression and the index that the replacement input ref should
   * point to. */
  private static RexNode replaceWithOriginalReferences(final RexBuilder rexBuilder,
      final RexNode expr, final Map<String, Integer> mapping) {
    // Currently we allow the following:
    // 1) compensation pred can be directly map to expression
    // 2) all references in compensation pred can be map to expressions
    RexShuttle visitor =
        new RexShuttle() {
          @Override public RexNode visitCall(RexCall call) {
            Integer pos = mapping.get(call.toString());
            if (pos != null) {
              // Found it
              return rexBuilder.makeInputRef(call.getType(), pos);
            }
            return super.visitCall(call);
          }

          @Override public RexNode visitTableInputRef(RexTableInputRef inputRef) {
            Integer pos = mapping.get(inputRef.toString());
            if (pos != null) {
              // Found it
              return rexBuilder.makeInputRef(inputRef.getType(), pos);
            }
            return super.visitTableInputRef(inputRef);
          }
        };
    return visitor.apply(expr);
  }

  /* Replaces all the input references by the position in the
   * input column set. If a reference index cannot be found in
   * the input set, then we return null. */
  private static RexNode shuttleReferences(final RexBuilder rexBuilder,
      final RexNode node, final Mapping mapping) {
    try {
      RexShuttle visitor =
          new RexShuttle() {
            @Override public RexNode visitInputRef(RexInputRef inputRef) {
              int pos = mapping.getTargetOpt(inputRef.getIndex());
              if (pos != -1) {
                // Found it
                return rexBuilder.makeInputRef(inputRef.getType(), pos);
              }
              throw Util.FoundOne.NULL;
            }
          };
      return visitor.apply(node);
    } catch (Util.FoundOne ex) {
      Util.swallow(ex, null);
      return null;
    }
  }

  /**
   * Class representing an equivalence class, i.e., a set of equivalent columns.
   */
  private static class EquivalenceClasses {

    private Map<RexTableInputRef, Set<RexTableInputRef>> nodeToEquivalenceClass;

    protected EquivalenceClasses() {
      nodeToEquivalenceClass = new HashMap<>();
    }

    protected void addEquivalenceClass(RexTableInputRef p1, RexTableInputRef p2) {
      Set<RexTableInputRef> c1 = nodeToEquivalenceClass.get(p1);
      Set<RexTableInputRef> c2 = nodeToEquivalenceClass.get(p2);
      if (c1 != null && c2 != null) {
        // Both present, we need to merge
        if (c1.size() < c2.size()) {
          // We swap them to merge
          c1 = c2;
          p1 = p2;
        }
        for (RexTableInputRef newRef : c2) {
          c1.add(newRef);
          nodeToEquivalenceClass.put(newRef, c1);
        }
      } else if (c1 != null) {
        // p1 present, we need to merge into it
        c1.add(p2);
        nodeToEquivalenceClass.put(p2, c1);
      } else if (c2 != null) {
        // p2 present, we need to merge into it
        c2.add(p1);
        nodeToEquivalenceClass.put(p1, c2);
      } else {
        // None are present, add to same equivalence class
        Set<RexTableInputRef> equivalenceClass = new LinkedHashSet<>();
        equivalenceClass.add(p1);
        equivalenceClass.add(p2);
        nodeToEquivalenceClass.put(p1, equivalenceClass);
        nodeToEquivalenceClass.put(p2, equivalenceClass);
      }
    }

    protected Map<RexTableInputRef, Set<RexTableInputRef>> getEquivalenceClassesMap() {
      return ImmutableMap.copyOf(nodeToEquivalenceClass);
    }

    protected List<Set<RexTableInputRef>> getEquivalenceClasses() {
      return ImmutableList.copyOf(nodeToEquivalenceClass.values());
    }
  }
}

// End AbstractMaterializedViewRule.java
