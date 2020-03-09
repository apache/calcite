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

import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.SubstitutionVisitor;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexExecutor;
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
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.graph.DefaultDirectedGraph;
import org.apache.calcite.util.graph.DefaultEdge;
import org.apache.calcite.util.graph.DirectedGraph;
import org.apache.calcite.util.mapping.Mapping;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
public abstract class MaterializedViewRule extends RelOptRule {

  //~ Instance fields --------------------------------------------------------

  /** Whether to generate rewritings containing union if the query results
   * are contained within the view results. */
  protected final boolean generateUnionRewriting;

  /** If we generate union rewriting, we might want to pull up projections
   * from the query itself to maximize rewriting opportunities. */
  protected final HepProgram unionRewritingPullProgram;

  /** Whether we should create the rewriting in the minimal subtree of plan
   * operators. */
  protected final boolean fastBailOut;

  //~ Constructors -----------------------------------------------------------

  /** Creates a AbstractMaterializedViewRule. */
  protected MaterializedViewRule(RelOptRuleOperand operand,
      RelBuilderFactory relBuilderFactory, String description,
      boolean generateUnionRewriting, HepProgram unionRewritingPullProgram,
      boolean fastBailOut) {
    super(operand, relBuilderFactory, description);
    this.generateUnionRewriting = generateUnionRewriting;
    this.unionRewritingPullProgram = unionRewritingPullProgram;
    this.fastBailOut = fastBailOut;
  }

  @Override public boolean matches(RelOptRuleCall call) {
    return !call.getPlanner().getMaterializations().isEmpty();
  }

  /**
   * Rewriting logic is based on "Optimizing Queries Using Materialized Views:
   * A Practical, Scalable Solution" by Goldstein and Larson.
   *
   * <p>On the query side, rules matches a Project-node chain or node, where node
   * is either an Aggregate or a Join. Subplan rooted at the node operator must
   * be composed of one or more of the following operators: TableScan, Project,
   * Filter, and Join.
   *
   * <p>For each join MV, we need to check the following:
   * <ol>
   * <li> The plan rooted at the Join operator in the view produces all rows
   * needed by the plan rooted at the Join operator in the query.</li>
   * <li> All columns required by compensating predicates, i.e., predicates that
   * need to be enforced over the view, are available at the view output.</li>
   * <li> All output expressions can be computed from the output of the view.</li>
   * <li> All output rows occur with the correct duplication factor. We might
   * rely on existing Unique-Key - Foreign-Key relationships to extract that
   * information.</li>
   * </ol>
   *
   * <p>In turn, for each aggregate MV, we need to check the following:
   * <ol>
   * <li> The plan rooted at the Aggregate operator in the view produces all rows
   * needed by the plan rooted at the Aggregate operator in the query.</li>
   * <li> All columns required by compensating predicates, i.e., predicates that
   * need to be enforced over the view, are available at the view output.</li>
   * <li> The grouping columns in the query are a subset of the grouping columns
   * in the view.</li>
   * <li> All columns required to perform further grouping are available in the
   * view output.</li>
   * <li> All columns required to compute output expressions are available in the
   * view output.</li>
   * </ol>
   *
   * <p>The rule contains multiple extensions compared to the original paper. One of
   * them is the possibility of creating rewritings using Union operators, e.g., if
   * the result of a query is partially contained in the materialized view.
   */
  protected void perform(RelOptRuleCall call, Project topProject, RelNode node) {
    final RexBuilder rexBuilder = node.getCluster().getRexBuilder();
    final RelMetadataQuery mq = call.getMetadataQuery();
    final RelOptPlanner planner = call.getPlanner();
    final RexExecutor executor =
        Util.first(planner.getExecutor(), RexUtil.EXECUTOR);
    final RelOptPredicateList predicates = RelOptPredicateList.EMPTY;
    final RexSimplify simplify =
        new RexSimplify(rexBuilder, predicates, executor);

    final List<RelOptMaterialization> materializations =
        planner.getMaterializations();

    if (!materializations.isEmpty()) {
      // 1. Explore query plan to recognize whether preconditions to
      // try to generate a rewriting are met
      if (!isValidPlan(topProject, node, mq)) {
        return;
      }

      // 2. Initialize all query related auxiliary data structures
      // that will be used throughout query rewriting process
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
      final RexNode pred =
          simplify.simplifyUnknownAsFalse(
              RexUtil.composeConjunction(rexBuilder,
                  queryPredicateList.pulledUpPredicates));
      final Pair<RexNode, RexNode> queryPreds = splitPredicates(rexBuilder, pred);

      // Extract query equivalence classes. An equivalence class is a set
      // of columns in the query output that are known to be equal.
      final EquivalenceClasses qEC = new EquivalenceClasses();
      for (RexNode conj : RelOptUtil.conjunctions(queryPreds.left)) {
        assert conj.isA(SqlKind.EQUALS);
        RexCall equiCond = (RexCall) conj;
        qEC.addEquivalenceClass(
            (RexTableInputRef) equiCond.getOperands().get(0),
            (RexTableInputRef) equiCond.getOperands().get(1));
      }

      // 3. We iterate through all applicable materializations trying to
      // rewrite the given query
      for (RelOptMaterialization materialization : materializations) {
        RelNode view = materialization.tableRel;
        Project topViewProject;
        RelNode viewNode;
        if (materialization.queryRel instanceof Project) {
          topViewProject = (Project) materialization.queryRel;
          viewNode = topViewProject.getInput();
        } else {
          topViewProject = null;
          viewNode = materialization.queryRel;
        }

        // Extract view table references
        final Set<RelTableRef> viewTableRefs = mq.getTableReferences(viewNode);
        if (viewTableRefs == null) {
          // Skip it
          continue;
        }

        // Filter relevant materializations. Currently, we only check whether
        // the materialization contains any table that is used by the query
        // TODO: Filtering of relevant materializations can be improved to be more fine-grained.
        boolean applicable = false;
        for (RelTableRef tableRef : viewTableRefs) {
          if (queryTableRefs.contains(tableRef)) {
            applicable = true;
            break;
          }
        }
        if (!applicable) {
          // Skip it
          continue;
        }

        // 3.1. View checks before proceeding
        if (!isValidPlan(topViewProject, viewNode, mq)) {
          // Skip it
          continue;
        }

        // 3.2. Initialize all query related auxiliary data structures
        // that will be used throughout query rewriting process
        // Extract view predicates
        final RelOptPredicateList viewPredicateList =
            mq.getAllPredicates(viewNode);
        if (viewPredicateList == null) {
          // Skip it
          continue;
        }
        final RexNode viewPred = simplify.simplifyUnknownAsFalse(
            RexUtil.composeConjunction(rexBuilder,
                viewPredicateList.pulledUpPredicates));
        final Pair<RexNode, RexNode> viewPreds = splitPredicates(rexBuilder, viewPred);

        // Extract view tables
        MatchModality matchModality;
        Multimap<RexTableInputRef, RexTableInputRef> compensationEquiColumns =
            ArrayListMultimap.create();
        if (!queryTableRefs.equals(viewTableRefs)) {
          // We try to compensate, e.g., for join queries it might be
          // possible to join missing tables with view to compute result.
          // Two supported cases: query tables are subset of view tables (we need to
          // check whether they are cardinality-preserving joins), or view tables are
          // subset of query tables (add additional tables through joins if possible)
          if (viewTableRefs.containsAll(queryTableRefs)) {
            matchModality = MatchModality.QUERY_PARTIAL;
            final EquivalenceClasses vEC = new EquivalenceClasses();
            for (RexNode conj : RelOptUtil.conjunctions(viewPreds.left)) {
              assert conj.isA(SqlKind.EQUALS);
              RexCall equiCond = (RexCall) conj;
              vEC.addEquivalenceClass(
                  (RexTableInputRef) equiCond.getOperands().get(0),
                  (RexTableInputRef) equiCond.getOperands().get(1));
            }
            if (!compensatePartial(viewTableRefs, vEC, queryTableRefs,
                    compensationEquiColumns)) {
              // Cannot rewrite, skip it
              continue;
            }
          } else if (queryTableRefs.containsAll(viewTableRefs)) {
            matchModality = MatchModality.VIEW_PARTIAL;
            ViewPartialRewriting partialRewritingResult = compensateViewPartial(
                call.builder(), rexBuilder, mq, view,
                topProject, node, queryTableRefs, qEC,
                topViewProject, viewNode, viewTableRefs);
            if (partialRewritingResult == null) {
              // Cannot rewrite, skip it
              continue;
            }
            // Rewrite succeeded
            view = partialRewritingResult.newView;
            topViewProject = partialRewritingResult.newTopViewProject;
            viewNode = partialRewritingResult.newViewNode;
          } else {
            // Skip it
            continue;
          }
        } else {
          matchModality = MatchModality.COMPLETE;
        }

        // 4. We map every table in the query to a table with the same qualified
        // name (all query tables are contained in the view, thus this is equivalent
        // to mapping every table in the query to a view table).
        final Multimap<RelTableRef, RelTableRef> multiMapTables = ArrayListMultimap.create();
        for (RelTableRef queryTableRef1 : queryTableRefs) {
          for (RelTableRef queryTableRef2 : queryTableRefs) {
            if (queryTableRef1.getQualifiedName().equals(
                queryTableRef2.getQualifiedName())) {
              multiMapTables.put(queryTableRef1, queryTableRef2);
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
        for (BiMap<RelTableRef, RelTableRef> queryToViewTableMapping : flatListMappings) {
          // TableMapping : mapping query tables -> view tables
          // 4.0. If compensation equivalence classes exist, we need to add
          // the mapping to the query mapping
          final EquivalenceClasses currQEC = EquivalenceClasses.copy(qEC);
          if (matchModality == MatchModality.QUERY_PARTIAL) {
            for (Entry<RexTableInputRef, RexTableInputRef> e
                : compensationEquiColumns.entries()) {
              // Copy origin
              RelTableRef queryTableRef = queryToViewTableMapping.inverse().get(
                  e.getKey().getTableRef());
              RexTableInputRef queryColumnRef = RexTableInputRef.of(queryTableRef,
                  e.getKey().getIndex(), e.getKey().getType());
              // Add to query equivalence classes and table mapping
              currQEC.addEquivalenceClass(queryColumnRef, e.getValue());
              queryToViewTableMapping.put(e.getValue().getTableRef(),
                  e.getValue().getTableRef()); // identity
            }
          }

          // 4.1. Compute compensation predicates, i.e., predicates that need to be
          // enforced over the view to retain query semantics. The resulting predicates
          // are expressed using {@link RexTableInputRef} over the query.
          // First, to establish relationship, we swap column references of the view
          // predicates to point to query tables and compute equivalence classes.
          final RexNode viewColumnsEquiPred = RexUtil.swapTableReferences(
              rexBuilder, viewPreds.left, queryToViewTableMapping.inverse());
          final EquivalenceClasses queryBasedVEC = new EquivalenceClasses();
          for (RexNode conj : RelOptUtil.conjunctions(viewColumnsEquiPred)) {
            assert conj.isA(SqlKind.EQUALS);
            RexCall equiCond = (RexCall) conj;
            queryBasedVEC.addEquivalenceClass(
                (RexTableInputRef) equiCond.getOperands().get(0),
                (RexTableInputRef) equiCond.getOperands().get(1));
          }
          Pair<RexNode, RexNode> compensationPreds =
              computeCompensationPredicates(rexBuilder, simplify,
                  currQEC, queryPreds, queryBasedVEC, viewPreds,
                  queryToViewTableMapping);
          if (compensationPreds == null && generateUnionRewriting) {
            // Attempt partial rewriting using union operator. This rewriting
            // will read some data from the view and the rest of the data from
            // the query computation. The resulting predicates are expressed
            // using {@link RexTableInputRef} over the view.
            compensationPreds = computeCompensationPredicates(rexBuilder, simplify,
                queryBasedVEC, viewPreds, currQEC, queryPreds,
                queryToViewTableMapping.inverse());
            if (compensationPreds == null) {
              // This was our last chance to use the view, skip it
              continue;
            }
            RexNode compensationColumnsEquiPred = compensationPreds.left;
            RexNode otherCompensationPred = compensationPreds.right;
            assert !compensationColumnsEquiPred.isAlwaysTrue()
                || !otherCompensationPred.isAlwaysTrue();

            // b. Generate union branch (query).
            final RelNode unionInputQuery = rewriteQuery(call.builder(), rexBuilder,
                simplify, mq, compensationColumnsEquiPred, otherCompensationPred,
                topProject, node, queryToViewTableMapping, queryBasedVEC, currQEC);
            if (unionInputQuery == null) {
              // Skip it
              continue;
            }

            // c. Generate union branch (view).
            // We trigger the unifying method. This method will either create a Project
            // or an Aggregate operator on top of the view. It will also compute the
            // output expressions for the query.
            final RelNode unionInputView = rewriteView(call.builder(), rexBuilder, simplify, mq,
                matchModality, true, view, topProject, node, topViewProject, viewNode,
                queryToViewTableMapping, currQEC);
            if (unionInputView == null) {
              // Skip it
              continue;
            }

            // d. Generate final rewriting (union).
            final RelNode result = createUnion(call.builder(), rexBuilder,
                topProject, unionInputQuery, unionInputView);
            if (result == null) {
              // Skip it
              continue;
            }
            call.transformTo(result);
          } else if (compensationPreds != null) {
            RexNode compensationColumnsEquiPred = compensationPreds.left;
            RexNode otherCompensationPred = compensationPreds.right;

            // a. Compute final compensation predicate.
            if (!compensationColumnsEquiPred.isAlwaysTrue()
                || !otherCompensationPred.isAlwaysTrue()) {
              // All columns required by compensating predicates must be contained
              // in the view output (condition 2).
              List<RexNode> viewExprs = topViewProject == null
                  ? extractReferences(rexBuilder, view)
                  : topViewProject.getChildExps();
              // For compensationColumnsEquiPred, we use the view equivalence classes,
              // since we want to enforce the rest
              if (!compensationColumnsEquiPred.isAlwaysTrue()) {
                compensationColumnsEquiPred = rewriteExpression(rexBuilder, mq,
                    view, viewNode, viewExprs, queryToViewTableMapping.inverse(), queryBasedVEC,
                    false, compensationColumnsEquiPred);
                if (compensationColumnsEquiPred == null) {
                  // Skip it
                  continue;
                }
              }
              // For the rest, we use the query equivalence classes
              if (!otherCompensationPred.isAlwaysTrue()) {
                otherCompensationPred = rewriteExpression(rexBuilder, mq,
                    view, viewNode, viewExprs, queryToViewTableMapping.inverse(), currQEC,
                    true, otherCompensationPred);
                if (otherCompensationPred == null) {
                  // Skip it
                  continue;
                }
              }
            }
            final RexNode viewCompensationPred =
                RexUtil.composeConjunction(rexBuilder,
                    ImmutableList.of(compensationColumnsEquiPred,
                        otherCompensationPred));

            // b. Generate final rewriting if possible.
            // First, we add the compensation predicate (if any) on top of the view.
            // Then, we trigger the unifying method. This method will either create a
            // Project or an Aggregate operator on top of the view. It will also compute
            // the output expressions for the query.
            RelBuilder builder = call.builder().transform(c -> c.withPruneInputOfAggregate(false));
            RelNode viewWithFilter;
            if (!viewCompensationPred.isAlwaysTrue()) {
              RexNode newPred =
                  simplify.simplifyUnknownAsFalse(viewCompensationPred);
              viewWithFilter = builder.push(view).filter(newPred).build();
              // No need to do anything if it's a leaf node.
              if (viewWithFilter.getInputs().isEmpty()) {
                call.transformTo(viewWithFilter);
                return;
              }
              // We add (and push) the filter to the view plan before triggering the rewriting.
              // This is useful in case some of the columns can be folded to same value after
              // filter is added.
              Pair<RelNode, RelNode> pushedNodes =
                  pushFilterToOriginalViewPlan(builder, topViewProject, viewNode, newPred);
              topViewProject = (Project) pushedNodes.left;
              viewNode = pushedNodes.right;
            } else {
              viewWithFilter = builder.push(view).build();
            }
            final RelNode result = rewriteView(builder, rexBuilder, simplify, mq, matchModality,
                false, viewWithFilter, topProject, node, topViewProject, viewNode,
                queryToViewTableMapping, currQEC);
            if (result == null) {
              // Skip it
              continue;
            }
            call.transformTo(result);
          } // end else
        }
      }
    }
  }

  protected abstract boolean isValidPlan(Project topProject, RelNode node,
      RelMetadataQuery mq);

  /**
   * It checks whether the query can be rewritten using the view even though the
   * query uses additional tables.
   *
   * <p>Rules implementing the method should follow different approaches depending on the
   * operators they rewrite.
   */
  protected abstract ViewPartialRewriting compensateViewPartial(
      RelBuilder relBuilder, RexBuilder rexBuilder, RelMetadataQuery mq, RelNode input,
      Project topProject, RelNode node, Set<RelTableRef> queryTableRefs, EquivalenceClasses queryEC,
      Project topViewProject, RelNode viewNode, Set<RelTableRef> viewTableRefs);

  /**
   * If the view will be used in a union rewriting, this method is responsible for
   * rewriting the query branch of the union using the given compensation predicate.
   *
   * <p>If a rewriting can be produced, we return that rewriting. If it cannot
   * be produced, we will return null.
   */
  protected abstract RelNode rewriteQuery(
      RelBuilder relBuilder, RexBuilder rexBuilder, RexSimplify simplify, RelMetadataQuery mq,
      RexNode compensationColumnsEquiPred, RexNode otherCompensationPred,
      Project topProject, RelNode node,
      BiMap<RelTableRef, RelTableRef> viewToQueryTableMapping,
      EquivalenceClasses viewEC, EquivalenceClasses queryEC);

  /**
   * If the view will be used in a union rewriting, this method is responsible for
   * generating the union and any other operator needed on top of it, e.g., a Project
   * operator.
   */
  protected abstract RelNode createUnion(RelBuilder relBuilder, RexBuilder rexBuilder,
      RelNode topProject, RelNode unionInputQuery, RelNode unionInputView);

  /**
   * Rewrites the query using the given view query.
   *
   * <p>The input node is a Scan on the view table and possibly a compensation Filter
   * on top. If a rewriting can be produced, we return that rewriting. If it cannot
   * be produced, we will return null.
   */
  protected abstract RelNode rewriteView(RelBuilder relBuilder, RexBuilder rexBuilder,
      RexSimplify simplify, RelMetadataQuery mq, MatchModality matchModality,
      boolean unionRewriting, RelNode input,
      Project topProject, RelNode node,
      Project topViewProject, RelNode viewNode,
      BiMap<RelTableRef, RelTableRef> queryToViewTableMapping,
      EquivalenceClasses queryEC);

  /**
   * Once we create a compensation predicate, this method is responsible for pushing
   * the resulting filter through the view nodes. This might be useful for rewritings
   * containing Aggregate operators, as some of the grouping columns might be removed,
   * which results in additional matching possibilities.
   *
   * <p>The method will return a pair of nodes: the new top project on the left and
   * the new node on the right.
   */
  protected abstract Pair<RelNode, RelNode> pushFilterToOriginalViewPlan(RelBuilder builder,
      RelNode topViewProject, RelNode viewNode, RexNode cond);


  //~ Methods ----------------------------------------------------------------

  /**
   * If the node is an Aggregate, it returns a list of references to the grouping columns.
   * Otherwise, it returns a list of references to all columns in the node.
   * The returned list is immutable.
   */
  protected List<RexNode> extractReferences(RexBuilder rexBuilder, RelNode node) {
    ImmutableList.Builder<RexNode> exprs = ImmutableList.builder();
    if (node instanceof Aggregate) {
      Aggregate aggregate = (Aggregate) node;
      for (int i = 0; i < aggregate.getGroupCount(); i++) {
        exprs.add(rexBuilder.makeInputRef(aggregate, i));
      }
    } else {
      for (int i = 0; i < node.getRowType().getFieldCount(); i++) {
        exprs.add(rexBuilder.makeInputRef(node, i));
      }
    }
    return exprs.build();
  }

  /**
   * It will flatten a multimap containing table references to table references,
   * producing all possible combinations of mappings. Each of the mappings will
   * be bi-directional.
   */
  protected List<BiMap<RelTableRef, RelTableRef>> generateTableMappings(
      Multimap<RelTableRef, RelTableRef> multiMapTables) {
    if (multiMapTables.isEmpty()) {
      return ImmutableList.of();
    }
    List<BiMap<RelTableRef, RelTableRef>> result =
        ImmutableList.of(
            HashBiMap.create());
    for (Entry<RelTableRef, Collection<RelTableRef>> e : multiMapTables.asMap().entrySet()) {
      if (e.getValue().size() == 1) {
        // Only one reference, we can just add it to every map
        RelTableRef target = e.getValue().iterator().next();
        for (BiMap<RelTableRef, RelTableRef> m : result) {
          m.put(e.getKey(), target);
        }
        continue;
      }
      // Multiple references: flatten
      ImmutableList.Builder<BiMap<RelTableRef, RelTableRef>> newResult =
          ImmutableList.builder();
      for (RelTableRef target : e.getValue()) {
        for (BiMap<RelTableRef, RelTableRef> m : result) {
          if (!m.containsValue(target)) {
            final BiMap<RelTableRef, RelTableRef> newM =
                HashBiMap.create(m);
            newM.put(e.getKey(), target);
            newResult.add(newM);
          }
        }
      }
      result = newResult.build();
    }
    return result;
  }

  /** Currently we only support TableScan - Project - Filter - Inner Join */
  protected boolean isValidRelNodePlan(RelNode node, RelMetadataQuery mq) {
    final Multimap<Class<? extends RelNode>, RelNode> m =
            mq.getNodeTypes(node);
    if (m == null) {
      return false;
    }

    for (Entry<Class<? extends RelNode>, Collection<RelNode>> e : m.asMap().entrySet()) {
      Class<? extends RelNode> c = e.getKey();
      if (!TableScan.class.isAssignableFrom(c)
              && !Project.class.isAssignableFrom(c)
              && !Filter.class.isAssignableFrom(c)
              && (!Join.class.isAssignableFrom(c))) {
        // Skip it
        return false;
      }
      if (Join.class.isAssignableFrom(c)) {
        for (RelNode n : e.getValue()) {
          final Join join = (Join) n;
          if (join.getJoinType() != JoinRelType.INNER && !join.isSemiJoin()) {
            // Skip it
            return false;
          }
        }
      }
    }
    return true;
  }

  /**
   * Classifies each of the predicates in the list into one of these two
   * categories:
   *
   * <ul>
   * <li> 1-l) column equality predicates, or
   * <li> 2-r) residual predicates, all the rest
   * </ul>
   *
   * <p>For each category, it creates the conjunction of the predicates. The
   * result is an pair of RexNode objects corresponding to each category.
   */
  protected Pair<RexNode, RexNode> splitPredicates(
      RexBuilder rexBuilder, RexNode pred) {
    List<RexNode> equiColumnsPreds = new ArrayList<>();
    List<RexNode> residualPreds = new ArrayList<>();
    for (RexNode e : RelOptUtil.conjunctions(pred)) {
      switch (e.getKind()) {
      case EQUALS:
        RexCall eqCall = (RexCall) e;
        if (RexUtil.isReferenceOrAccess(eqCall.getOperands().get(0), false)
                && RexUtil.isReferenceOrAccess(eqCall.getOperands().get(1), false)) {
          equiColumnsPreds.add(e);
        } else {
          residualPreds.add(e);
        }
        break;
      default:
        residualPreds.add(e);
      }
    }
    return Pair.of(
        RexUtil.composeConjunction(rexBuilder, equiColumnsPreds),
        RexUtil.composeConjunction(rexBuilder, residualPreds));
  }

  /**
   * It checks whether the target can be rewritten using the source even though the
   * source uses additional tables. In order to do that, we need to double-check
   * that every join that exists in the source and is not in the target is a
   * cardinality-preserving join, i.e., it only appends columns to the row
   * without changing its multiplicity. Thus, the join needs to be:
   * <ul>
   * <li> Equi-join </li>
   * <li> Between all columns in the keys </li>
   * <li> Foreign-key columns do not allow NULL values </li>
   * <li> Foreign-key </li>
   * <li> Unique-key </li>
   * </ul>
   *
   * <p>If it can be rewritten, it returns true. Further, it inserts the missing equi-join
   * predicates in the input {@code compensationEquiColumns} multimap if it is provided.
   * If it cannot be rewritten, it returns false.
   */
  protected boolean compensatePartial(
      Set<RelTableRef> sourceTableRefs,
      EquivalenceClasses sourceEC,
      Set<RelTableRef> targetTableRefs,
      Multimap<RexTableInputRef, RexTableInputRef> compensationEquiColumns) {
    // Create UK-FK graph with view tables
    final DirectedGraph<RelTableRef, Edge> graph =
        DefaultDirectedGraph.create(Edge::new);
    final Multimap<List<String>, RelTableRef> tableVNameToTableRefs =
        ArrayListMultimap.create();
    final Set<RelTableRef> extraTableRefs = new HashSet<>();
    for (RelTableRef tRef : sourceTableRefs) {
      // Add tables in view as vertices
      graph.addVertex(tRef);
      tableVNameToTableRefs.put(tRef.getQualifiedName(), tRef);
      if (!targetTableRefs.contains(tRef)) {
        // Add to extra tables if table is not part of the query
        extraTableRefs.add(tRef);
      }
    }
    for (RelTableRef tRef : graph.vertexSet()) {
      // Add edges between tables
      List<RelReferentialConstraint> constraints =
          tRef.getTable().getReferentialConstraints();
      for (RelReferentialConstraint constraint : constraints) {
        Collection<RelTableRef> parentTableRefs =
            tableVNameToTableRefs.get(constraint.getTargetQualifiedName());
        for (RelTableRef parentTRef : parentTableRefs) {
          boolean canBeRewritten = true;
          Multimap<RexTableInputRef, RexTableInputRef> equiColumns =
                  ArrayListMultimap.create();
          for (int pos = 0; pos < constraint.getNumColumns(); pos++) {
            int foreignKeyPos = constraint.getColumnPairs().get(pos).source;
            RelDataType foreignKeyColumnType =
                tRef.getTable().getRowType().getFieldList().get(foreignKeyPos).getType();
            RexTableInputRef foreignKeyColumnRef =
                RexTableInputRef.of(tRef, foreignKeyPos, foreignKeyColumnType);
            int uniqueKeyPos = constraint.getColumnPairs().get(pos).target;
            RexTableInputRef uniqueKeyColumnRef = RexTableInputRef.of(parentTRef, uniqueKeyPos,
                parentTRef.getTable().getRowType().getFieldList().get(uniqueKeyPos).getType());
            if (!foreignKeyColumnType.isNullable()
                && sourceEC.getEquivalenceClassesMap().containsKey(uniqueKeyColumnRef)
                && sourceEC.getEquivalenceClassesMap().get(uniqueKeyColumnRef)
                    .contains(foreignKeyColumnRef)) {
              equiColumns.put(foreignKeyColumnRef, uniqueKeyColumnRef);
            } else {
              canBeRewritten = false;
              break;
            }
          }
          if (canBeRewritten) {
            // Add edge FK -> UK
            Edge edge = graph.getEdge(tRef, parentTRef);
            if (edge == null) {
              edge = graph.addEdge(tRef, parentTRef);
            }
            edge.equiColumns.putAll(equiColumns);
          }
        }
      }
    }

    // Try to eliminate tables from graph: if we can do it, it means extra tables in
    // view are cardinality-preserving joins
    boolean done = false;
    do {
      List<RelTableRef> nodesToRemove = new ArrayList<>();
      for (RelTableRef tRef : graph.vertexSet()) {
        if (graph.getInwardEdges(tRef).size() == 1
            && graph.getOutwardEdges(tRef).isEmpty()) {
          // UK-FK join
          nodesToRemove.add(tRef);
          if (compensationEquiColumns != null && extraTableRefs.contains(tRef)) {
            // We need to add to compensation columns as the table is not present in the query
            compensationEquiColumns.putAll(graph.getInwardEdges(tRef).get(0).equiColumns);
          }
        }
      }
      if (!nodesToRemove.isEmpty()) {
        graph.removeAllVertices(nodesToRemove);
      } else {
        done = true;
      }
    } while (!done);

    // After removing them, we check whether all the remaining tables in the graph
    // are tables present in the query: if they are, we can try to rewrite
    if (!Collections.disjoint(graph.vertexSet(), extraTableRefs)) {
      return false;
    }
    return true;
  }

  /**
   * We check whether the predicates in the source are contained in the predicates
   * in the target. The method treats separately the equi-column predicates, the
   * range predicates, and the rest of predicates.
   *
   * <p>If the containment is confirmed, we produce compensation predicates that
   * need to be added to the target to produce the results in the source. Thus,
   * if source and target expressions are equivalent, those predicates will be the
   * true constant.
   *
   * <p>In turn, if containment cannot be confirmed, the method returns null.
   */
  protected Pair<RexNode, RexNode> computeCompensationPredicates(
      RexBuilder rexBuilder,
      RexSimplify simplify,
      EquivalenceClasses sourceEC,
      Pair<RexNode, RexNode> sourcePreds,
      EquivalenceClasses targetEC,
      Pair<RexNode, RexNode> targetPreds,
      BiMap<RelTableRef, RelTableRef> sourceToTargetTableMapping) {
    final RexNode compensationColumnsEquiPred;
    final RexNode compensationPred;

    // 1. Establish relationship between source and target equivalence classes.
    // If every target equivalence class is not a subset of a source
    // equivalence class, we bail out.
    compensationColumnsEquiPred = generateEquivalenceClasses(
        rexBuilder, sourceEC, targetEC);
    if (compensationColumnsEquiPred == null) {
      // Cannot rewrite
      return null;
    }

    // 2. We check that that residual predicates of the source are satisfied within the target.
    // Compute compensating predicates.
    final RexNode queryPred = RexUtil.swapColumnReferences(
        rexBuilder, sourcePreds.right, sourceEC.getEquivalenceClassesMap());
    final RexNode viewPred = RexUtil.swapTableColumnReferences(
        rexBuilder, targetPreds.right, sourceToTargetTableMapping.inverse(),
        sourceEC.getEquivalenceClassesMap());
    compensationPred = SubstitutionVisitor.splitFilter(
        simplify, queryPred, viewPred);
    if (compensationPred == null) {
      // Cannot rewrite
      return null;
    }

    return Pair.of(compensationColumnsEquiPred, compensationPred);
  }

  /**
   * Given the equi-column predicates of the source and the target and the
   * computed equivalence classes, it extracts possible mappings between
   * the equivalence classes.
   *
   * <p>If there is no mapping, it returns null. If there is a exact match,
   * it will return a compensation predicate that evaluates to true.
   * Finally, if a compensation predicate needs to be enforced on top of
   * the target to make the equivalences classes match, it returns that
   * compensation predicate.
   */
  protected RexNode generateEquivalenceClasses(RexBuilder rexBuilder,
      EquivalenceClasses sourceEC, EquivalenceClasses targetEC) {
    if (sourceEC.getEquivalenceClasses().isEmpty() && targetEC.getEquivalenceClasses().isEmpty()) {
      // No column equality predicates in query and view
      // Empty mapping and compensation predicate
      return rexBuilder.makeLiteral(true);
    }
    if (sourceEC.getEquivalenceClasses().isEmpty() && !targetEC.getEquivalenceClasses().isEmpty()) {
      // No column equality predicates in source, but column equality predicates in target
      return null;
    }

    final List<Set<RexTableInputRef>> sourceEquivalenceClasses = sourceEC.getEquivalenceClasses();
    final List<Set<RexTableInputRef>> targetEquivalenceClasses = targetEC.getEquivalenceClasses();
    final Multimap<Integer, Integer> mapping = extractPossibleMapping(
        sourceEquivalenceClasses, targetEquivalenceClasses);
    if (mapping == null) {
      // Did not find mapping between the equivalence classes,
      // bail out
      return null;
    }

    // Create the compensation predicate
    RexNode compensationPredicate = rexBuilder.makeLiteral(true);
    for (int i = 0; i < sourceEquivalenceClasses.size(); i++) {
      if (!mapping.containsKey(i)) {
        // Add all predicates
        Iterator<RexTableInputRef> it = sourceEquivalenceClasses.get(i).iterator();
        RexTableInputRef e0 = it.next();
        while (it.hasNext()) {
          RexNode equals = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
              e0, it.next());
          compensationPredicate = rexBuilder.makeCall(SqlStdOperatorTable.AND,
              compensationPredicate, equals);
        }
      } else {
        // Add only predicates that are not there
        for (int j : mapping.get(i)) {
          Set<RexTableInputRef> difference = new HashSet<>(
              sourceEquivalenceClasses.get(i));
          difference.removeAll(targetEquivalenceClasses.get(j));
          for (RexTableInputRef e : difference) {
            RexNode equals = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                e, targetEquivalenceClasses.get(j).iterator().next());
            compensationPredicate = rexBuilder.makeCall(SqlStdOperatorTable.AND,
                compensationPredicate, equals);
          }
        }
      }
    }
    return compensationPredicate;
  }

  /**
   * Given the source and target equivalence classes, it extracts the possible mappings
   * from each source equivalence class to each target equivalence class.
   *
   * <p>If any of the source equivalence classes cannot be mapped to a target equivalence
   * class, it returns null.
   */
  protected Multimap<Integer, Integer> extractPossibleMapping(
      List<Set<RexTableInputRef>> sourceEquivalenceClasses,
      List<Set<RexTableInputRef>> targetEquivalenceClasses) {
    Multimap<Integer, Integer> mapping = ArrayListMultimap.create();
    for (int i = 0; i < targetEquivalenceClasses.size(); i++) {
      boolean foundQueryEquivalenceClass = false;
      final Set<RexTableInputRef> viewEquivalenceClass = targetEquivalenceClasses.get(i);
      for (int j = 0; j < sourceEquivalenceClasses.size(); j++) {
        final Set<RexTableInputRef> queryEquivalenceClass = sourceEquivalenceClasses.get(j);
        if (queryEquivalenceClass.containsAll(viewEquivalenceClass)) {
          mapping.put(j, i);
          foundQueryEquivalenceClass = true;
          break;
        }
      } // end for

      if (!foundQueryEquivalenceClass) {
        // Target equivalence class not found in source equivalence class
        return null;
      }
    } // end for

    return mapping;
  }

  /**
   * First, the method takes the node expressions {@code nodeExprs} and swaps the table
   * and column references using the table mapping and the equivalence classes.
   * If {@code swapTableColumn} is true, it swaps the table reference and then the column reference,
   * otherwise it swaps the column reference and then the table reference.
   *
   * <p>Then, the method will rewrite the input expression {@code exprToRewrite}, replacing the
   * {@link RexTableInputRef} by references to the positions in {@code nodeExprs}.
   *
   * <p>The method will return the rewritten expression. If any of the expressions in the input
   * expression cannot be mapped, it will return null.
   */
  protected RexNode rewriteExpression(
      RexBuilder rexBuilder,
      RelMetadataQuery mq,
      RelNode targetNode,
      RelNode node,
      List<RexNode> nodeExprs,
      BiMap<RelTableRef, RelTableRef> tableMapping,
      EquivalenceClasses ec,
      boolean swapTableColumn,
      RexNode exprToRewrite) {
    List<RexNode> rewrittenExprs = rewriteExpressions(rexBuilder, mq, targetNode, node, nodeExprs,
        tableMapping, ec, swapTableColumn, ImmutableList.of(exprToRewrite));
    if (rewrittenExprs == null) {
      return null;
    }
    assert rewrittenExprs.size() == 1;
    return rewrittenExprs.get(0);
  }

  /**
   * First, the method takes the node expressions {@code nodeExprs} and swaps the table
   * and column references using the table mapping and the equivalence classes.
   * If {@code swapTableColumn} is true, it swaps the table reference and then the column reference,
   * otherwise it swaps the column reference and then the table reference.
   *
   * <p>Then, the method will rewrite the input expressions {@code exprsToRewrite}, replacing the
   * {@link RexTableInputRef} by references to the positions in {@code nodeExprs}.
   *
   * <p>The method will return the rewritten expressions. If any of the subexpressions in the input
   * expressions cannot be mapped, it will return null.
   */
  protected List<RexNode> rewriteExpressions(
      RexBuilder rexBuilder,
      RelMetadataQuery mq,
      RelNode targetNode,
      RelNode node,
      List<RexNode> nodeExprs,
      BiMap<RelTableRef, RelTableRef> tableMapping,
      EquivalenceClasses ec,
      boolean swapTableColumn,
      List<RexNode> exprsToRewrite) {
    NodeLineage nodeLineage;
    if (swapTableColumn) {
      nodeLineage = generateSwapTableColumnReferencesLineage(rexBuilder, mq, node,
          tableMapping, ec, nodeExprs);
    } else {
      nodeLineage = generateSwapColumnTableReferencesLineage(rexBuilder, mq, node,
          tableMapping, ec, nodeExprs);
    }

    List<RexNode> rewrittenExprs = new ArrayList<>(exprsToRewrite.size());
    for (RexNode exprToRewrite : exprsToRewrite) {
      RexNode rewrittenExpr = replaceWithOriginalReferences(
          rexBuilder, targetNode, nodeLineage, exprToRewrite);
      if (RexUtil.containsTableInputRef(rewrittenExpr) != null) {
        // Some expressions were not present in view output
        return null;
      }
      rewrittenExprs.add(rewrittenExpr);
    }
    return rewrittenExprs;
  }

  /**
   * It swaps the table references and then the column references of the input
   * expressions using the table mapping and the equivalence classes.
   */
  protected NodeLineage generateSwapTableColumnReferencesLineage(
      RexBuilder rexBuilder,
      RelMetadataQuery mq,
      RelNode node,
      BiMap<RelTableRef, RelTableRef> tableMapping,
      EquivalenceClasses ec,
      List<RexNode> nodeExprs) {
    final Map<RexNode, Integer> exprsLineage = new HashMap<>();
    final Map<RexNode, Integer> exprsLineageLosslessCasts = new HashMap<>();
    for (int i = 0; i < nodeExprs.size(); i++) {
      final Set<RexNode> s = mq.getExpressionLineage(node, nodeExprs.get(i));
      if (s == null) {
        // Next expression
        continue;
      }
      // We only support project - filter - join, thus it should map to
      // a single expression
      assert s.size() == 1;
      // Rewrite expr. First we swap the table references following the table
      // mapping, then we take first element from the corresponding equivalence class
      final RexNode e = RexUtil.swapTableColumnReferences(rexBuilder,
          s.iterator().next(), tableMapping, ec.getEquivalenceClassesMap());
      exprsLineage.put(e, i);
      if (RexUtil.isLosslessCast(e)) {
        exprsLineageLosslessCasts.put(((RexCall) e).getOperands().get(0), i);
      }
    }
    return new NodeLineage(exprsLineage, exprsLineageLosslessCasts);
  }

  /**
   * It swaps the column references and then the table references of the input
   * expressions using the equivalence classes and the table mapping.
   */
  protected NodeLineage generateSwapColumnTableReferencesLineage(
      RexBuilder rexBuilder,
      RelMetadataQuery mq,
      RelNode node,
      BiMap<RelTableRef, RelTableRef> tableMapping,
      EquivalenceClasses ec,
      List<RexNode> nodeExprs) {
    final Map<RexNode, Integer> exprsLineage = new HashMap<>();
    final Map<RexNode, Integer> exprsLineageLosslessCasts = new HashMap<>();
    for (int i = 0; i < nodeExprs.size(); i++) {
      final Set<RexNode> s = mq.getExpressionLineage(node, nodeExprs.get(i));
      if (s == null) {
        // Next expression
        continue;
      }
      // We only support project - filter - join, thus it should map to
      // a single expression
      final RexNode node2 = Iterables.getOnlyElement(s);
      // Rewrite expr. First we take first element from the corresponding equivalence class,
      // then we swap the table references following the table mapping
      final RexNode e = RexUtil.swapColumnTableReferences(rexBuilder, node2,
          ec.getEquivalenceClassesMap(), tableMapping);
      exprsLineage.put(e, i);
      if (RexUtil.isLosslessCast(e)) {
        exprsLineageLosslessCasts.put(((RexCall) e).getOperands().get(0), i);
      }
    }
    return new NodeLineage(exprsLineage, exprsLineageLosslessCasts);
  }

  /**
   * Given the input expression, it will replace (sub)expressions when possible
   * using the content of the mapping. In particular, the mapping contains the
   * digest of the expression and the index that the replacement input ref should
   * point to.
   */
  protected RexNode replaceWithOriginalReferences(final RexBuilder rexBuilder,
      final RelNode node, final NodeLineage nodeLineage, final RexNode exprToRewrite) {
    // Currently we allow the following:
    // 1) compensation pred can be directly map to expression
    // 2) all references in compensation pred can be map to expressions
    // We support bypassing lossless casts.
    RexShuttle visitor =
        new RexShuttle() {
          @Override public RexNode visitCall(RexCall call) {
            RexNode rw = replace(call);
            return rw != null ? rw : super.visitCall(call);
          }

          @Override public RexNode visitTableInputRef(RexTableInputRef inputRef) {
            RexNode rw = replace(inputRef);
            return rw != null ? rw : super.visitTableInputRef(inputRef);
          }

          private RexNode replace(RexNode e) {
            Integer pos = nodeLineage.exprsLineage.get(e);
            if (pos != null) {
              // Found it
              return rexBuilder.makeInputRef(node, pos);
            }
            pos = nodeLineage.exprsLineageLosslessCasts.get(e);
            if (pos != null) {
              // Found it
              return rexBuilder.makeCast(
                  e.getType(), rexBuilder.makeInputRef(node, pos));
            }
            return null;
          }
        };
    return visitor.apply(exprToRewrite);
  }

  /**
   * Replaces all the input references by the position in the
   * input column set. If a reference index cannot be found in
   * the input set, then we return null.
   */
  protected RexNode shuttleReferences(final RexBuilder rexBuilder,
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
   * Replaces all the possible sub-expressions by input references
   * to the input node.
   */
  protected RexNode shuttleReferences(final RexBuilder rexBuilder,
      final RexNode expr, final Multimap<RexNode, Integer> exprsLineage) {
    return shuttleReferences(rexBuilder, expr,
        exprsLineage, null, null);
  }

  /**
   * Replaces all the possible sub-expressions by input references
   * to the input node. If available, it uses the rewriting mapping
   * to change the position to reference. Takes the reference type
   * from the input node.
   */
  protected RexNode shuttleReferences(final RexBuilder rexBuilder,
      final RexNode expr, final Multimap<RexNode, Integer> exprsLineage,
      final RelNode node, final Multimap<Integer, Integer> rewritingMapping) {
    try {
      RexShuttle visitor =
          new RexShuttle() {
            @Override public RexNode visitTableInputRef(RexTableInputRef ref) {
              Collection<Integer> c = exprsLineage.get(ref);
              if (c.isEmpty()) {
                // Cannot map expression
                throw Util.FoundOne.NULL;
              }
              int pos = c.iterator().next();
              if (rewritingMapping != null) {
                pos = rewritingMapping.get(pos).iterator().next();
                if (pos == -1) {
                  // Cannot map expression
                  throw Util.FoundOne.NULL;
                }
              }
              if (node != null) {
                return rexBuilder.makeInputRef(node, pos);
              }
              return rexBuilder.makeInputRef(ref.getType(), pos);
            }

            @Override public RexNode visitInputRef(RexInputRef inputRef) {
              Collection<Integer> c = exprsLineage.get(inputRef);
              if (c.isEmpty()) {
                // Cannot map expression
                throw Util.FoundOne.NULL;
              }
              int pos = c.iterator().next();
              if (rewritingMapping != null) {
                pos = rewritingMapping.get(pos).iterator().next();
                if (pos == -1) {
                  // Cannot map expression
                  throw Util.FoundOne.NULL;
                }
              }
              if (node != null) {
                return rexBuilder.makeInputRef(node, pos);
              }
              return rexBuilder.makeInputRef(inputRef.getType(), pos);
            }

            @Override public RexNode visitCall(final RexCall call) {
              Collection<Integer> c = exprsLineage.get(call);
              if (c.isEmpty()) {
                // Cannot map expression
                return super.visitCall(call);
              }
              int pos = c.iterator().next();
              if (rewritingMapping != null) {
                pos = rewritingMapping.get(pos).iterator().next();
                if (pos == -1) {
                  // Cannot map expression
                  return super.visitCall(call);
                }
              }
              if (node != null) {
                return rexBuilder.makeInputRef(node, pos);
              }
              return rexBuilder.makeInputRef(call.getType(), pos);
            }
          };
      return visitor.apply(expr);
    } catch (Util.FoundOne ex) {
      Util.swallow(ex, null);
      return null;
    }
  }

  /**
   * Class representing an equivalence class, i.e., a set of equivalent columns
   */
  protected static class EquivalenceClasses {

    private final Map<RexTableInputRef, Set<RexTableInputRef>> nodeToEquivalenceClass;
    private Map<RexTableInputRef, Set<RexTableInputRef>> cacheEquivalenceClassesMap;
    private List<Set<RexTableInputRef>> cacheEquivalenceClasses;

    protected EquivalenceClasses() {
      nodeToEquivalenceClass = new HashMap<>();
      cacheEquivalenceClassesMap = ImmutableMap.of();
      cacheEquivalenceClasses = ImmutableList.of();
    }

    protected void addEquivalenceClass(RexTableInputRef p1, RexTableInputRef p2) {
      // Clear cache
      cacheEquivalenceClassesMap = null;
      cacheEquivalenceClasses = null;

      Set<RexTableInputRef> c1 = nodeToEquivalenceClass.get(p1);
      Set<RexTableInputRef> c2 = nodeToEquivalenceClass.get(p2);
      if (c1 != null && c2 != null) {
        // Both present, we need to merge
        if (c1.size() < c2.size()) {
          // We swap them to merge
          Set<RexTableInputRef> c2Temp = c2;
          c2 = c1;
          c1 = c2Temp;
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
      if (cacheEquivalenceClassesMap == null) {
        cacheEquivalenceClassesMap = ImmutableMap.copyOf(nodeToEquivalenceClass);
      }
      return cacheEquivalenceClassesMap;
    }

    protected List<Set<RexTableInputRef>> getEquivalenceClasses() {
      if (cacheEquivalenceClasses == null) {
        Set<RexTableInputRef> visited = new HashSet<>();
        ImmutableList.Builder<Set<RexTableInputRef>> builder =
                ImmutableList.builder();
        for (Set<RexTableInputRef> set : nodeToEquivalenceClass.values()) {
          if (Collections.disjoint(visited, set)) {
            builder.add(set);
            visited.addAll(set);
          }
        }
        cacheEquivalenceClasses = builder.build();
      }
      return cacheEquivalenceClasses;
    }

    protected static EquivalenceClasses copy(EquivalenceClasses ec) {
      final EquivalenceClasses newEc = new EquivalenceClasses();
      for (Entry<RexTableInputRef, Set<RexTableInputRef>> e
          : ec.nodeToEquivalenceClass.entrySet()) {
        newEc.nodeToEquivalenceClass.put(
            e.getKey(), Sets.newLinkedHashSet(e.getValue()));
      }
      newEc.cacheEquivalenceClassesMap = null;
      newEc.cacheEquivalenceClasses = null;
      return newEc;
    }
  }

  /** Expression lineage details. */
  protected static class NodeLineage {
    private final Map<RexNode, Integer> exprsLineage;
    private final Map<RexNode, Integer> exprsLineageLosslessCasts;

    private NodeLineage(Map<RexNode, Integer> exprsLineage,
        Map<RexNode, Integer> exprsLineageLosslessCasts) {
      this.exprsLineage = ImmutableMap.copyOf(exprsLineage);
      this.exprsLineageLosslessCasts =
          ImmutableMap.copyOf(exprsLineageLosslessCasts);
    }
  }

  /** Edge for graph */
  protected static class Edge extends DefaultEdge {
    final Multimap<RexTableInputRef, RexTableInputRef> equiColumns =
        ArrayListMultimap.create();

    Edge(RelTableRef source, RelTableRef target) {
      super(source, target);
    }

    public String toString() {
      return "{" + source + " -> " + target + "}";
    }
  }

  /** View partitioning result */
  protected static class ViewPartialRewriting {
    private final RelNode newView;
    private final Project newTopViewProject;
    private final RelNode newViewNode;

    private ViewPartialRewriting(RelNode newView, Project newTopViewProject, RelNode newViewNode) {
      this.newView = newView;
      this.newTopViewProject = newTopViewProject;
      this.newViewNode = newViewNode;
    }

    protected static ViewPartialRewriting of(
        RelNode newView, Project newTopViewProject, RelNode newViewNode) {
      return new ViewPartialRewriting(newView, newTopViewProject, newViewNode);
    }
  }

  /** Complete, view partial, or query partial. */
  protected enum MatchModality {
    COMPLETE,
    VIEW_PARTIAL,
    QUERY_PARTIAL
  }

}
