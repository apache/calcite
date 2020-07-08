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

import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexTableInputRef.RelTableRef;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Materialized view rewriting for join.
 *
 * @param <C> Configuration type
 */
public abstract class MaterializedViewJoinRule<C extends MaterializedViewJoinRule.Config>
    extends MaterializedViewRule<C> {

  /** Creates a MaterializedViewJoinRule. */
  MaterializedViewJoinRule(C config) {
    super(config);
  }

  @Override protected boolean isValidPlan(Project topProject, RelNode node,
      RelMetadataQuery mq) {
    return isValidRelNodePlan(node, mq);
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
    // We only create the rewriting in the minimal subtree of plan operators.
    // Otherwise we will produce many EQUAL rewritings at different levels of
    // the plan.
    // View: (A JOIN B) JOIN C
    // Query: (((A JOIN B) JOIN D) JOIN C) JOIN E
    // We produce it at:
    // ((A JOIN B) JOIN D) JOIN C
    // But not at:
    // (((A JOIN B) JOIN D) JOIN C) JOIN E
    if (config.fastBailOut()) {
      for (RelNode joinInput : node.getInputs()) {
        if (mq.getTableReferences(joinInput).containsAll(viewTableRefs)) {
          return null;
        }
      }
    }

    // Extract tables that are in the query and not in the view
    final Set<RelTableRef> extraTableRefs = new HashSet<>();
    for (RelTableRef tRef : queryTableRefs) {
      if (!viewTableRefs.contains(tRef)) {
        // Add to extra tables if table is not part of the view
        extraTableRefs.add(tRef);
      }
    }

    // Rewrite the view and the view plan. We only need to add the missing
    // tables on top of the view and view plan using a cartesian product.
    // Then the rest of the rewriting algorithm can be executed in the same
    // fashion, and if there are predicates between the existing and missing
    // tables, the rewriting algorithm will enforce them.
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

    relBuilder.push(topViewProject != null ? topViewProject : viewNode);
    for (RelNode newRel : newRels) {
      // Add to the view plan
      relBuilder.push(newRel);
      relBuilder.join(JoinRelType.INNER, rexBuilder.makeLiteral(true));
    }
    final RelNode newViewNode = relBuilder.build();

    return ViewPartialRewriting.of(newView, null, newViewNode);
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
      BiMap<RelTableRef, RelTableRef> viewToQueryTableMapping,
      EquivalenceClasses viewEC, EquivalenceClasses queryEC) {
    // Our target node is the node below the root, which should have the maximum
    // number of available expressions in the tree in order to maximize our
    // number of rewritings.
    // We create a project on top. If the program is available, we execute
    // it to maximize rewriting opportunities. For instance, a program might
    // pull up all the expressions that are below the aggregate so we can
    // introduce compensation filters easily. This is important depending on
    // the planner strategy.
    RelNode newNode = node;
    RelNode target = node;
    if (config.unionRewritingPullProgram() != null) {
      final HepPlanner tmpPlanner =
          new HepPlanner(config.unionRewritingPullProgram());
      tmpPlanner.setRoot(newNode);
      newNode = tmpPlanner.findBestExp();
      target = newNode.getInput(0);
    }

    // All columns required by compensating predicates must be contained
    // in the query.
    List<RexNode> queryExprs = extractReferences(rexBuilder, target);

    if (!compensationColumnsEquiPred.isAlwaysTrue()) {
      compensationColumnsEquiPred = rewriteExpression(rexBuilder, mq,
          target, target, queryExprs, viewToQueryTableMapping.inverse(), queryEC, false,
          compensationColumnsEquiPred);
      if (compensationColumnsEquiPred == null) {
        // Skip it
        return null;
      }
    }
    // For the rest, we use the query equivalence classes
    if (!otherCompensationPred.isAlwaysTrue()) {
      otherCompensationPred = rewriteExpression(rexBuilder, mq,
          target, target, queryExprs, viewToQueryTableMapping.inverse(), viewEC, true,
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
    if (config.unionRewritingPullProgram() != null) {
      rewrittenPlan = newNode.copy(
          newNode.getTraitSet(), ImmutableList.of(rewrittenPlan));
    }
    if (topProject != null) {
      return topProject.copy(topProject.getTraitSet(), ImmutableList.of(rewrittenPlan));
    }
    return rewrittenPlan;
  }

  @Override protected RelNode createUnion(RelBuilder relBuilder, RexBuilder rexBuilder,
      RelNode topProject, RelNode unionInputQuery, RelNode unionInputView) {
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
    return relBuilder.build();
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
    List<RexNode> exprs = topProject == null
        ? extractReferences(rexBuilder, node)
        : topProject.getProjects();
    List<RexNode> exprsLineage = new ArrayList<>(exprs.size());
    for (RexNode expr : exprs) {
      Set<RexNode> s = mq.getExpressionLineage(node, expr);
      if (s == null) {
        // Bail out
        return null;
      }
      assert s.size() == 1;
      // Rewrite expr. Take first element from the corresponding equivalence class
      // (no need to swap the table references following the table mapping)
      exprsLineage.add(
          RexUtil.swapColumnReferences(rexBuilder,
              s.iterator().next(), queryEC.getEquivalenceClassesMap()));
    }
    List<RexNode> viewExprs = topViewProject == null
        ? extractReferences(rexBuilder, viewNode)
        : topViewProject.getProjects();
    List<RexNode> rewrittenExprs = rewriteExpressions(rexBuilder, mq, input, viewNode, viewExprs,
        queryToViewTableMapping.inverse(), queryEC, true, exprsLineage);
    if (rewrittenExprs == null) {
      return null;
    }
    return relBuilder
        .push(input)
        .project(rewrittenExprs)
        .convert(topProject != null ? topProject.getRowType() : node.getRowType(), false)
        .build();
  }

  @Override public Pair<RelNode, RelNode> pushFilterToOriginalViewPlan(RelBuilder builder,
      RelNode topViewProject, RelNode viewNode, RexNode cond) {
    // Nothing to do
    return Pair.of(topViewProject, viewNode);
  }
}
