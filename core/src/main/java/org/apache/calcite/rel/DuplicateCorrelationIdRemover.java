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
package org.apache.calcite.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.core.ConditionalCorrelate;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Gives new ids to correlation variables, so that different variables of a
 * plan have different ids (enforcing invariant 1 of {@link CorrelateInvariantChecker}).
 *
 * <p>Updates a plan which reuses the same CorrelationId multiple times to use
 * each CorrelationId only once.  {@link CorrelateInvariantChecker} considers such
 * plans illegal. This analysis converts plans with duplicate ids into plans
 * where each id is used only once.  Each id refers to the nearest declaration
 * that has the use in its scope.
 */
public class DuplicateCorrelationIdRemover {
  private final RelOptCluster cluster;

  /** Each id that the plan declares or uses. A new id is not one of them. */
  private final Set<CorrelationId> idsInPlan = new HashSet<>();

  /** Each id that a declaration has in the plan that this remover returns. */
  private final Set<CorrelationId> declaredIds = new HashSet<>();

  /** For each node, the ids of the free variable uses of the sub-plan that has this
   * node as its root. */
  private final IdentityHashMap<RelNode, Set<CorrelationId>> freeIds =
      new IdentityHashMap<>();

  /** For each node, and each renaming of its free ids, the node that replaces
   * it in the plan that this remover returns. */
  private final IdentityHashMap<RelNode, Map<Map<CorrelationId, CorrelationId>, RelNode>>
      renamed = new IdentityHashMap<>();

  private DuplicateCorrelationIdRemover(RelOptCluster cluster) {
    this.cluster = cluster;
  }

  /** Returns a plan in which different variables have different ids. Returns
   * {@code plan} if it obeys this rule already. */
  public static RelNode makeIdsUnique(RelNode plan) {
    final DuplicateCorrelationIdRemover remover =
        new DuplicateCorrelationIdRemover(plan.getCluster());
    remover.freeIdsOf(plan);
    return remover.rename(plan, ImmutableMap.of());
  }

  /** Returns a plan in which each use of a free variable whose id is a key of
   * {@code renaming} has the corresponding value as its id (alpha-renaming).
   *
   * <p>The renaming avoids capture: a declaration in the plan that has one of
   * the new ids gets another id, so that it does not hide a renamed use. As
   * {@link #makeIdsUnique} does, the renaming also gives new ids to repeated
   * declarations.
   *
   * @param renaming New id of each free id that changes; a new id must not be
   *                 a free id of the plan
   */
  public static RelNode renameFreeIds(RelNode plan, Map<CorrelationId, CorrelationId> renaming) {
    final DuplicateCorrelationIdRemover remover =
        new DuplicateCorrelationIdRemover(plan.getCluster());
    remover.freeIdsOf(plan);
    remover.idsInPlan.addAll(renaming.values());
    remover.declaredIds.addAll(renaming.values());
    return remover.rename(plan, renaming);
  }

  /** Returns the ids of the free variables of a sub-plan, and adds each id of the
   * sub-plan to {@link #idsInPlan}. */
  private Set<CorrelationId> freeIdsOf(RelNode node0) {
    final RelNode node = node0.stripped();
    final Set<CorrelationId> known = freeIds.get(node);
    if (known != null) {
      return known;
    }
    final Set<CorrelationId> declared = node.getVariablesSet();
    idsInPlan.addAll(declared);
    final Set<CorrelationId> free = new HashSet<>();
    final Set<CorrelationId> subQueryFree = new HashSet<>();
    node.accept(new RexShuttle() {
      @Override public RexNode visitCorrelVariable(RexCorrelVariable variable) {
        idsInPlan.add(variable.id);
        free.add(variable.id);
        return variable;
      }

      @Override public RexNode visitSubQuery(RexSubQuery subQuery) {
        subQueryFree.addAll(freeIdsOf(subQuery.rel));
        return super.visitSubQuery(subQuery);
      }
    });
    if (node instanceof Correlate) {
      final Correlate correlate = (Correlate) node;
      // The condition of a ConditionalCorrelate is not in the scope of its variable
      free.addAll(subQueryFree);
      free.addAll(freeIdsOf(correlate.getLeft()));
      // The scope of the variable is the right input
      final Set<CorrelationId> rightFree = new HashSet<>(freeIdsOf(correlate.getRight()));
      rightFree.remove(correlate.getCorrelationId());
      free.addAll(rightFree);
    } else {
      // The scope of the variables of a Project, Filter or Join is its sub-queries
      subQueryFree.removeAll(declared);
      free.addAll(subQueryFree);
      for (RelNode input : node.getInputs()) {
        free.addAll(freeIdsOf(input));
      }
    }
    freeIds.put(node, free);
    return free;
  }

  /** Returns the id that a declaration of {@code id} gets in the plan that
   * this remover returns, and adds it to {@link #declaredIds}.
   *
   * <p>If no other declaration in that plan has {@code id}, returns
   * {@code id}. Else returns a fresh id: an id that is not in
   * {@link #idsInPlan}, thus no node of the input plan declares or uses it. */
  private CorrelationId freshId(CorrelationId id) {
    if (declaredIds.add(id)) {
      return id;
    }
    CorrelationId newId;
    do {
      newId = cluster.createCorrel();
    } while (idsInPlan.contains(newId));
    idsInPlan.add(newId);
    declaredIds.add(newId);
    return newId;
  }

  /** Returns a renaming that also contains the entries of {@code newIds}.
   * An entry of {@code newIds} takes precedence over an entry of {@code renaming}. */
  private static Map<CorrelationId, CorrelationId> extend(
      Map<CorrelationId, CorrelationId> renaming, Map<CorrelationId, CorrelationId> newIds) {
    final Map<CorrelationId, CorrelationId> extended = new HashMap<>(renaming);
    newIds.forEach((id, newId) -> {
      if (id.equals(newId)) {
        extended.remove(id);
      } else {
        extended.put(id, newId);
      }
    });
    return extended;
  }

  /** Returns the node that replaces {@code node0} in the result plan.
   *
   * @param renaming New id of each free id of the node that changes
   */
  private RelNode rename(RelNode node0, Map<CorrelationId, CorrelationId> renaming) {
    final RelNode node = node0.stripped();
    final Set<CorrelationId> free = requireNonNull(freeIds.get(node));
    final ImmutableMap.Builder<CorrelationId, CorrelationId> builder = ImmutableMap.builder();
    renaming.forEach((id, newId) -> {
      if (free.contains(id)) {
        builder.put(id, newId);
      }
    });
    final ImmutableMap<CorrelationId, CorrelationId> key = builder.build();
    final Map<Map<CorrelationId, CorrelationId>, RelNode> byRenaming =
        renamed.computeIfAbsent(node, n -> new HashMap<>());
    final RelNode known = byRenaming.get(key);
    if (known != null) {
      return known;
    }
    final RelNode result =
        node instanceof Correlate
            ? renameCorrelate((Correlate) node, key)
            : renameOther(node, key);
    byRenaming.put(key, result);
    return result;
  }

  /** Returns a shuttle that gives new ids to the uses in row expressions.
   *
   * @param renaming         New ids of the uses outside sub-queries
   * @param subQueryRenaming New ids of the free variables of the sub-queries
   */
  private RexShuttle useRenamer(Map<CorrelationId, CorrelationId> renaming,
      Map<CorrelationId, CorrelationId> subQueryRenaming) {
    return new RexShuttle() {
      @Override public RexNode visitCorrelVariable(RexCorrelVariable variable) {
        final CorrelationId newId = renaming.get(variable.id);
        return newId == null
            ? variable
            : cluster.getRexBuilder().makeCorrel(variable.getType(), newId);
      }

      @Override public RexNode visitSubQuery(RexSubQuery subQuery) {
        final RelNode rel = rename(subQuery.rel, subQueryRenaming);
        return super.visitSubQuery(rel == subQuery.rel ? subQuery : subQuery.clone(rel));
      }
    };
  }

  private RelNode renameCorrelate(Correlate correlate,
      Map<CorrelationId, CorrelationId> renaming) {
    final CorrelationId id = correlate.getCorrelationId();
    final CorrelationId newId = freshId(id);
    final RelNode left = rename(correlate.getLeft(), renaming);
    final RelNode right =
        rename(correlate.getRight(), extend(renaming, ImmutableMap.of(id, newId)));
    final RexNode condition = correlate.getCondition();
    final RexNode newCondition = condition.accept(useRenamer(renaming, renaming));
    if (newId.equals(id) && left == correlate.getLeft() && right == correlate.getRight()
        && newCondition == condition) {
      return correlate;
    }
    if (correlate instanceof ConditionalCorrelate) {
      return ((ConditionalCorrelate) correlate).copy(correlate.getTraitSet(), left, right,
          newId, correlate.getRequiredColumns(), correlate.getJoinType(), newCondition);
    }
    return correlate.copy(correlate.getTraitSet(), left, right, newId,
        correlate.getRequiredColumns(), correlate.getJoinType());
  }

  private RelNode renameOther(RelNode node, Map<CorrelationId, CorrelationId> renaming) {
    final Map<CorrelationId, CorrelationId> newIds = new LinkedHashMap<>();
    for (CorrelationId id : node.getVariablesSet()) {
      newIds.put(id, freshId(id));
    }
    final Map<CorrelationId, CorrelationId> subQueryRenaming = extend(renaming, newIds);

    // The inputs are not in the scope of the variables of the node
    final List<RelNode> inputs = new ArrayList<>();
    boolean inputsChanged = false;
    for (RelNode input : node.getInputs()) {
      final RelNode newInput = rename(input, renaming);
      inputsChanged |= newInput != input;
      inputs.add(newInput);
    }
    final RelNode withInputs =
        inputsChanged ? node.copy(node.getTraitSet(), inputs) : node;
    final RelNode result = withInputs.accept(useRenamer(renaming, subQueryRenaming));
    if (newIds.keySet().equals(ImmutableSet.copyOf(newIds.values()))) {
      return result;
    }
    return withVariablesSet(result, ImmutableSet.copyOf(newIds.values()));
  }

  /** Returns a copy of a node that declares other variables. */
  private static RelNode withVariablesSet(RelNode node,
      ImmutableSet<CorrelationId> variablesSet) {
    if (node instanceof LogicalProject) {
      final LogicalProject project = (LogicalProject) node;
      return LogicalProject.create(project.getInput(), project.getHints(),
          project.getProjects(), project.getRowType(), variablesSet);
    }
    if (node instanceof LogicalFilter) {
      final LogicalFilter filter = (LogicalFilter) node;
      return LogicalFilter.create(filter.getInput(), filter.getCondition(),
          variablesSet);
    }
    if (node instanceof LogicalJoin) {
      final LogicalJoin join = (LogicalJoin) node;
      return LogicalJoin.create(join.getLeft(), join.getRight(), join.getHints(),
          join.getCondition(), variablesSet, join.getJoinType(),
          join.isSemiJoinDone(), ImmutableList.copyOf(join.getSystemFieldList()));
    }
    throw new UnsupportedOperationException("Cannot give new ids to the "
        + "variables of " + node.getRelTypeName());
  }
}
