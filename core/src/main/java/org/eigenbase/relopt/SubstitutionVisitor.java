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
package org.eigenbase.relopt;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.eigenbase.rel.*;
import org.eigenbase.rel.rules.RemoveTrivialProjectRule;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.*;
import org.eigenbase.sql.SqlKind;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.trace.EigenbaseTrace;
import org.eigenbase.util.Pair;

import net.hydromatic.linq4j.Ord;

import net.hydromatic.optiq.prepare.OptiqPrepareImpl;

import com.google.common.collect.ImmutableList;

/**
 * Substitutes part of a tree of relational expressions with another tree.
 *
 * <p>The call {@code new SubstitutionVisitor(target, query).go(replacement))}
 * will return {@code query} with every occurrence of {@code target} replaced
 * by {@code replacement}.</p>
 *
 * <p>The following example shows how {@code SubstitutionVisitor} can be used
 * for materialized view recognition.</p>
 *
 * <ul>
 * <li>query = SELECT a, c FROM t WHERE x = 5 AND b = 4</li>
 * <li>target = SELECT a, b, c FROM t WHERE x = 5</li>
 * <li>replacement = SELECT * FROM mv</li>
 * <li>result = SELECT a, c FROM mv WHERE b = 4</li>
 * </ul>
 *
 * <p>Note that {@code result} uses the materialized view table {@code mv} and a
 * simplified condition {@code b = 4}.</p>
 *
 * <p>Uses a bottom-up matching algorithm. Nodes do not need to be identical.
 * At each level, returns the residue.</p>
 *
 * <p>The inputs must only include the core relational operators:
 * {@link TableAccessRel},
 * {@link FilterRel},
 * {@link ProjectRel},
 * {@link JoinRel},
 * {@link UnionRel},
 * {@link AggregateRel}.</p>
 */
public class SubstitutionVisitor {
  private static final boolean DEBUG = OptiqPrepareImpl.DEBUG;

  private static final Logger LOGGER = EigenbaseTrace.getPlannerTracer();

  private static final List<UnifyRule> RULES =
      Arrays.<UnifyRule>asList(
          ScanUnifyRule.INSTANCE,
          ProjectToProjectUnifyRule.INSTANCE,
          FilterToProjectUnifyRule.INSTANCE,
          ProjectToFilterUnifyRule.INSTANCE,
          FilterToFilterUnifyRule.INSTANCE);

  private static final Map<Pair<Class, Class>, List<UnifyRule>> RULE_MAP =
      new HashMap<Pair<Class, Class>, List<UnifyRule>>();

  private final RelNode query;
  private final RelNode target;

  /**
   * Map from each node in the query and the materialization query
   * to its parent.
   */
  final Map<RelNode, Parentage> parentMap =
      new IdentityHashMap<RelNode, Parentage>();

  /**
   * Nodes in {@link #target} that have no children.
   */
  final List<RelNode> targetLeaves;

  /**
   * Nodes in {@link #query} that have no children.
   */
  final List<RelNode> queryLeaves;

  final Map<RelNode, RelNode> replacementMap =
      new HashMap<RelNode, RelNode>();

  public SubstitutionVisitor(RelNode target, RelNode query) {
    this.query = query;
    this.target = target;
    final Set<RelNode> parents = new HashSet<RelNode>();
    final List<RelNode> allNodes = new ArrayList<RelNode>();
    final RelVisitor visitor =
        new RelVisitor() {
          public void visit(RelNode node, int ordinal, RelNode parent) {
            parentMap.put(node, new Parentage(parent, ordinal));
            parents.add(parent);
            allNodes.add(node);
            super.visit(node, ordinal, parent);
          }
        };
    visitor.go(target);

    // Populate the list of leaves in the tree under "target".
    // Leaves are all nodes that are not parents.
    // For determinism, it is important that the list is in scan order.
    allNodes.removeAll(parents);
    targetLeaves = ImmutableList.copyOf(allNodes);

    allNodes.clear();
    visitor.go(query);
    allNodes.removeAll(parents);
    queryLeaves = ImmutableList.copyOf(allNodes);
  }

  /**
   * Maps a condition onto a target.
   *
   * <p>If condition is stronger than target, returns the residue.
   * If it is equal to target, returns the expression that evaluates to
   * the constant {@code true}. If it is weaker than target, returns
   * {@code null}.</p>
   *
   * <p>The terms satisfy the relation
   * <pre>
   *     {@code residue = condition AND NOT target}
   * </pre>
   *
   * <p>Example #1: condition stronger than target</p>
   * <ul>
   * <li>condition: x = 1 AND y = 2</li>
   * <li>target: x = 1</li>
   * <li>residue: y = 2</li>
   * </ul>
   *
   * <p>Example #2: target weaker than target (valid, but not currently
   * implemented)</p>
   * <ul>
   * <li>condition: x = 1</li>
   * <li>target: x = 1 OR z = 3</li>
   * <li>residue: z = 3</li>
   * </ul>
   *
   * <p>Example #3: condition and target are equivalent</p>
   * <ul>
   * <li>condition: x = 1 and y = 2</li>
   * <li>target: y = 2 and x = 1</li>
   * <li>residue: true</li>
   * </ul>
   *
   * <p>Example #4: condition weaker than target</p>
   * <ul>
   * <li>condition: x = 1</li>
   * <li>target: x = 1 AND y = 2</li>
   * <li>residue: null (i.e. no match)</li>
   * </ul>
   *
   * <p>There are many other possible examples. It amounts to solving
   * whether {@code condition AND NOT target} can ever evaluate to
   * true, and therefore is a form of the NP-complete
   * <a href"http://en.wikipedia.org/wiki/Satisfiability">Satisfiability</a>
   * problem.</p>
   */
  static RexNode splitFilter(
      RexBuilder rexBuilder, RexNode condition, RexNode target) {
    RexNode x = andNot(rexBuilder, target, condition);
    if (mayBeSatisfiable(x)) {
      RexNode x2 = andNot(rexBuilder, condition, target);
      return simplify(rexBuilder, x2);
    }
    return null;
  }

  /**
   * Returns whether a boolean expression ever returns true.
   *
   * <p>This method may give false positives. For instance, it will say
   * that {@code x = 5 AND x > 10} is satisfiable, because at present it
   * cannot prove that it is not.</p>
   */
  public static boolean mayBeSatisfiable(RexNode e) {
    // Example:
    //  e: x = 1 AND y = 2 AND z = 3 AND NOT (x = 1 AND y = 2)
    //  disjunctions: {x = 1, y = 2, z = 3}
    //  notDisjunctions: {x = 1 AND y = 2}
    final List<RexNode> disjunctions = new ArrayList<RexNode>();
    final List<RexNode> notDisjunctions = new ArrayList<RexNode>();
    RelOptUtil.decomposeConjunction(e, disjunctions, notDisjunctions);

    // If there is a single FALSE or NOT TRUE, the whole expression is
    // always false.
    for (RexNode disjunction : disjunctions) {
      switch (disjunction.getKind()) {
      case LITERAL:
        if (!RexLiteral.booleanValue(disjunction)) {
          return false;
        }
      }
    }
    for (RexNode disjunction : notDisjunctions) {
      switch (disjunction.getKind()) {
      case LITERAL:
        if (RexLiteral.booleanValue(disjunction)) {
          return false;
        }
      }
    }
    // If one of the not-disjunctions is a disjunction that is wholly
    // contained in the disjunctions list, the expression is not
    // satisfiable.
    //
    // Example #1. x AND y AND z AND NOT (x AND y)  - not satisfiable
    // Example #2. x AND y AND NOT (x AND y)        - not satisfiable
    // Example #3. x AND y AND NOT (x AND y AND z)  - may be satisfiable
    for (RexNode notDisjunction : notDisjunctions) {
      final List<RexNode> disjunctions2 =
          RelOptUtil.conjunctions(notDisjunction);
      if (disjunctions.containsAll(disjunctions2)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Simplifies a boolean expression.
   *
   * <p>In particular:</p>
   * <ul>
   * <li>{@code simplify(x = 1 AND y = 2 AND NOT x = 1)}
   * returns {@code y = 2}</li>
   * <li>{@code simplify(x = 1 AND FALSE)}
   * returns {@code FALSE}</li>
   * </ul>
   */
  public static RexNode simplify(RexBuilder rexBuilder, RexNode e) {
    final List<RexNode> disjunctions = RelOptUtil.conjunctions(e);
    final List<RexNode> notDisjunctions = new ArrayList<RexNode>();
    for (int i = 0; i < disjunctions.size(); i++) {
      final RexNode disjunction = disjunctions.get(i);
      final SqlKind kind = disjunction.getKind();
      switch (kind) {
      case NOT:
        notDisjunctions.add(
            ((RexCall) disjunction).getOperands().get(0));
        disjunctions.remove(i);
        --i;
        break;
      case LITERAL:
        if (!RexLiteral.booleanValue(disjunction)) {
          return disjunction; // false
        } else {
          disjunctions.remove(i);
          --i;
        }
      }
    }
    if (disjunctions.isEmpty() && notDisjunctions.isEmpty()) {
      return rexBuilder.makeLiteral(true);
    }
    // If one of the not-disjunctions is a disjunction that is wholly
    // contained in the disjunctions list, the expression is not
    // satisfiable.
    //
    // Example #1. x AND y AND z AND NOT (x AND y)  - not satisfiable
    // Example #2. x AND y AND NOT (x AND y)        - not satisfiable
    // Example #3. x AND y AND NOT (x AND y AND z)  - may be satisfiable
    for (RexNode notDisjunction : notDisjunctions) {
      final List<RexNode> disjunctions2 =
          RelOptUtil.conjunctions(notDisjunction);
      if (disjunctions.containsAll(disjunctions2)) {
        return rexBuilder.makeLiteral(false);
      }
    }
    // Add the NOT disjunctions back in.
    for (RexNode notDisjunction : notDisjunctions) {
      disjunctions.add(
          rexBuilder.makeCall(
              SqlStdOperatorTable.NOT,
              notDisjunction));
    }
    return RexUtil.composeConjunction(rexBuilder, disjunctions, false);
  }

  /**
   * Creates the expression {@code e1 AND NOT e2}.
   */
  static RexNode andNot(RexBuilder rexBuilder, RexNode e1, RexNode e2) {
    return rexBuilder.makeCall(
        SqlStdOperatorTable.AND,
        e1,
        rexBuilder.makeCall(
            SqlStdOperatorTable.NOT,
            e2));
  }

  public RelNode go(RelNode replacement) {
    assert RelOptUtil.equalType("target", target, "replacement", replacement,
        true);
    replacementMap.put(target, replacement);
    final UnifyResult unifyResult = matchRecurse(target);
    if (unifyResult != null) {
      final RelNode node =
          RelOptUtil.replace(query, unifyResult.query, unifyResult.result);
      if (DEBUG) {
        System.out.println(
            "Convert: query=" + RelOptUtil.toString(query)
            + "\nnode=" + RelOptUtil.toString(node));
      }
      return node;
    }
    return null;
  }

  private UnifyResult matchRecurse(RelNode target) {
    final List<RelNode> targetInputs = target.getInputs();
    final List<RelNode> queryInputs = new ArrayList<RelNode>();
    RelNode queryParent = null;

    for (RelNode targetInput : targetInputs) {
      UnifyResult unifyResult = matchRecurse(targetInput);
      if (unifyResult == null) {
        return null;
      }
      queryInputs.add(unifyResult.result);
      Parentage pair = parentMap.get(unifyResult.query);
      queryParent = pair.parent;
/*
            queryParent = RelOptUtil.replaceInput(
                pair.left, pair.right, unifyResult.result);
            parentMap.put(queryParent, pair);
*/
    }

    if (targetInputs.isEmpty()) {
      for (RelNode queryLeaf : queryLeaves) {
        for (UnifyRule rule : applicableRules(queryLeaf, target)) {
          final UnifyResult x = apply(rule, queryLeaf, target);
          if (x != null) {
            if (DEBUG) {
              System.out.println(
                  "Rule: " + rule
                  + "\nQuery:\n"
                  + RelOptUtil.toString(queryParent)
                  + (x.query != queryParent
                     ? "\nQuery (original):\n"
                     + RelOptUtil.toString(queryParent)
                     : "")
                  + "\nTarget:\n"
                  + RelOptUtil.toString(target)
                  + "\nResult:\n"
                  + RelOptUtil.toString(x.result)
                  + "\n");
            }
            return x;
          }
        }
      }
    } else {
      for (UnifyRule rule : applicableRules(queryParent, target)) {
        final UnifyResult x = apply(rule, queryParent, target);
        if (x != null) {
          if (DEBUG) {
            System.out.println(
                "Rule: " + rule
                + "\nQuery:\n"
                + RelOptUtil.toString(queryParent)
                + (x.query != queryParent
                   ? "\nQuery (original):\n"
                   + RelOptUtil.toString(queryParent)
                   : "")
                + "\nTarget:\n"
                + RelOptUtil.toString(target)
                + "\nResult:\n"
                + RelOptUtil.toString(x.result)
                + "\n");
          }
          return x;
        }
      }
    }
    return null;
  }

  private static List<UnifyRule> applicableRules(RelNode query,
      RelNode target) {
    final Class queryClass = query.getClass();
    final Class targetClass = target.getClass();
    final Pair<Class, Class> key = Pair.of(queryClass, targetClass);
    List<UnifyRule> list = RULE_MAP.get(key);
    if (list == null) {
      final ImmutableList.Builder<UnifyRule> builder =
          ImmutableList.builder();
      for (UnifyRule rule : RULES) {
        //noinspection unchecked
        if (rule.getQueryClass().isAssignableFrom(queryClass)
            && rule.getTargetClass().isAssignableFrom(targetClass)) {
          builder.add(rule);
        }
      }
      list = builder.build();
      RULE_MAP.put(key, list);
    }
    return list;
  }

  private <Q extends RelNode, T extends RelNode> UnifyResult apply(
      UnifyRule<Q, T> rule, Q query, T target) {
    final Class<Q> queryClass = rule.getQueryClass();
    final Class<T> targetClass = rule.getTargetClass();
    if (queryClass.isInstance(query)
        && targetClass.isInstance(target)) {
      return rule.apply(
          new UnifyIn<Q, T>(
              queryClass.cast(query),
              targetClass.cast(target)));
    }
    return null;
  }

  /** Exception thrown to exit a matcher. Not really an error. */
  private static class MatchFailed extends RuntimeException {
    public static final MatchFailed INSTANCE = new MatchFailed();
  }

  /** Rule that attempts to match a query relational expression
   * against a target relational expression.
   *
   * <p>The rule declares the query and target types; this allows the
   * engine to fire only a few rules in a given context.</p>
   */
  private interface UnifyRule<Q extends RelNode, T extends RelNode> {
    Class<Q> getQueryClass();

    Class<T> getTargetClass();

    /**
     * <p>Applies this rule to a particular node in a query. The goal is
     * to convert {@code query} into {@code target}. Before the rule is
     * invoked, Optiq has made sure that query's children are equivalent
     * to target's children.
     *
     * <p>There are 3 possible outcomes:</p>
     *
     * <ul>
     *
     * <li>{@code query} already exactly matches {@code target}; returns
     * {@code target}</li>
     *
     * <li>{@code query} is sufficiently close to a match for
     * {@code target}; returns {@code target}</li>
     *
     * <li>{@code query} cannot be made to match {@code target}; returns
     * null</li>
     *
     * </ul>
     *
     * <p>REVIEW: Is possible that we match query PLUS one or more of its
     * ancestors?</p>
     *
     * @param in Input parameters
     */
    UnifyResult apply(UnifyIn<Q, T> in);
  }

  /**
   * Arguments to an application of a {@link UnifyRule}.
   */
  private class UnifyIn<Q extends RelNode, T extends RelNode> {
    final Q query;
    final T target;

    public UnifyIn(Q query, T target) {
      this.query = query;
      this.target = target;
    }

    /** Returns the parent of a node, which child it is, and a bitmap of which
     * columns are used by the parent. */
    public Parentage parent(RelNode node) {
      return parentMap.get(node);
    }

    UnifyResult result(RelNode result) {
      assert RelOptUtil.contains(result, target);
      assert RelOptUtil.equalType("result", result, "query", query, true);
      RelNode replace = replacementMap.get(target);
      if (replace != null) {
        result = RelOptUtil.replace(result, target, replace);
      }
      return new UnifyResult(query, target, result);
    }

    /**
     * Creates a {@link UnifyIn} based on the parent of {@code query}.
     */
    public <Q2 extends RelNode> UnifyIn<Q2, T> create(Q2 query) {
      return new UnifyIn<Q2, T>(query, target);
    }
  }

  /**
   * Result of an application of a {@link UnifyRule} indicating that the
   * rule successfully matched {@code query} against {@code target} and
   * generated a {@code result} that is equivalent to {@code query} and
   * contains {@code target}.
   */
  private static class UnifyResult {
    private final RelNode query;
    private final RelNode target;
    // equivalent to "query", contains "result"
    private final RelNode result;

    UnifyResult(RelNode query, RelNode target, RelNode result) {
      this.query = query;
      this.target = target;
      this.result = result;
    }
  }

  /** Abstract base class for implementing {@link UnifyRule}. */
  private abstract static
  class AbstractUnifyRule<Q extends RelNode, T extends RelNode>
      implements UnifyRule<Q, T> {
    private final Class<Q> queryClass;
    private final Class<T> targetClass;

    public AbstractUnifyRule(Class<Q> queryClass, Class<T> targetClass) {
      this.queryClass = queryClass;
      this.targetClass = targetClass;
    }

    public Class<Q> getQueryClass() {
      return queryClass;
    }

    public Class<T> getTargetClass() {
      return targetClass;
    }
  }

  /** Implementation of {@link UnifyRule} that matches a table scan
   * ({@link TableAccessRelBase} or a sub-class). */
  private static class ScanUnifyRule
      extends AbstractUnifyRule<TableAccessRelBase, TableAccessRelBase> {
    public static final ScanUnifyRule INSTANCE = new ScanUnifyRule();

    public ScanUnifyRule() {
      super(TableAccessRelBase.class, TableAccessRelBase.class);
    }

    public UnifyResult apply(
        UnifyIn<TableAccessRelBase, TableAccessRelBase> in) {
      if (in.query.getTable().getQualifiedName().equals(
          in.target.getTable().getQualifiedName())) {
        return in.result(in.target);
      }
      return null;
    }
  }

  /** Implementation of {@link UnifyRule} that matches {@link ProjectRel}. */
  private static class ProjectToProjectUnifyRule
      extends AbstractUnifyRule<ProjectRel, ProjectRel> {
    public static final ProjectToProjectUnifyRule INSTANCE =
        new ProjectToProjectUnifyRule();

    private ProjectToProjectUnifyRule() {
      super(ProjectRel.class, ProjectRel.class);
    }

    public UnifyResult apply(UnifyIn<ProjectRel, ProjectRel> in) {
      final RexShuttle shuttle = getRexShuttle(in.target);
      final List<RexNode> newProjects;
      try {
        newProjects = shuttle.apply(in.query.getProjects());
      } catch (MatchFailed e) {
        return null;
      }
      final ProjectRel newProject =
          new ProjectRel(
              in.target.getCluster(),
              in.target.getCluster().traitSetOf(
                  in.query.getCollationList().isEmpty()
                      ? RelCollationImpl.EMPTY
                      : in.query.getCollationList().get(0)),
              in.target,
              newProjects,
              in.query.getRowType(),
              in.query.getFlags());
      final RelNode newProject2 =
          RemoveTrivialProjectRule.strip(newProject);
      return in.result(newProject2);
    }
  }

  /** Implementation of {@link UnifyRule} that matches a {@link FilterRel}
   * to a {@link ProjectRel}. */
  private static class FilterToProjectUnifyRule
      extends AbstractUnifyRule<FilterRel, ProjectRel> {
    public static final FilterToProjectUnifyRule INSTANCE =
        new FilterToProjectUnifyRule();

    private FilterToProjectUnifyRule() {
      super(FilterRel.class, ProjectRel.class);
    }

    public UnifyResult apply(UnifyIn<FilterRel, ProjectRel> in) {
      // Child of projectTarget is equivalent to child of filterQuery.
      try {
        // TODO: make sure that constants are ok
        final RexShuttle shuttle = getRexShuttle(in.target);
        final RexNode newCondition;
        try {
          newCondition = in.query.getCondition().accept(shuttle);
        } catch (MatchFailed e) {
          return null;
        }
        final FilterRel newFilter =
            new FilterRel(
                in.query.getCluster(),
                in.target,
                newCondition);
        final RelNode inverse = invert(in.query, newFilter, in.target);
        return in.result(inverse);
      } catch (MatchFailed e) {
        return null;
      }
    }

    private RelNode invert(RelNode model, RelNode input, ProjectRel project) {
      if (LOGGER.isLoggable(Level.FINER)) {
        LOGGER.finer("SubstitutionVisitor: invert:\n"
            + "model: " + model + "\n"
            + "input: " + input + "\n"
            + "project: " + project + "\n");
      }
      final List<RexNode> exprList = new ArrayList<RexNode>();
      final RexBuilder rexBuilder = model.getCluster().getRexBuilder();
      for (RelDataTypeField field : model.getRowType().getFieldList()) {
        exprList.add(rexBuilder.makeZeroLiteral(field.getType()));
      }
      for (Ord<RexNode> expr : Ord.zip(project.getProjects())) {
        if (expr.e instanceof RexInputRef) {
          final int target = ((RexInputRef) expr.e).getIndex();
          exprList.set(expr.i,
              rexBuilder.makeInputRef(input, target));
        }
      }
      return new ProjectRel(model.getCluster(), model.getTraitSet(), input,
          exprList, model.getRowType(), ProjectRelBase.Flags.BOXED);
    }
  }

  /** Implementation of {@link UnifyRule} that matches a {@link FilterRel}. */
  private static class FilterToFilterUnifyRule
      extends AbstractUnifyRule<FilterRel, FilterRel> {
    public static final FilterToFilterUnifyRule INSTANCE =
        new FilterToFilterUnifyRule();

    private FilterToFilterUnifyRule() {
      super(FilterRel.class, FilterRel.class);
    }

    public UnifyResult apply(UnifyIn<FilterRel, FilterRel> in) {
      // in.query can be rewritten in terms of in.target if its condition
      // is weaker. For example:
      //   query: SELECT * FROM t WHERE x = 1 AND y = 2
      //   target: SELECT * FROM t WHERE x = 1
      // transforms to
      //   result: SELECT * FROM (target) WHERE y = 2
      final FilterRel newFilter = createFilter(in.query, in.target);
      if (newFilter == null) {
        return null;
      }
      return in.result(newFilter);
    }

    FilterRel createFilter(FilterRel query, FilterRel target) {
      final RelOptCluster cluster = query.getCluster();
      final RexNode newCondition =
          splitFilter(
              cluster.getRexBuilder(), query.getCondition(),
              target.getCondition());
      if (newCondition == null) {
        // Could not map query onto target.
        return null;
      }
      if (newCondition.isAlwaysTrue()) {
        return target;
      }
      return new FilterRel(cluster, target, newCondition);
    }
  }

  /** Implementation of {@link UnifyRule} that matches a {@link ProjectRel} to
   * a {@link FilterRel}. */
  private static class ProjectToFilterUnifyRule
      extends AbstractUnifyRule<ProjectRel, FilterRel> {
    public static final ProjectToFilterUnifyRule INSTANCE =
        new ProjectToFilterUnifyRule();

    private ProjectToFilterUnifyRule() {
      super(ProjectRel.class, FilterRel.class);
    }

    public UnifyResult apply(UnifyIn<ProjectRel, FilterRel> in) {
      final Parentage queryParent = in.parent(in.query);
      if (queryParent.parent instanceof FilterRel) {
        final UnifyIn<FilterRel, FilterRel> in2 =
            in.create((FilterRel) queryParent.parent);
        final FilterRel newFilter =
            FilterToFilterUnifyRule.INSTANCE.createFilter(
                in2.query, in2.target);
        if (newFilter == null) {
          return null;
        }
        return in2.result(
            in.query.copy(
                in.query.getTraitSet(),
                ImmutableList.<RelNode>of(newFilter)));
      }
      return null;
    }
  }

  private static RexShuttle getRexShuttle(ProjectRel target) {
    final Map<String, Integer> map = new HashMap<String, Integer>();
    for (RexNode e : target.getProjects()) {
      map.put(e.toString(), map.size());
    }
    return new RexShuttle() {
      @Override public RexNode visitInputRef(RexInputRef ref) {
        final Integer integer = map.get(ref.getName());
        if (integer != null) {
          return new RexInputRef(integer, ref.getType());
        }
        throw MatchFailed.INSTANCE;
      }

      @Override public RexNode visitCall(RexCall call) {
        final Integer integer = map.get(call.toString());
        if (integer != null) {
          return new RexInputRef(integer, call.getType());
        }
        return super.visitCall(call);
      }
    };
  }

  private static class Parentage {
    final RelNode parent;
    final int ordinal;

    private Parentage(RelNode parent, int ordinal) {
      this.parent = parent;
      this.ordinal = ordinal;
    }
  }
}

// End SubstitutionVisitor.java
