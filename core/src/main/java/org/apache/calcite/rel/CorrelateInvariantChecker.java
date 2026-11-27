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

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Litmus;

import com.google.common.collect.ImmutableList;

import org.jspecify.annotations.Nullable;

import java.util.BitSet;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Checks that a plan obeys the invariants of correlation variables.
 *
 * <p>This comment gives the invariants, and the rules for code that makes or
 * changes a plan.
 *
 * <h2>Correlation variables</h2>
 *
 * <p>A correlation variable is a reference to a row. The evaluation of one
 * sub-plan produces the row, and a different sub-plan reads the row through
 * the variable. The name of a variable is its correlation id ({@link
 * CorrelationId}), which a plan shows as {@code $cor0}. We often use
 * the two concepts interchangeably.
 *
 * <p>The Rel node that connects the two sub-plans is the declaration of the
 * variable. For example, a {@link Correlate} evaluates its right input one
 * time for each row of its left input. During each evaluation of the right
 * input, the variable refers to that row of the left input.
 *
 * <p>For example, for the tables {@code t(k, v)} and {@code s(k, w)},
 *
 * <blockquote><pre>
 * SELECT k, (SELECT SUM(w) FROM s WHERE s.k = t.k) FROM t
 * </pre></blockquote>
 *
 * <p>becomes
 *
 * <blockquote><pre>
 * LogicalProject(variablesSet=[[$cor0]], K=[$0], EXPR$1=[$SCALAR_QUERY({  -- declaration of $cor0
 * LogicalAggregate(group=[{}], EXPR$0=[SUM($0)])                          -- scope of $cor0
 *   LogicalProject(W=[$1])
 *     LogicalFilter(condition=[=($0, $cor0.K)])                           -- use of $cor0
 *       LogicalTableScan(table=[[S]])
 * })])
 *   LogicalTableScan(table=[[T]])                                         -- outside scope of $cor0
 * </pre></blockquote>
 *
 * <p>and, after the removal of the sub-query,
 *
 * <blockquote><pre>
 * LogicalProject(K=[$0], EXPR$1=[$2])
 *   LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{0}])  -- declaration
 *     LogicalTableScan(table=[[T]])                                                -- outside scope
 *     LogicalAggregate(group=[{}], EXPR$0=[SUM($0)])                               -- scope
 *       LogicalProject(W=[$1])
 *         LogicalFilter(condition=[=($0, $cor0.K)])                                -- use
 *           LogicalTableScan(table=[[S]])
 * </pre></blockquote>
 *
 * <table>
 *   <caption>Terms</caption>
 *   <tr><th>Term</th><th>Definition</th></tr>
 *   <tr><td>Plan</td>
 *     <td>A rooted Rel DAG, including the plans of the
 *     sub-queries ({@link RexSubQuery}) in the row expressions ({@link
 *     RexNode})</td></tr>
 *   <tr><td>Complete plan</td>
 *     <td>A plan that is not a subplan of a larger plan</td></tr>
 *   <tr><td>Correlation id</td>
 *     <td>The name of a variable. A declaration and a use refer to a
 *     variable by its id</td></tr>
 *   <tr><td>Correlation Variable Declaration</td>
 *     <td>The Rel node that connects the sub-plan that produces the row and
 *     the sub-plan that reads the row. {@link RelNode#getVariablesSet()}
 *     returns the ids of the variables that a node declares</td></tr>
 *   <tr><td>Correlation Variable Use</td>
 *     <td>A {@link RexCorrelVariable} expression. It refers to the variable
 *     by its id. A use usually reads one field of the
 *     row, as in {@code $cor0.K}. It can also read the full row, as in
 *     {@code $cor0}</td></tr>
 *   <tr><td>Variable Scope</td>
 *     <td>The sub-plan that can read the row the variable refers to.
 *     Uses of the variable are only allowed within its scope.
 *     The declaration establishes the scope</td></tr>
 *   <tr><td>Free variable</td>
 *     <td>A variable used in a plan without a declaration in the plan</td></tr>
 *   <tr><td>(Alpha) Rename</td>
 *     <td>Give a variable a new id: replace the id in the declaration and
 *     in all the uses within its scope</td></tr>
 * </table>
 *
 * <table>
 *   <caption>Rel Nodes that can declare a variable</caption>
 *   <tr><th>Node</th><th>The variable refers to</th><th>Scope</th></tr>
 *   <tr><td>{@link Correlate}</td>
 *     <td>A row of the left input. The Correlate evaluates its right input
 *     one time for each row of its left input</td>
 *     <td>The plan of the right input</td></tr>
 *   <tr><td>{@link Project}, {@link Filter}</td>
 *     <td>A row of the input. The node evaluates its row expressions one
 *     time for each row of its input</td>
 *     <td>The plans of the sub-queries in the row expressions of the
 *     node</td></tr>
 *   <tr><td>{@link Join}</td>
 *     <td>The fields of a row of the left input, followed by the fields of
 *     a row of the right input. The Join evaluates its condition one time
 *     for each such pair of rows</td>
 *     <td>The plans of the sub-queries in the condition</td></tr>
 * </table>
 *
 * <p>A Project, Filter or Join declares a variable only for the correlated
 * sub-queries in its row expressions. For a Join, these are the sub-queries
 * in the ON condition, which can read the rows of both inputs.
 *
 * <h2>Invariants</h2>
 *
 * <p>A complete plan obeys the following invariants. A subplan can have free variables.
 * {@link #isValid} checks the invariants.
 *
 * <table>
 *   <caption>Invariants of correlation variables</caption>
 *   <tr><th>Invariant</th><th>Rule</th></tr>
 *   <tr><td>1. Unique ids</td>
 *     <td>Different variables have different ids. However, a plan can contain
 *     one sub-plan in more than one position. Thus an id can be declared in
 *     two positions of a plan only if the declarations are <em>equal</em>.
 *     Equality can be: object identity (one Rel node that the plan
 *     reaches along two paths, i.e., the plan is a DAG), and {@link
 *     RelNode#deepEquals(Object)} (two Rel node objects with equal
 *     contents)</td></tr>
 *   <tr><td>2. Used only within scope</td>
 *     <td>Each use of a variable is in the scope of the declaration of
 *     that variable. Thus a complete plan has no free variables, the left
 *     input of a Correlate does not use the variable of that Correlate,
 *     and a Project, Filter or Join uses its variable only in
 *     sub-queries</td></tr>
 *   <tr><td>3. One variable per node</td>
 *     <td>A Rel node declares at most one variable. A Correlate always
 *     declares exactly one</td></tr>
 *   <tr><td>4. Type</td>
 *     <td>The type of a use ({@link RexCorrelVariable#getType()}) is a
 *     record type. It can have more fields or fewer fields than the row
 *     that the variable refers to. However, each field that a use reads
 *     must exist in the row, at the same position and with the same type.
 *     A use that reads the full row must have the type of the row.
 *     Ideally the type of each use should exactly match the type of the row.
 *     Plans do not obey the stricter rule
 *     today, and the difference is possibly a bug</td></tr>
 *   <tr><td>5. Required columns</td>
 *     <td>{@link Correlate#getRequiredColumns()} contains the ordinal of
 *     each field that the uses of the variable of the Correlate read. A
 *     use that reads the full row reads all the fields</td></tr>
 * </table>
 *
 * <h2>Rules for code that creates or changes a plan</h2>
 *
 * <table>
 *   <caption>Operations on declarations</caption>
 *   <tr><th>Operation</th><th>Rule</th></tr>
 *   <tr><td>Make a declaration</td>
 *     <td>Get a new id from {@link
 *     org.apache.calcite.plan.RelOptCluster#createCorrel()}. No other
 *     variable of the cluster has this id</td></tr>
 *   <tr><td>Replace a declaration</td>
 *     <td>The new node can keep the id, because the old node is not in the
 *     plan after the change</td></tr>
 *   <tr><td>Copy a declaration</td>
 *     <td>If the plan keeps the old node and also gets a new node that is
 *     not equal to the old node, rename the variable of the new
 *     node</td></tr>
 *   <tr><td>Insert a subplan into a plan</td>
 *     <td>Use capture-avoiding substitution: alpha-rename each variable that
 *     the subplan declares, if the plan declares or uses the same id. The
 *     free variables of the subplan keep their ids</td></tr>
 *   <tr><td>Remove a declaration</td>
 *     <td>First remove all the uses of the variable, or replace them with
 *     other expressions</td></tr>
 * </table>
 *
 * <p>Any plan returned by a planner must obey these invariants.
 *
 * <p>Note: currently {@link
 * org.apache.calcite.adapter.enumerable.EnumerableBatchNestedLoopJoin} does
 * not obey invariants 2 and 3. It is a Join, but it declares many variables,
 * each variable refers to a row of the left input, and the right input uses
 * the variables. See
 * <a href="https://issues.apache.org/jira/browse/CALCITE-7955">[CALCITE-7955]
 * EnumerableBatchNestedLoopJoin abuses correlation id in its implementation</a>.
 *
 * <h2>Algorithm</h2>
 *
 * <p>A free variable of a sub-plan is a variable that the sub-plan uses, but
 * does not declare (see the terms above). For each sub-plan, the checker
 * computes the uses of its free variables. For example, in the plan above
 * after the removal of the sub-query:
 *
 * <table>
 *   <caption>Free variables of each sub-plan</caption>
 *   <tr><th>Root of the sub-plan</th><th>Free variables</th></tr>
 *   <tr><td>LogicalCorrelate</td>
 *     <td>None. The Correlate declares $cor0, and the use $cor0.K is in its
 *     right input, thus in the scope of $cor0</td></tr>
 *   <tr><td>LogicalAggregate, LogicalProject, LogicalFilter</td>
 *     <td>$cor0, which the LogicalFilter uses as $cor0.K. These sub-plans do
 *     not contain the Correlate</td></tr>
 *   <tr><td>LogicalTableScan</td>
 *     <td>None. A scan uses no variables</td></tr>
 * </table>
 *
 * <p>The checker computes these uses from the bottom up. For the sub-plan
 * with root N:
 *
 * <blockquote><pre>
 * uses of the free variables of N
 *   = uses in the row expressions of N
 *   + uses of the free variables of the sub-queries of N
 *   + uses of the free variables of the inputs of N
 *   - uses in the scope of the variable that N declares
 * </pre></blockquote>
 *
 * <p>The checker keeps the result for each node, thus it visits each Rel
 * node one time, also when the plan is a DAG that reaches the node along
 * many paths. A complete plan has no free variables: each use of a free
 * variable of the root breaks invariant 2.
 *
 * <table>
 *   <caption>Steps for one node</caption>
 *   <tr><th>Node</th><th>Action</th></tr>
 *   <tr><td>A node that declares a variable</td>
 *     <td>Check that the node declares one variable only (invariant 3).
 *     Compare the node with the first node that declared the same id
 *     (invariant 1)</td></tr>
 *   <tr><td>A Correlate</td>
 *     <td>Take the uses of its variable from the uses of the free variables
 *     of the right input. These uses are in scope. Compare the type of each
 *     one with the type of the left row (invariant 4). Check that the
 *     required columns contain each field that these uses read
 *     (invariant 5)</td></tr>
 *   <tr><td>A Project, Filter or Join</td>
 *     <td>Take the uses of its variable from the uses of the free variables
 *     of its sub-queries. These uses are in scope. Compare the type of each
 *     one with the type of the row that the variable refers to
 *     (invariant 4)</td></tr>
 *   <tr><td>The root of the plan</td>
 *     <td>Each use of a free variable of the root breaks invariant 2</td></tr>
 * </table>
 *
 * @see RelValidityChecker
 */
public class CorrelateInvariantChecker {
  /** Field ordinal of a use that reads the full row. */
  private static final int FULL_ROW = -1;

  private final Set<String> violations = new LinkedHashSet<>();

  /** The first node that declares each correlation id. */
  private final Map<CorrelationId, RelNode> declarations = new HashMap<>();

  /** For each node that the checker completed, the uses of the free
   * variables of the sub-plan that has this node as its root. */
  private final IdentityHashMap<RelNode, Set<Use>> freeUses = new IdentityHashMap<>();

  private CorrelateInvariantChecker() {
  }

  /** Returns whether a complete plan obeys the invariants of correlation
   * variables. */
  public static boolean isValid(RelNode plan, Litmus litmus) {
    final List<String> violations = violations(plan);
    if (violations.isEmpty()) {
      return litmus.succeed();
    }
    return litmus.fail("{}", String.join("\n", violations));
  }

  /** Returns one message for each invariant of correlation variables that a
   * complete plan does not obey. */
  public static List<String> violations(RelNode plan) {
    final CorrelateInvariantChecker checker = new CorrelateInvariantChecker();
    // A complete plan has no free variables
    for (Use use : checker.freeUsesOf(plan)) {
      final CorrelationId id = use.variable.id;
      final RelNode declaration = checker.declarations.get(id);
      if (declaration == null) {
        // Invariant 2: used only within scope
        checker.violations.add(id + " is used in "
            + describe(use.node) + ", but no node declares it");
      } else if (declaration.deepEquals(use.node)) {
        // Invariant 2: used only within scope
        checker.violations.add(id + " is used in "
            + describe(use.node) + ", but not in a sub-query of that node");
      } else {
        // Invariant 2: used only within scope
        checker.violations.add(id + " is used in "
            + describe(use.node) + ", which is not in the scope of "
            + describe(declaration));
      }
    }
    return ImmutableList.copyOf(checker.violations);
  }

  private static String describe(RelNode node) {
    return node.getRelTypeName() + "#" + node.getId();
  }

  /** Returns the type of the input row on which a Project, Filter or Join
   * evaluates its row expressions: an
   * {@link org.apache.calcite.rex.RexInputRef} in these expressions is a
   * field of this row. For a Join, this row has the fields of the left
   * input followed by the fields of the right input. Returns null for other
   * nodes. */
  private static @Nullable RelDataType inputRowType(RelNode node) {
    if (node instanceof Project || node instanceof Filter) {
      return node.getInput(0).getRowType();
    }
    if (node instanceof Join) {
      final Join join = (Join) node;
      return SqlValidatorUtil.createJoinType(
          join.getCluster().getTypeFactory(), join.getLeft().getRowType(),
          join.getRight().getRowType(), null, ImmutableList.of());
    }
    return null;
  }

  /** Checks the type of a use against the type of the row that its variable
   * refers to. */
  private void checkType(Use use, RelDataType rowType) {
    // TODO: eventually we should strengthen this invariant to
    // require that the type of each use is exactly the row type (field names
    // excepted). About 200 core tests have plans that do not obey this rule.
    final CorrelationId id = use.variable.id;
    final RelDataType type = use.variable.getType();
    if (use.field == FULL_ROW) {
      if (!type.isStruct()
          || !RelOptUtil.areRowTypesEqual(type, rowType, false)) {
        // Invariant 4: type
        violations.add(id + " has type "
            + type.getFullTypeString() + " in " + describe(use.node)
            + ", but it refers to a row of type "
            + rowType.getFullTypeString());
      }
    } else if (use.field >= rowType.getFieldCount()) {
      // Invariant 4: type
      violations.add(describe(use.node) + " reads field "
          + use.field + " of " + id + ", but it refers to a row with "
          + rowType.getFieldCount() + " fields");
    } else {
      final RelDataType fieldType = type.getFieldList().get(use.field).getType();
      final RelDataType rowFieldType =
          rowType.getFieldList().get(use.field).getType();
      if (!fieldType.equals(rowFieldType)) {
        // Invariant 4: type
        violations.add("Field " + use.field + " of " + id
            + " has type " + fieldType.getFullTypeString() + " in "
            + describe(use.node) + ", but it has type "
            + rowFieldType.getFullTypeString() + " in the row");
      }
    }
  }

  /** Checks the uses that are in the scope of the variables of a node.
   *
   * @param scopeUses Uses of the free variables of the sub-plans that are
   *                  the scope
   * @param declared  Variables that the node declares
   * @param rowType   Type of the row that these variables refer to
   * @param free      Receives the uses of other variables, which stay free
   * @return Ordinals of the fields that the uses in scope read
   */
  private BitSet checkUsesInScope(Set<Use> scopeUses, Set<CorrelationId> declared,
      RelDataType rowType, Set<Use> free) {
    final BitSet read = new BitSet();
    for (Use use : scopeUses) {
      if (!declared.contains(use.variable.id)) {
        free.add(use);
        continue;
      }
      checkType(use, rowType);
      if (use.field == FULL_ROW) {
        read.set(0, rowType.getFieldCount());
      } else {
        read.set(use.field);
      }
    }
    return read;
  }

  /** Finds the uses in the row expressions of a node.
   *
   * @param ownUses      Receives the uses that are not in a sub-query
   * @param subQueryUses Receives the uses of the free variables of the
   *                     sub-queries
   */
  private void addExpressionUses(RelNode node, Set<Use> ownUses, Set<Use> subQueryUses) {
    final RexShuttle finder = new RexShuttle() {
      @Override public RexNode visitCorrelVariable(RexCorrelVariable variable) {
        ownUses.add(new Use(variable, FULL_ROW, node));
        return variable;
      }

      @Override public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
        final RexNode expr = fieldAccess.getReferenceExpr();
        if (expr instanceof RexCorrelVariable) {
          final int field = fieldAccess.getField().getIndex();
          ownUses.add(new Use((RexCorrelVariable) expr, field, node));
          return fieldAccess;
        }
        return super.visitFieldAccess(fieldAccess);
      }

      @Override public RexNode visitSubQuery(RexSubQuery subQuery) {
        subQueryUses.addAll(freeUsesOf(subQuery.rel));
        return super.visitSubQuery(subQuery);
      }
    };
    if (node instanceof Calc) {
      // A Calc can store $cor0.f as two expressions, $cor0 and $t.f;
      // reconstruct $cor0.f from the two expressions
      final RexProgram program = ((Calc) node).getProgram();
      program.expandList(program.getProjectList()).forEach(e -> e.accept(finder));
      final RexLocalRef condition = program.getCondition();
      if (condition != null) {
        program.expandLocalRef(condition).accept(finder);
      }
    } else {
      node.accept(finder);
    }
  }

  /** Returns the uses of the free variables of the sub-plan that has
   * {@code node0} as its root: the variables that the sub-plan uses, but
   * does not declare. Also makes the checks of each node of the sub-plan,
   * one time for each node. */
  private Set<Use> freeUsesOf(RelNode node0) {
    final RelNode node = node0.stripped();
    final Set<Use> known = freeUses.get(node);
    if (known != null) {
      // The plan is a DAG, and the checker reached this node on another path
      return known;
    }
    final Set<CorrelationId> declared = node.getVariablesSet();
    for (CorrelationId id : declared) {
      final RelNode previous = declarations.putIfAbsent(id, node);
      if (previous != null && previous != node && !previous.deepEquals(node)) {
        // Invariant 1: unique ids
        violations.add(id + " is declared by "
            + describe(previous) + " and by " + describe(node));
      }
    }
    final Set<Use> free = new LinkedHashSet<>();
    final Set<Use> subQueryUses = new LinkedHashSet<>();
    addExpressionUses(node, free, subQueryUses);
    if (node instanceof Correlate) {
      final Correlate correlate = (Correlate) node;
      free.addAll(subQueryUses);
      free.addAll(freeUsesOf(correlate.getLeft()));
      // The scope of the variable is the right input
      final BitSet read =
          checkUsesInScope(freeUsesOf(correlate.getRight()), declared,
              correlate.getLeft().getRowType(), free);
      if (!correlate.getRequiredColumns().contains(ImmutableBitSet.fromBitSet(read))) {
        // Invariant 5: required columns
        violations.add("The right input of " + describe(correlate)
            + " reads fields " + read + " of " + correlate.getCorrelationId()
            + ", but requiredColumns is " + correlate.getRequiredColumns());
      }
    } else {
      if (declared.size() > 1) {
        // Invariant 3: one variable per node
        violations.add(describe(node) + " declares " + declared.size() + " variables");
      }
      // A Project, Filter or Join can use its variable only in sub-queries.
      // The variable refers to the row on which the node evaluates its row expressions
      final RelDataType rowType = inputRowType(node);
      if (rowType == null) {
        free.addAll(subQueryUses);
      } else {
        checkUsesInScope(subQueryUses, declared, rowType, free);
      }
      for (RelNode input : node.getInputs()) {
        free.addAll(freeUsesOf(input));
      }
    }
    freeUses.put(node, free);
    return free;
  }

  /** A use of a variable in a row expression of a node. */
  private static class Use {
    final RexCorrelVariable variable;
    /** Ordinal of the field that the use reads, or {@link #FULL_ROW}. */
    final int field;
    /** Node that contains the use in a row expression. */
    final RelNode node;

    Use(RexCorrelVariable variable, int field, RelNode node) {
      this.variable = variable;
      this.field = field;
      this.node = node;
    }
  }
}
