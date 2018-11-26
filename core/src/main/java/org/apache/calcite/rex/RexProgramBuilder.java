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
package org.apache.calcite.rex;

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Workspace for constructing a {@link RexProgram}.
 *
 * <p>RexProgramBuilder is necessary because a {@link RexProgram} is immutable.
 * (The {@link String} class has the same problem: it is immutable, so they
 * introduced {@link StringBuilder}.)
 */
public class RexProgramBuilder {
  //~ Instance fields --------------------------------------------------------

  private final RexBuilder rexBuilder;
  private final RelDataType inputRowType;
  private final List<RexNode> exprList = new ArrayList<>();
  private final Map<Pair<RexNode, String>, RexLocalRef> exprMap =
      new HashMap<>();
  private final List<RexLocalRef> localRefList = new ArrayList<>();
  private final List<RexLocalRef> projectRefList = new ArrayList<>();
  private final List<String> projectNameList = new ArrayList<>();
  private final RexSimplify simplify;
  private RexLocalRef conditionRef = null;
  private boolean validating;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a program-builder that will not simplify.
   */
  public RexProgramBuilder(RelDataType inputRowType, RexBuilder rexBuilder) {
    this(inputRowType, rexBuilder, null);
  }

  /**
   * Creates a program-builder.
   */
  private RexProgramBuilder(RelDataType inputRowType, RexBuilder rexBuilder,
      RexSimplify simplify) {
    this.inputRowType = Objects.requireNonNull(inputRowType);
    this.rexBuilder = Objects.requireNonNull(rexBuilder);
    this.simplify = simplify; // may be null
    this.validating = assertionsAreEnabled();

    // Pre-create an expression for each input field.
    if (inputRowType.isStruct()) {
      final List<RelDataTypeField> fields = inputRowType.getFieldList();
      for (int i = 0; i < fields.size(); i++) {
        registerInternal(RexInputRef.of(i, fields), false);
      }
    }
  }

  /**
   * Creates a program builder with the same contents as a program.
   *
   * @param rexBuilder     Rex builder
   * @param inputRowType   Input row type
   * @param exprList       Common expressions
   * @param projectList    Projections
   * @param condition      Condition, or null
   * @param outputRowType  Output row type
   * @param normalize      Whether to normalize
   * @param simplify       Simplifier, or null to not simplify
   */
  private RexProgramBuilder(
      RexBuilder rexBuilder,
      final RelDataType inputRowType,
      final List<RexNode> exprList,
      final Iterable<? extends RexNode> projectList,
      RexNode condition,
      final RelDataType outputRowType,
      boolean normalize,
      RexSimplify simplify) {
    this(inputRowType, rexBuilder, simplify);

    // Create a shuttle for registering input expressions.
    final RexShuttle shuttle =
        new RegisterMidputShuttle(true, exprList);

    // If we are not normalizing, register all internal expressions. If we
    // are normalizing, expressions will be registered if and when they are
    // first used.
    if (!normalize) {
      for (RexNode expr : exprList) {
        expr.accept(shuttle);
      }
    }

    final RexShuttle expander = new RexProgram.ExpansionShuttle(exprList);

    // Register project expressions
    // and create a named project item.
    final List<RelDataTypeField> fieldList = outputRowType.getFieldList();
    for (Pair<? extends RexNode, RelDataTypeField> pair
        : Pair.zip(projectList, fieldList)) {
      final RexNode project;
      if (simplify != null) {
        project = simplify.simplify(pair.left.accept(expander));
      } else {
        project = pair.left;
      }
      final String name = pair.right.getName();
      final RexLocalRef ref = (RexLocalRef) project.accept(shuttle);
      addProject(ref.getIndex(), name);
    }

    // Register the condition, if there is one.
    if (condition != null) {
      if (simplify != null) {
        condition = simplify.simplify(
            rexBuilder.makeCall(SqlStdOperatorTable.IS_TRUE,
                condition.accept(expander)));
        if (condition.isAlwaysTrue()) {
          condition = null;
        }
      }
      if (condition != null) {
        final RexLocalRef ref = (RexLocalRef) condition.accept(shuttle);
        addCondition(ref);
      }
    }
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Returns whether assertions are enabled in this class.
   */
  private static boolean assertionsAreEnabled() {
    boolean assertionsEnabled = false;
    //noinspection AssertWithSideEffects
    assert assertionsEnabled = true;
    return assertionsEnabled;
  }

  private void validate(final RexNode expr, final int fieldOrdinal) {
    final RexVisitor<Void> validator =
        new RexVisitorImpl<Void>(true) {
          public Void visitInputRef(RexInputRef input) {
            final int index = input.getIndex();
            final List<RelDataTypeField> fields =
                inputRowType.getFieldList();
            if (index < fields.size()) {
              final RelDataTypeField inputField = fields.get(index);
              if (input.getType() != inputField.getType()) {
                throw new AssertionError("in expression " + expr
                    + ", field reference " + input + " has inconsistent type");
              }
            } else {
              if (index >= fieldOrdinal) {
                throw new AssertionError("in expression " + expr
                    + ", field reference " + input + " is out of bounds");
              }
              RexNode refExpr = exprList.get(index);
              if (refExpr.getType() != input.getType()) {
                throw new AssertionError("in expression " + expr
                    + ", field reference " + input + " has inconsistent type");
              }
            }
            return null;
          }
        };
    expr.accept(validator);
  }

  /**
   * Adds a project expression to the program.
   *
   * <p>The expression specified in terms of the input fields. If not, call
   * {@link #registerOutput(RexNode)} first.
   *
   * @param expr Expression to add
   * @param name Name of field in output row type; if null, a unique name will
   *             be generated when the program is created
   * @return the ref created
   */
  public RexLocalRef addProject(RexNode expr, String name) {
    final RexLocalRef ref = registerInput(expr);
    return addProject(ref.getIndex(), name);
  }

  /**
   * Adds a projection based upon the <code>index</code>th expression.
   *
   * @param ordinal Index of expression to project
   * @param name    Name of field in output row type; if null, a unique name
   *                will be generated when the program is created
   * @return the ref created
   */
  public RexLocalRef addProject(int ordinal, final String name) {
    final RexLocalRef ref = localRefList.get(ordinal);
    projectRefList.add(ref);
    projectNameList.add(name);
    return ref;
  }

  /**
   * Adds a project expression to the program at a given position.
   *
   * <p>The expression specified in terms of the input fields. If not, call
   * {@link #registerOutput(RexNode)} first.
   *
   * @param at   Position in project list to add expression
   * @param expr Expression to add
   * @param name Name of field in output row type; if null, a unique name will
   *             be generated when the program is created
   * @return the ref created
   */
  public RexLocalRef addProject(int at, RexNode expr, String name) {
    final RexLocalRef ref = registerInput(expr);
    projectRefList.add(at, ref);
    projectNameList.add(at, name);
    return ref;
  }

  /**
   * Adds a projection based upon the <code>index</code>th expression at a
   * given position.
   *
   * @param at      Position in project list to add expression
   * @param ordinal Index of expression to project
   * @param name    Name of field in output row type; if null, a unique name
   *                will be generated when the program is created
   * @return the ref created
   */
  public RexLocalRef addProject(int at, int ordinal, final String name) {
    return addProject(
        at,
        localRefList.get(ordinal),
        name);
  }

  /**
   * Sets the condition of the program.
   *
   * <p>The expression must be specified in terms of the input fields. If
   * not, call {@link #registerOutput(RexNode)} first.</p>
   */
  public void addCondition(RexNode expr) {
    assert expr != null;
    if (conditionRef == null) {
      conditionRef = registerInput(expr);
    } else {
      // AND the new condition with the existing condition.
      // If the new condition is identical to the existing condition, skip it.
      RexLocalRef ref = registerInput(expr);
      if (!ref.equals(conditionRef)) {
        conditionRef =
            registerInput(
                rexBuilder.makeCall(
                    SqlStdOperatorTable.AND,
                    conditionRef,
                    ref));
      }
    }
  }

  /**
   * Registers an expression in the list of common sub-expressions, and
   * returns a reference to that expression.

   * <p>The expression must be expressed in terms of the <em>inputs</em> of
   * this program.</p>
   */
  public RexLocalRef registerInput(RexNode expr) {
    final RexShuttle shuttle = new RegisterInputShuttle(true);
    final RexNode ref = expr.accept(shuttle);
    return (RexLocalRef) ref;
  }

  /**
   * Converts an expression expressed in terms of the <em>outputs</em> of this
   * program into an expression expressed in terms of the <em>inputs</em>,
   * registers it in the list of common sub-expressions, and returns a
   * reference to that expression.
   *
   * @param expr Expression to register
   */
  public RexLocalRef registerOutput(RexNode expr) {
    final RexShuttle shuttle = new RegisterOutputShuttle(exprList);
    final RexNode ref = expr.accept(shuttle);
    return (RexLocalRef) ref;
  }

  /**
   * Registers an expression in the list of common sub-expressions, and
   * returns a reference to that expression.
   *
   * <p>If an equivalent sub-expression already exists, creates another
   * expression only if <code>force</code> is true.
   *
   * @param expr  Expression to register
   * @param force Whether to create a new sub-expression if an equivalent
   *              sub-expression exists.
   */
  private RexLocalRef registerInternal(RexNode expr, boolean force) {
    final RexSimplify simplify =
        new RexSimplify(rexBuilder, RelOptPredicateList.EMPTY, RexUtil.EXECUTOR);
    expr = simplify.simplifyPreservingType(expr);

    RexLocalRef ref;
    final Pair<RexNode, String> key;
    if (expr instanceof RexLocalRef) {
      key = null;
      ref = (RexLocalRef) expr;
    } else {
      key = RexUtil.makeKey(expr);
      ref = exprMap.get(key);
    }
    if (ref == null) {
      if (validating) {
        validate(
            expr,
            exprList.size());
      }

      // Add expression to list, and return a new reference to it.
      ref = addExpr(expr);
      exprMap.put(key, ref);
    } else {
      if (force) {
        // Add expression to list, but return the previous ref.
        addExpr(expr);
      }
    }

    for (;;) {
      int index = ref.index;
      final RexNode expr2 = exprList.get(index);
      if (expr2 instanceof RexLocalRef) {
        ref = (RexLocalRef) expr2;
      } else {
        return ref;
      }
    }
  }

  /**
   * Adds an expression to the list of common expressions, and returns a
   * reference to the expression. <b>DOES NOT CHECK WHETHER THE EXPRESSION
   * ALREADY EXISTS</b>.
   *
   * @param expr Expression
   * @return Reference to expression
   */
  public RexLocalRef addExpr(RexNode expr) {
    RexLocalRef ref;
    final int index = exprList.size();
    exprList.add(expr);
    ref =
        new RexLocalRef(
            index,
            expr.getType());
    localRefList.add(ref);
    return ref;
  }

  /**
   * Converts the state of the program builder to an immutable program,
   * normalizing in the process.
   *
   * <p>It is OK to call this method, modify the program specification (by
   * adding projections, and so forth), and call this method again.
   */
  public RexProgram getProgram() {
    return getProgram(true);
  }

  /**
   * Converts the state of the program builder to an immutable program.
   *
   * <p>It is OK to call this method, modify the program specification (by
   * adding projections, and so forth), and call this method again.
   *
   * @param normalize Whether to normalize
   */
  public RexProgram getProgram(boolean normalize) {
    assert projectRefList.size() == projectNameList.size();

    // Make sure all fields have a name.
    generateMissingNames();
    RelDataType outputRowType = computeOutputRowType();

    if (normalize) {
      return create(
          rexBuilder,
          inputRowType,
          exprList,
          projectRefList,
          conditionRef,
          outputRowType,
          true)
          .getProgram(false);
    }

    return new RexProgram(
        inputRowType,
        exprList,
        projectRefList,
        conditionRef,
        outputRowType);
  }

  private RelDataType computeOutputRowType() {
    return RexUtil.createStructType(rexBuilder.typeFactory, projectRefList,
        projectNameList, null);
  }

  private void generateMissingNames() {
    int i = -1;
    int j = 0;
    for (String projectName : projectNameList) {
      ++i;
      if (projectName == null) {
        while (true) {
          final String candidateName = "$" + j++;
          if (!projectNameList.contains(candidateName)) {
            projectNameList.set(i, candidateName);
            break;
          }
        }
      }
    }
  }

  /**
   * Creates a program builder and initializes it from an existing program.
   *
   * <p>Calling {@link #getProgram()} immediately after creation will return a
   * program equivalent (in terms of external behavior) to the existing
   * program.
   *
   * <p>The existing program will not be changed. (It cannot: programs are
   * immutable.)
   *
   * @param program    Existing program
   * @param rexBuilder Rex builder
   * @param normalize  Whether to normalize
   * @return A program builder initialized with an equivalent program
   */
  public static RexProgramBuilder forProgram(
      RexProgram program,
      RexBuilder rexBuilder,
      boolean normalize) {
    assert program.isValid(Litmus.THROW, null);
    final RelDataType inputRowType = program.getInputRowType();
    final List<RexLocalRef> projectRefs = program.getProjectList();
    final RexLocalRef conditionRef = program.getCondition();
    final List<RexNode> exprs = program.getExprList();
    final RelDataType outputRowType = program.getOutputRowType();
    return create(
        rexBuilder,
        inputRowType,
        exprs,
        projectRefs,
        conditionRef,
        outputRowType,
        normalize,
        false);
  }

  /**
   * Creates a program builder with the same contents as a program.
   *
   * <p>If {@code normalize}, converts the program to canonical form. In
   * canonical form, in addition to the usual constraints:
   *
   * <ul>
   * <li>The first N internal expressions are {@link RexInputRef}s to the N
   * input fields;
   * <li>Subsequent internal expressions reference only preceding expressions;
   * <li>Arguments to {@link RexCall}s must be {@link RexLocalRef}s (that is,
   * expressions must have maximum depth 1)
   * </ul>
   *
   * <p>there are additional constraints:
   *
   * <ul>
   * <li>Expressions appear in the left-deep order they are needed by
   * the projections and (if present) the condition. Thus, expression N+1
   * is the leftmost argument (literal or or call) in the expansion of
   * projection #0.
   * <li>There are no duplicate expressions
   * <li>There are no unused expressions
   * </ul>
   *
   * @param rexBuilder     Rex builder
   * @param inputRowType   Input row type
   * @param exprList       Common expressions
   * @param projectList    Projections
   * @param condition      Condition, or null
   * @param outputRowType  Output row type
   * @param normalize      Whether to normalize
   * @param simplify       Whether to simplify expressions
   * @return A program builder
   */
  public static RexProgramBuilder create(
      RexBuilder rexBuilder,
      final RelDataType inputRowType,
      final List<RexNode> exprList,
      final List<? extends RexNode> projectList,
      final RexNode condition,
      final RelDataType outputRowType,
      boolean normalize,
      RexSimplify simplify) {
    return new RexProgramBuilder(rexBuilder, inputRowType, exprList,
        projectList, condition, outputRowType, normalize, simplify);
  }

  @Deprecated // to be removed before 2.0
  public static RexProgramBuilder create(
      RexBuilder rexBuilder,
      final RelDataType inputRowType,
      final List<RexNode> exprList,
      final List<? extends RexNode> projectList,
      final RexNode condition,
      final RelDataType outputRowType,
      boolean normalize,
      boolean simplify_) {
    RexSimplify simplify = null;
    if (simplify_) {
      simplify = new RexSimplify(rexBuilder, RelOptPredicateList.EMPTY,
          RexUtil.EXECUTOR);
    }
    return new RexProgramBuilder(rexBuilder, inputRowType, exprList,
        projectList, condition, outputRowType, normalize, simplify);
  }

  @Deprecated // to be removed before 2.0
  public static RexProgramBuilder create(
      RexBuilder rexBuilder,
      final RelDataType inputRowType,
      final List<RexNode> exprList,
      final List<? extends RexNode> projectList,
      final RexNode condition,
      final RelDataType outputRowType,
      boolean normalize) {
    return create(rexBuilder, inputRowType, exprList, projectList, condition,
        outputRowType, normalize, null);
  }

  /**
   * Creates a program builder with the same contents as a program, applying a
   * shuttle first.
   *
   * <p>TODO: Refactor the above create method in terms of this one.
   *
   * @param rexBuilder     Rex builder
   * @param inputRowType   Input row type
   * @param exprList       Common expressions
   * @param projectRefList Projections
   * @param conditionRef   Condition, or null
   * @param outputRowType  Output row type
   * @param shuttle        Shuttle to apply to each expression before adding it
   *                       to the program builder
   * @param updateRefs     Whether to update references that changes as a result
   *                       of rewrites made by the shuttle
   * @return A program builder
   */
  public static RexProgramBuilder create(
      RexBuilder rexBuilder,
      final RelDataType inputRowType,
      final List<RexNode> exprList,
      final List<RexLocalRef> projectRefList,
      final RexLocalRef conditionRef,
      final RelDataType outputRowType,
      final RexShuttle shuttle,
      final boolean updateRefs) {
    final RexProgramBuilder progBuilder =
        new RexProgramBuilder(inputRowType, rexBuilder);
    progBuilder.add(
        exprList,
        projectRefList,
        conditionRef,
        outputRowType,
        shuttle,
        updateRefs);
    return progBuilder;
  }

  @Deprecated // to be removed before 2.0
  public static RexProgram normalize(
      RexBuilder rexBuilder,
      RexProgram program) {
    return program.normalize(rexBuilder, null);
  }

  /**
   * Adds a set of expressions, projections and filters, applying a shuttle
   * first.
   *
   * @param exprList       Common expressions
   * @param projectRefList Projections
   * @param conditionRef   Condition, or null
   * @param outputRowType  Output row type
   * @param shuttle        Shuttle to apply to each expression before adding it
   *                       to the program builder
   * @param updateRefs     Whether to update references that changes as a result
   *                       of rewrites made by the shuttle
   */
  private void add(
      List<RexNode> exprList,
      List<RexLocalRef> projectRefList,
      RexLocalRef conditionRef,
      final RelDataType outputRowType,
      RexShuttle shuttle,
      boolean updateRefs) {
    final List<RelDataTypeField> outFields = outputRowType.getFieldList();
    final RexShuttle registerInputShuttle = new RegisterInputShuttle(false);

    // For each common expression, first apply the user's shuttle, then
    // register the result.
    // REVIEW jpham 28-Apr-2006: if the user shuttle rewrites an input
    // expression, then input references may change
    List<RexLocalRef> newRefs = new ArrayList<>(exprList.size());
    RexShuttle refShuttle = new UpdateRefShuttle(newRefs);
    int i = 0;
    for (RexNode expr : exprList) {
      RexNode newExpr = expr;
      if (updateRefs) {
        newExpr = expr.accept(refShuttle);
      }
      newExpr = newExpr.accept(shuttle);
      newRefs.add(
          i++,
          (RexLocalRef) newExpr.accept(registerInputShuttle));
    }
    i = -1;
    for (RexLocalRef oldRef : projectRefList) {
      ++i;
      RexLocalRef ref = oldRef;
      if (updateRefs) {
        ref = (RexLocalRef) oldRef.accept(refShuttle);
      }
      ref = (RexLocalRef) ref.accept(shuttle);
      this.projectRefList.add(ref);
      final String name = outFields.get(i).getName();
      assert name != null;
      projectNameList.add(name);
    }
    if (conditionRef != null) {
      if (updateRefs) {
        conditionRef = (RexLocalRef) conditionRef.accept(refShuttle);
      }
      conditionRef = (RexLocalRef) conditionRef.accept(shuttle);
      addCondition(conditionRef);
    }
  }

  /**
   * Merges two programs together, and normalizes the result.
   *
   * @param topProgram    Top program. Its expressions are in terms of the
   *                      outputs of the bottom program.
   * @param bottomProgram Bottom program. Its expressions are in terms of the
   *                      result fields of the relational expression's input
   * @param rexBuilder    Rex builder
   * @return Merged program
   * @see #mergePrograms(RexProgram, RexProgram, RexBuilder, boolean)
   */
  public static RexProgram mergePrograms(
      RexProgram topProgram,
      RexProgram bottomProgram,
      RexBuilder rexBuilder) {
    return mergePrograms(topProgram, bottomProgram, rexBuilder, true);
  }

  /**
   * Merges two programs together.
   *
   * <p>All expressions become common sub-expressions. For example, the query
   *
   * <blockquote><pre>SELECT x + 1 AS p, x + y AS q FROM (
   *   SELECT a + b AS x, c AS y
   *   FROM t
   *   WHERE c = 6)}</pre></blockquote>
   *
   * <p>would be represented as the programs
   *
   * <blockquote><pre>
   *   Calc:
   *       Projects={$2, $3},
   *       Condition=null,
   *       Exprs={$0, $1, $0 + 1, $0 + $1})
   *   Calc(
   *       Projects={$3, $2},
   *       Condition={$4}
   *       Exprs={$0, $1, $2, $0 + $1, $2 = 6}
   * </pre></blockquote>
   *
   * <p>The merged program is
   *
   * <blockquote><pre>
   *   Calc(
   *      Projects={$4, $5}
   *      Condition=$6
   *      Exprs={0: $0       // a
   *             1: $1        // b
   *             2: $2        // c
   *             3: ($0 + $1) // x = a + b
   *             4: ($3 + 1)  // p = x + 1
   *             5: ($3 + $2) // q = x + y
   *             6: ($2 = 6)  // c = 6
   * </pre></blockquote>
   *
   * <p>Another example:</p>
   *
   * <blockquote>
   * <pre>SELECT *
   * FROM (
   *   SELECT a + b AS x, c AS y
   *   FROM t
   *   WHERE c = 6)
   * WHERE x = 5</pre>
   * </blockquote>
   *
   * <p>becomes
   *
   * <blockquote>
   * <pre>SELECT a + b AS x, c AS y
   * FROM t
   * WHERE c = 6 AND (a + b) = 5</pre>
   * </blockquote>
   *
   * @param topProgram    Top program. Its expressions are in terms of the
   *                      outputs of the bottom program.
   * @param bottomProgram Bottom program. Its expressions are in terms of the
   *                      result fields of the relational expression's input
   * @param rexBuilder    Rex builder
   * @param normalize     Whether to convert program to canonical form
   * @return Merged program
   */
  public static RexProgram mergePrograms(
      RexProgram topProgram,
      RexProgram bottomProgram,
      RexBuilder rexBuilder,
      boolean normalize) {
    // Initialize a program builder with the same expressions, outputs
    // and condition as the bottom program.
    assert bottomProgram.isValid(Litmus.THROW, null);
    assert topProgram.isValid(Litmus.THROW, null);
    final RexProgramBuilder progBuilder =
        RexProgramBuilder.forProgram(bottomProgram, rexBuilder, false);

    // Drive from the outputs of the top program. Register each expression
    // used as an output.
    final List<RexLocalRef> projectRefList =
        progBuilder.registerProjectsAndCondition(topProgram);

    // Switch to the projects needed by the top program. The original
    // projects of the bottom program are no longer needed.
    progBuilder.clearProjects();
    final RelDataType outputRowType = topProgram.getOutputRowType();
    for (Pair<RexLocalRef, String> pair
        : Pair.zip(projectRefList, outputRowType.getFieldNames(), true)) {
      progBuilder.addProject(pair.left, pair.right);
    }
    RexProgram mergedProg = progBuilder.getProgram(normalize);
    assert mergedProg.isValid(Litmus.THROW, null);
    assert mergedProg.getOutputRowType() == topProgram.getOutputRowType();
    return mergedProg;
  }

  private List<RexLocalRef> registerProjectsAndCondition(RexProgram program) {
    final List<RexNode> exprList = program.getExprList();
    final List<RexLocalRef> projectRefList = new ArrayList<>();
    final RexShuttle shuttle = new RegisterOutputShuttle(exprList);

    // For each project, lookup the expr and expand it so it is in terms of
    // bottomCalc's input fields
    for (RexLocalRef topProject : program.getProjectList()) {
      final RexNode topExpr = exprList.get(topProject.getIndex());
      final RexLocalRef expanded = (RexLocalRef) topExpr.accept(shuttle);

      // Remember the expr, but don't add to the project list yet.
      projectRefList.add(expanded);
    }

    // Similarly for the condition.
    final RexLocalRef topCondition = program.getCondition();
    if (topCondition != null) {
      final RexNode topExpr = exprList.get(topCondition.getIndex());
      final RexLocalRef expanded = (RexLocalRef) topExpr.accept(shuttle);

      addCondition(registerInput(expanded));
    }
    return projectRefList;
  }

  /**
   * Removes all project items.
   *
   * <p>After calling this method, you may need to re-normalize.</p>
   */
  public void clearProjects() {
    projectRefList.clear();
    projectNameList.clear();
  }

  /**
   * Clears the condition.
   *
   * <p>After calling this method, you may need to re-normalize.</p>
   */
  public void clearCondition() {
    conditionRef = null;
  }

  /**
   * Adds a project item for every input field.
   *
   * <p>You cannot call this method if there are other project items.
   */
  public void addIdentity() {
    assert projectRefList.isEmpty();
    for (RelDataTypeField field : inputRowType.getFieldList()) {
      addProject(
          new RexInputRef(
              field.getIndex(),
              field.getType()),
          field.getName());
    }
  }

  /**
   * Creates a reference to a given input field.
   *
   * @param index Ordinal of input field, must be less than the number of
   *              fields in the input type
   * @return Reference to input field
   */
  public RexLocalRef makeInputRef(int index) {
    final List<RelDataTypeField> fields = inputRowType.getFieldList();
    assert index < fields.size();
    final RelDataTypeField field = fields.get(index);
    return new RexLocalRef(
        index,
        field.getType());
  }

  /**
   * Returns the rowtype of the input to the program
   */
  public RelDataType getInputRowType() {
    return inputRowType;
  }

  /**
   * Returns the list of project expressions.
   */
  public List<RexLocalRef> getProjectList() {
    return projectRefList;
  }

  //~ Inner Classes ----------------------------------------------------------

  /** Shuttle that visits a tree of {@link RexNode} and registers them
   * in a program. */
  private abstract class RegisterShuttle extends RexShuttle {
    public RexNode visitCall(RexCall call) {
      final RexNode expr = super.visitCall(call);
      return registerInternal(expr, false);
    }

    public RexNode visitOver(RexOver over) {
      final RexNode expr = super.visitOver(over);
      return registerInternal(expr, false);
    }

    public RexNode visitLiteral(RexLiteral literal) {
      final RexNode expr = super.visitLiteral(literal);
      return registerInternal(expr, false);
    }

    public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
      final RexNode expr = super.visitFieldAccess(fieldAccess);
      return registerInternal(expr, false);
    }

    public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
      final RexNode expr = super.visitDynamicParam(dynamicParam);
      return registerInternal(expr, false);
    }

    public RexNode visitCorrelVariable(RexCorrelVariable variable) {
      final RexNode expr = super.visitCorrelVariable(variable);
      return registerInternal(expr, false);
    }
  }

  /**
   * Shuttle which walks over an expression, registering each sub-expression.
   * Each {@link RexInputRef} is assumed to refer to an <em>input</em> of the
   * program.
   */
  private class RegisterInputShuttle extends RegisterShuttle {
    private final boolean valid;

    protected RegisterInputShuttle(boolean valid) {
      this.valid = valid;
    }

    public RexNode visitInputRef(RexInputRef input) {
      final int index = input.getIndex();
      if (valid) {
        // The expression should already be valid. Check that its
        // index is within bounds.
        if ((index < 0) || (index >= inputRowType.getFieldCount())) {
          assert false
              : "RexInputRef index " + index + " out of range 0.."
              + (inputRowType.getFieldCount() - 1);
        }

        // Check that the type is consistent with the referenced
        // field. If it is an object type, the rules are different, so
        // skip the check.
        assert input.getType().isStruct()
            || RelOptUtil.eq("type1", input.getType(),
                "type2", inputRowType.getFieldList().get(index).getType(),
                Litmus.THROW);
      }

      // Return a reference to the N'th expression, which should be
      // equivalent.
      final RexLocalRef ref = localRefList.get(index);
      return ref;
    }

    public RexNode visitLocalRef(RexLocalRef local) {
      if (valid) {
        // The expression should already be valid.
        final int index = local.getIndex();
        assert index >= 0 : index;
        assert index < exprList.size()
            : "index=" + index + ", exprList=" + exprList;
        assert RelOptUtil.eq(
            "expr type",
            exprList.get(index).getType(),
            "ref type",
            local.getType(),
            Litmus.THROW);
      }

      // Resolve the expression to an input.
      while (true) {
        final int index = local.getIndex();
        final RexNode expr = exprList.get(index);
        if (expr instanceof RexLocalRef) {
          local = (RexLocalRef) expr;
          if (local.index >= index) {
            throw new AssertionError(
                "expr " + local + " references later expr " + local.index);
          }
        } else {
          // Add expression to the list, just so that subsequent
          // expressions don't get screwed up. This expression is
          // unused, so will be eliminated soon.
          return registerInternal(local, false);
        }
      }
    }
  }

  /**
   * Extension to {@link RegisterInputShuttle} which allows expressions to be
   * in terms of inputs or previous common sub-expressions.
   */
  private class RegisterMidputShuttle extends RegisterInputShuttle {
    private final List<RexNode> localExprList;

    protected RegisterMidputShuttle(
        boolean valid,
        List<RexNode> localExprList) {
      super(valid);
      this.localExprList = localExprList;
    }

    public RexNode visitLocalRef(RexLocalRef local) {
      // Convert a local ref into the common-subexpression it references.
      final int index = local.getIndex();
      return localExprList.get(index).accept(this);
    }
  }

  /**
   * Shuttle which walks over an expression, registering each sub-expression.
   * Each {@link RexInputRef} is assumed to refer to an <em>output</em> of the
   * program.
   */
  private class RegisterOutputShuttle extends RegisterShuttle {
    private final List<RexNode> localExprList;

    RegisterOutputShuttle(List<RexNode> localExprList) {
      super();
      this.localExprList = localExprList;
    }

    public RexNode visitInputRef(RexInputRef input) {
      // This expression refers to the Nth project column. Lookup that
      // column and find out what common sub-expression IT refers to.
      final int index = input.getIndex();
      final RexLocalRef local = projectRefList.get(index);
      assert RelOptUtil.eq(
          "type1",
          local.getType(),
          "type2",
          input.getType(),
          Litmus.THROW);
      return local;
    }

    public RexNode visitLocalRef(RexLocalRef local) {
      // Convert a local ref into the common-subexpression it references.
      final int index = local.getIndex();
      return localExprList.get(index).accept(this);
    }
  }

  /**
   * Shuttle which rewires {@link RexLocalRef} using a list of updated
   * references
   */
  private class UpdateRefShuttle extends RexShuttle {
    private List<RexLocalRef> newRefs;

    private UpdateRefShuttle(List<RexLocalRef> newRefs) {
      this.newRefs = newRefs;
    }

    public RexNode visitLocalRef(RexLocalRef localRef) {
      return newRefs.get(localRef.getIndex());
    }
  }
}

// End RexProgramBuilder.java
