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
package org.eigenbase.rel.rules;

import java.util.*;
import java.util.regex.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.util.Stacks;
import org.eigenbase.util.Util;

/**
 * Collection of planner rules that apply various simplifying transformations on
 * RexNode trees. Currently, there are two transformations:
 *
 * <ul>
 * <li>Constant reduction, which evaluates constant subtrees, replacing them
 * with a corresponding RexLiteral
 * <li>Removal of redundant casts, which occurs when the argument into the cast
 * is the same as the type of the resulting cast expression
 * </ul>
 */
public abstract class ReduceExpressionsRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  /**
   * Regular expression that matches the description of all instances of this
   * rule and {@link ReduceValuesRule} also. Use
   * it to prevent the planner from invoking these rules.
   */
  public static final Pattern EXCLUSION_PATTERN =
      Pattern.compile("Reduce(Expressions|Values)Rule.*");

  /**
   * Singleton rule that reduces constants inside a {@link FilterRel}. If the
   * condition is a constant, the filter is removed (if TRUE) or replaced with
   * {@link EmptyRel} (if FALSE or NULL).
   */
  public static final ReduceExpressionsRule FILTER_INSTANCE =
      new ReduceExpressionsRule(FilterRel.class,
          "ReduceExpressionsRule[Filter]") {
        public void onMatch(RelOptRuleCall call) {
          FilterRel filter = call.rel(0);
          List<RexNode> expList = new ArrayList<RexNode>(filter.getChildExps());
          RexNode newConditionExp;
          boolean reduced;
          if (reduceExpressions(filter, expList)) {
            assert expList.size() == 1;
            newConditionExp = expList.get(0);
            reduced = true;
          } else {
            // No reduction, but let's still test the original
            // predicate to see if it was already a constant,
            // in which case we don't need any runtime decision
            // about filtering.
            newConditionExp = filter.getChildExps().get(0);
            reduced = false;
          }
          if (newConditionExp.isAlwaysTrue()) {
            call.transformTo(
                filter.getChild());
          } else if (
              (newConditionExp instanceof RexLiteral)
                  || RexUtil.isNullLiteral(newConditionExp, true)) {
            call.transformTo(
                new EmptyRel(
                    filter.getCluster(),
                    filter.getRowType()));
          } else if (reduced) {
            call.transformTo(
                RelOptUtil.createFilter(
                    filter.getChild(),
                    expList.get(0)));
          } else {
            if (newConditionExp instanceof RexCall) {
              RexCall rexCall = (RexCall) newConditionExp;
              boolean reverse =
                  rexCall.getOperator()
                      == SqlStdOperatorTable.NOT;
              if (reverse) {
                rexCall = (RexCall) rexCall.getOperands().get(0);
              }
              reduceNotNullableFilter(call, filter, rexCall, reverse);
            }
            return;
          }

          // New plan is absolutely better than old plan.
          call.getPlanner().setImportance(filter, 0.0);
        }

        private void reduceNotNullableFilter(
            RelOptRuleCall call,
            FilterRel filter,
            RexCall rexCall,
            boolean reverse) {
          // If the expression is a IS [NOT] NULL on a non-nullable
          // column, then we can either remove the filter or replace
          // it with an EmptyRel.
          SqlOperator op = rexCall.getOperator();
          boolean alwaysTrue;
          switch (rexCall.getKind()) {
          case IS_NULL:
          case IS_UNKNOWN:
            alwaysTrue = false;
            break;
          case IS_NOT_NULL:
            alwaysTrue = true;
            break;
          default:
            return;
          }
          if (reverse) {
            alwaysTrue = !alwaysTrue;
          }
          RexNode operand = rexCall.getOperands().get(0);
          if (operand instanceof RexInputRef) {
            RexInputRef inputRef = (RexInputRef) operand;
            if (!inputRef.getType().isNullable()) {
              if (alwaysTrue) {
                call.transformTo(filter.getChild());
              } else {
                call.transformTo(
                    new EmptyRel(
                        filter.getCluster(),
                        filter.getRowType()));
              }
            }
          }
        }
      };

  public static final ReduceExpressionsRule PROJECT_INSTANCE =
      new ReduceExpressionsRule(ProjectRel.class,
          "ReduceExpressionsRule[Project]") {
        public void onMatch(RelOptRuleCall call) {
          ProjectRel project = call.rel(0);
          List<RexNode> expList =
              new ArrayList<RexNode>(project.getChildExps());
          if (reduceExpressions(project, expList)) {
            call.transformTo(
                new ProjectRel(
                    project.getCluster(),
                    project.getTraitSet(),
                    project.getChild(),
                    expList,
                    project.getRowType(),
                    ProjectRel.Flags.BOXED));

            // New plan is absolutely better than old plan.
            call.getPlanner().setImportance(project, 0.0);
          }
        }
      };

  public static final ReduceExpressionsRule JOIN_INSTANCE =
      new ReduceExpressionsRule(JoinRelBase.class,
          "ReduceExpressionsRule[Join]") {
        public void onMatch(RelOptRuleCall call) {
          final JoinRelBase join = call.rel(0);
          List<RexNode> expList = new ArrayList<RexNode>(join.getChildExps());
          if (reduceExpressions(join, expList)) {
            call.transformTo(
                join.copy(
                    join.getTraitSet(),
                    expList.get(0),
                    join.getLeft(),
                    join.getRight(),
                    join.getJoinType(),
                    join.isSemiJoinDone()));

            // New plan is absolutely better than old plan.
            call.getPlanner().setImportance(join, 0.0);
          }
        }
      };

  public static final ReduceExpressionsRule CALC_INSTANCE =
      new ReduceExpressionsRule(CalcRel.class, "ReduceExpressionsRule[Calc]") {
        public void onMatch(RelOptRuleCall call) {
          CalcRel calc = call.rel(0);
          RexProgram program = calc.getProgram();
          final List<RexNode> exprList = program.getExprList();

          // Form a list of expressions with sub-expressions fully expanded.
          final List<RexNode> expandedExprList =
              new ArrayList<RexNode>(exprList.size());
          final RexShuttle shuttle =
              new RexShuttle() {
                public RexNode visitLocalRef(RexLocalRef localRef) {
                  return expandedExprList.get(localRef.getIndex());
                }
              };
          for (RexNode expr : exprList) {
            expandedExprList.add(expr.accept(shuttle));
          }
          if (reduceExpressions(calc, expandedExprList)) {
            final RexProgramBuilder builder =
                new RexProgramBuilder(
                    calc.getChild().getRowType(),
                    calc.getCluster().getRexBuilder());
            List<RexLocalRef> list = new ArrayList<RexLocalRef>();
            for (RexNode expr : expandedExprList) {
              list.add(builder.registerInput(expr));
            }
            if (program.getCondition() != null) {
              final int conditionIndex =
                  program.getCondition().getIndex();
              final RexNode newConditionExp =
                  expandedExprList.get(conditionIndex);
              if (newConditionExp.isAlwaysTrue()) {
                // condition is always TRUE - drop it
              } else if (newConditionExp instanceof RexLiteral
                  || RexUtil.isNullLiteral(newConditionExp, true)) {
                // condition is always NULL or FALSE - replace calc
                // with empty
                call.transformTo(
                    new EmptyRel(
                        calc.getCluster(),
                        calc.getRowType()));
                return;
              } else {
                builder.addCondition(list.get(conditionIndex));
              }
            }
            int k = 0;
            for (RexLocalRef projectExpr : program.getProjectList()) {
              final int index = projectExpr.getIndex();
              builder.addProject(
                  list.get(index).getIndex(),
                  program.getOutputRowType().getFieldNames().get(k++));
            }
            call.transformTo(
                new CalcRel(
                    calc.getCluster(),
                    calc.getTraitSet(),
                    calc.getChild(),
                    calc.getRowType(),
                    builder.getProgram(),
                    calc.getCollationList()));

            // New plan is absolutely better than old plan.
            call.getPlanner().setImportance(calc, 0.0);
          }
        }
      };

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a ReduceExpressionsRule.
   *
   * @param clazz class of rels to which this rule should apply
   */
  private ReduceExpressionsRule(Class<? extends RelNode> clazz, String desc) {
    super(operand(clazz, any()), desc);
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Reduces a list of expressions.
   *
   * @param rel     Relational expression
   * @param expList List of expressions, modified in place
   * @return whether reduction found something to change, and succeeded
   */
  static boolean reduceExpressions(RelNode rel, List<RexNode> expList) {
    RexBuilder rexBuilder = rel.getCluster().getRexBuilder();

    // Find reducible expressions.
    List<RexNode> constExps = new ArrayList<RexNode>();
    List<Boolean> addCasts = new ArrayList<Boolean>();
    List<RexNode> removableCasts = new ArrayList<RexNode>();
    findReducibleExps(rel.getCluster().getTypeFactory(), expList,
        constExps, addCasts, removableCasts);
    if (constExps.isEmpty() && removableCasts.isEmpty()) {
      return false;
    }

    // Remove redundant casts before reducing constant expressions.
    // If the argument to the redundant cast is a reducible constant,
    // reducing that argument to a constant first will result in not being
    // able to locate the original cast expression.
    if (!removableCasts.isEmpty()) {
      List<RexNode> reducedExprs = new ArrayList<RexNode>();
      List<Boolean> noCasts = new ArrayList<Boolean>();
      for (RexNode exp : removableCasts) {
        RexCall call = (RexCall) exp;
        reducedExprs.add(call.getOperands().get(0));
        noCasts.add(false);
      }
      RexReplacer replacer =
          new RexReplacer(
              rexBuilder,
              removableCasts,
              reducedExprs,
              noCasts);
      replacer.mutate(expList);
    }

    if (constExps.isEmpty()) {
      return true;
    }

    // Compute the values they reduce to.
    RelOptPlanner.Executor executor =
        rel.getCluster().getPlanner().getExecutor();
    List<RexNode> reducedValues = new ArrayList<RexNode>();
    executor.reduce(rexBuilder, constExps, reducedValues);

    // For ProjectRel, we have to be sure to preserve the result
    // types, so always cast regardless of the expression type.
    // For other RelNodes like FilterRel, in general, this isn't necessary,
    // and the presence of casts could hinder other rules such as sarg
    // analysis, which require bare literals.  But there are special cases,
    // like when the expression is a UDR argument, that need to be
    // handled as special cases.
    if (rel instanceof ProjectRel) {
      for (int i = 0; i < reducedValues.size(); i++) {
        addCasts.set(i, true);
      }
    }

    RexReplacer replacer =
        new RexReplacer(
            rexBuilder,
            constExps,
            reducedValues,
            addCasts);
    replacer.mutate(expList);
    return true;
  }

  /**
   * Locates expressions that can be reduced to literals or converted to
   * expressions with redundant casts removed.
   *
   * @param typeFactory    Type factory
   * @param exps           list of candidate expressions to be examined for
   *                       reduction
   * @param constExps      returns the list of expressions that can be constant
   *                       reduced
   * @param addCasts       indicator for each expression that can be constant
   *                       reduced, whether a cast of the resulting reduced
   *                       expression is potentially necessary
   * @param removableCasts returns the list of cast expressions where the cast
   */
  private static void findReducibleExps(
      RelDataTypeFactory typeFactory,
      List<RexNode> exps,
      List<RexNode> constExps,
      List<Boolean> addCasts,
      List<RexNode> removableCasts) {
    ReducibleExprLocator gardener =
        new ReducibleExprLocator(typeFactory, constExps, addCasts,
            removableCasts);
    for (RexNode exp : exps) {
      gardener.analyze(exp);
    }
    assert constExps.size() == addCasts.size();
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Replaces expressions with their reductions. Note that we only have to
   * look for RexCall, since nothing else is reducible in the first place.
   */
  private static class RexReplacer extends RexShuttle {
    private final RexBuilder rexBuilder;
    private final List<RexNode> reducibleExps;
    private final List<RexNode> reducedValues;
    private final List<Boolean> addCasts;

    RexReplacer(
        RexBuilder rexBuilder,
        List<RexNode> reducibleExps,
        List<RexNode> reducedValues,
        List<Boolean> addCasts) {
      this.rexBuilder = rexBuilder;
      this.reducibleExps = reducibleExps;
      this.reducedValues = reducedValues;
      this.addCasts = addCasts;
    }

    @Override
    public RexNode visitCall(final RexCall call) {
      int i = reducibleExps.indexOf(call);
      if (i == -1) {
        return super.visitCall(call);
      }
      RexNode replacement = reducedValues.get(i);
      if (addCasts.get(i)
          && (replacement.getType() != call.getType())) {
        // Handle change from nullable to NOT NULL by claiming
        // that the result is still nullable, even though
        // we know it isn't.
        //
        // Also, we cannot reduce CAST('abc' AS VARCHAR(4)) to 'abc'.
        // If we make 'abc' of type VARCHAR(4), we may later encounter
        // the same expression in a ProjectRel's digest where it has
        // type VARCHAR(3), and that's wrong.
        replacement =
            rexBuilder.makeCast(
                call.getType(),
                replacement);
      }
      return replacement;
    }
  }

  /**
   * Helper class used to locate expressions that either can be reduced to
   * literals or contain redundant casts.
   */
  private static class ReducibleExprLocator extends RexVisitorImpl<Void> {
    /** Whether an expression is constant, and if so, whether it can be
     * reduced to a simpler constant. */
    enum Constancy {
      NON_CONSTANT, REDUCIBLE_CONSTANT, IRREDUCIBLE_CONSTANT
    }

    private final RelDataTypeFactory typeFactory;

    private final List<Constancy> stack;

    private final List<RexNode> constExprs;

    private final List<Boolean> addCasts;

    private final List<RexNode> removableCasts;

    private final List<SqlOperator> parentCallTypeStack;

    ReducibleExprLocator(RelDataTypeFactory typeFactory,
        List<RexNode> constExprs, List<Boolean> addCasts,
        List<RexNode> removableCasts) {
      // go deep
      super(true);
      this.typeFactory = typeFactory;
      this.constExprs = constExprs;
      this.addCasts = addCasts;
      this.removableCasts = removableCasts;
      this.stack = new ArrayList<Constancy>();
      this.parentCallTypeStack = new ArrayList<SqlOperator>();
    }

    public void analyze(RexNode exp) {
      assert stack.isEmpty();

      exp.accept(this);

      // Deal with top of stack
      assert stack.size() == 1;
      assert parentCallTypeStack.isEmpty();
      Constancy rootConstancy = stack.get(0);
      if (rootConstancy == Constancy.REDUCIBLE_CONSTANT) {
        // The entire subtree was constant, so add it to the result.
        addResult(exp);
      }
      stack.clear();
    }

    private Void pushVariable() {
      stack.add(Constancy.NON_CONSTANT);
      return null;
    }

    private void addResult(RexNode exp) {
      // Cast of literal can't be reduced, so skip those (otherwise we'd
      // go into an infinite loop as we add them back).
      if (exp.getKind() == SqlKind.CAST) {
        RexCall cast = (RexCall) exp;
        RexNode operand = cast.getOperands().get(0);
        if (operand instanceof RexLiteral) {
          return;
        }
      }
      constExprs.add(exp);

      // In the case where the expression corresponds to a UDR argument,
      // we need to preserve casts.  Note that this only applies to
      // the topmost argument, not expressions nested within the UDR
      // call.
      //
      // REVIEW zfong 6/13/08 - Are there other expressions where we
      // also need to preserve casts?
      if (parentCallTypeStack.isEmpty()) {
        addCasts.add(false);
      } else {
        addCasts.add(isUdf(Stacks.peek(parentCallTypeStack)));
      }
    }

    private Boolean isUdf(SqlOperator operator) {
      // return operator instanceof UserDefinedRoutine
      return false;
    }

    public Void visitInputRef(RexInputRef inputRef) {
      return pushVariable();
    }

    public Void visitLiteral(RexLiteral literal) {
      stack.add(Constancy.IRREDUCIBLE_CONSTANT);
      return null;
    }

    public Void visitOver(RexOver over) {
      // assume non-constant (running SUM(1) looks constant but isn't)
      analyzeCall(over, Constancy.NON_CONSTANT);
      return null;
    }

    public Void visitCorrelVariable(RexCorrelVariable correlVariable) {
      return pushVariable();
    }

    public Void visitCall(RexCall call) {
      // assume REDUCIBLE_CONSTANT until proven otherwise
      analyzeCall(call, Constancy.REDUCIBLE_CONSTANT);
      return null;
    }

    private void analyzeCall(RexCall call, Constancy callConstancy) {
      Stacks.push(parentCallTypeStack, call.getOperator());

      // visit operands, pushing their states onto stack
      super.visitCall(call);

      // look for NON_CONSTANT operands
      int operandCount = call.getOperands().size();
      List<Constancy> operandStack = Util.last(stack, operandCount);
      for (Constancy operandConstancy : operandStack) {
        if (operandConstancy == Constancy.NON_CONSTANT) {
          callConstancy = Constancy.NON_CONSTANT;
        }
      }

      // Even if all operands are constant, the call itself may
      // be non-deterministic.
      if (!call.getOperator().isDeterministic()) {
        callConstancy = Constancy.NON_CONSTANT;
      } else if (call.getOperator().isDynamicFunction()) {
        // We can reduce the call to a constant, but we can't
        // cache the plan if the function is dynamic.
        // For now, treat it same as non-deterministic.
        callConstancy = Constancy.NON_CONSTANT;
      }

      // Row operator itself can't be reduced to a literal, but if
      // the operands are constants, we still want to reduce those
      if ((callConstancy == Constancy.REDUCIBLE_CONSTANT)
          && (call.getOperator() instanceof SqlRowOperator)) {
        callConstancy = Constancy.NON_CONSTANT;
      }

      if (callConstancy == Constancy.NON_CONSTANT) {
        // any REDUCIBLE_CONSTANT children are now known to be maximal
        // reducible subtrees, so they can be added to the result
        // list
        for (int iOperand = 0; iOperand < operandCount; ++iOperand) {
          Constancy constancy = operandStack.get(iOperand);
          if (constancy == Constancy.REDUCIBLE_CONSTANT) {
            addResult(call.getOperands().get(iOperand));
          }
        }

        // if this cast expression can't be reduced to a literal,
        // then see if we can remove the cast
        if (call.getOperator() == SqlStdOperatorTable.CAST) {
          reduceCasts(call);
        }
      }

      // pop operands off of the stack
      operandStack.clear();

      // pop this parent call operator off the stack
      Stacks.pop(parentCallTypeStack, call.getOperator());

      // push constancy result for this call onto stack
      stack.add(callConstancy);
    }

    private void reduceCasts(RexCall outerCast) {
      List<RexNode> operands = outerCast.getOperands();
      if (operands.size() != 1) {
        return;
      }
      RelDataType outerCastType = outerCast.getType();
      RelDataType operandType = operands.get(0).getType();
      if (operandType.equals(outerCastType)) {
        removableCasts.add(outerCast);
        return;
      }

      // See if the reduction
      // CAST((CAST x AS type) AS type NOT NULL)
      // -> CAST(x AS type NOT NULL)
      // applies.  TODO jvs 15-Dec-2008:  consider
      // similar cases for precision changes.
      if (!(operands.get(0) instanceof RexCall)) {
        return;
      }
      RexCall innerCast = (RexCall) operands.get(0);
      if (innerCast.getOperator() != SqlStdOperatorTable.CAST) {
        return;
      }
      if (innerCast.getOperands().size() != 1) {
        return;
      }
      RelDataType outerTypeNullable =
          typeFactory.createTypeWithNullability(outerCastType, true);
      RelDataType innerTypeNullable =
          typeFactory.createTypeWithNullability(operandType, true);
      if (outerTypeNullable != innerTypeNullable) {
        return;
      }
      if (operandType.isNullable()) {
        removableCasts.add(innerCast);
      }
    }

    public Void visitDynamicParam(RexDynamicParam dynamicParam) {
      return pushVariable();
    }

    public Void visitRangeRef(RexRangeRef rangeRef) {
      return pushVariable();
    }

    public Void visitFieldAccess(RexFieldAccess fieldAccess) {
      return pushVariable();
    }
  }

}

// End ReduceExpressionsRule.java
