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

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Predicate1;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.Strong;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.PredicateImpl;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.ControlFlowException;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Utility methods concerning row-expressions.
 */
public class RexUtil {

  private static final Function<? super RexNode, ? extends RexNode> ADD_NOT =
      new Function<RexNode, RexNode>() {
        public RexNode apply(RexNode input) {
          return new RexCall(input.getType(), SqlStdOperatorTable.NOT,
              ImmutableList.of(input));
        }
      };

  private static final Predicate1<RexNode> IS_FLAT_PREDICATE =
      new Predicate1<RexNode>() {
        public boolean apply(RexNode v1) {
          return isFlat(v1);
        }
      };

  private static final Function<Object, String> TO_STRING =
      new Function<Object, String>() {
        public String apply(Object input) {
          return input.toString();
        }
      };

  private static final Function<RexNode, RelDataType> TYPE_FN =
      new Function<RexNode, RelDataType>() {
        public RelDataType apply(RexNode input) {
          return input.getType();
        }
      };

  private static final Function<RelDataType, RelDataTypeFamily> FAMILY_FN =
      new Function<RelDataType, RelDataTypeFamily>() {
        public RelDataTypeFamily apply(RelDataType input) {
          return input.getFamily();
        }
      };

  /** Executor for a bit of constant reduction. The user can pass in another executor. */
  public static final RexExecutor EXECUTOR =
      new RexExecutorImpl(Schemas.createDataContext(null));

  private RexUtil() {
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Returns a guess for the selectivity of an expression.
   *
   * @param exp expression of interest, or null for none (implying a
   *            selectivity of 1.0)
   * @return guessed selectivity
   */
  public static double getSelectivity(RexNode exp) {
    if ((exp == null) || exp.isAlwaysTrue()) {
      return 1d;
    }
    return 0.1d;
  }

  /**
   * Generates a cast from one row type to another
   *
   * @param rexBuilder RexBuilder to use for constructing casts
   * @param lhsRowType target row type
   * @param rhsRowType source row type; fields must be 1-to-1 with lhsRowType,
   *                   in same order
   * @return cast expressions
   */
  public static List<RexNode> generateCastExpressions(
      RexBuilder rexBuilder,
      RelDataType lhsRowType,
      RelDataType rhsRowType) {
    final List<RelDataTypeField> fieldList = rhsRowType.getFieldList();
    int n = fieldList.size();
    assert n == lhsRowType.getFieldCount()
        : "field count: lhs [" + lhsRowType + "] rhs [" + rhsRowType + "]";
    List<RexNode> rhsExps = new ArrayList<>();
    for (RelDataTypeField field : fieldList) {
      rhsExps.add(
          rexBuilder.makeInputRef(field.getType(), field.getIndex()));
    }
    return generateCastExpressions(rexBuilder, lhsRowType, rhsExps);
  }

  /**
   * Generates a cast for a row type.
   *
   * @param rexBuilder RexBuilder to use for constructing casts
   * @param lhsRowType target row type
   * @param rhsExps    expressions to be cast
   * @return cast expressions
   */
  public static List<RexNode> generateCastExpressions(
      RexBuilder rexBuilder,
      RelDataType lhsRowType,
      List<RexNode> rhsExps) {
    List<RelDataTypeField> lhsFields = lhsRowType.getFieldList();
    List<RexNode> castExps = new ArrayList<>();
    for (Pair<RelDataTypeField, RexNode> pair
        : Pair.zip(lhsFields, rhsExps, true)) {
      RelDataTypeField lhsField = pair.left;
      RelDataType lhsType = lhsField.getType();
      final RexNode rhsExp = pair.right;
      RelDataType rhsType = rhsExp.getType();
      if (lhsType.equals(rhsType)) {
        castExps.add(rhsExp);
      } else {
        castExps.add(rexBuilder.makeCast(lhsType, rhsExp));
      }
    }
    return castExps;
  }

  /**
   * Returns whether a node represents the NULL value.
   *
   * <p>Examples:
   *
   * <ul>
   * <li>For {@link org.apache.calcite.rex.RexLiteral} Unknown, returns false.
   * <li>For <code>CAST(NULL AS <i>type</i>)</code>, returns true if <code>
   * allowCast</code> is true, false otherwise.
   * <li>For <code>CAST(CAST(NULL AS <i>type</i>) AS <i>type</i>))</code>,
   * returns false.
   * </ul>
   */
  public static boolean isNullLiteral(
      RexNode node,
      boolean allowCast) {
    if (node instanceof RexLiteral) {
      RexLiteral literal = (RexLiteral) node;
      if (literal.getTypeName() == SqlTypeName.NULL) {
        assert null == literal.getValue();
        return true;
      } else {
        // We don't regard UNKNOWN -- SqlLiteral(null,Boolean) -- as
        // NULL.
        return false;
      }
    }
    if (allowCast) {
      if (node.isA(SqlKind.CAST)) {
        RexCall call = (RexCall) node;
        if (isNullLiteral(call.operands.get(0), false)) {
          // node is "CAST(NULL as type)"
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Returns whether a node represents the NULL value or a series of nested
   * {@code CAST(NULL AS type)} calls. For example:
   * <code>isNull(CAST(CAST(NULL as INTEGER) AS VARCHAR(1)))</code>
   * returns {@code true}.
   */
  public static boolean isNull(RexNode expr) {
    switch (expr.getKind()) {
    case LITERAL:
      return ((RexLiteral) expr).getValue2() == null;
    case CAST:
      return isNull(((RexCall) expr).operands.get(0));
    default:
      return false;
    }
  }

  /**
   * Returns whether a node represents a literal.
   *
   * <p>Examples:
   *
   * <ul>
   * <li>For <code>CAST(literal AS <i>type</i>)</code>, returns true if <code>
   * allowCast</code> is true, false otherwise.
   * <li>For <code>CAST(CAST(literal AS <i>type</i>) AS <i>type</i>))</code>,
   * returns false.
   * </ul>
   *
   * @param node The node, never null.
   * @param allowCast whether to regard CAST(literal) as a literal
   * @return Whether the node is a literal
   */
  public static boolean isLiteral(RexNode node, boolean allowCast) {
    assert node != null;
    if (node instanceof RexLiteral) {
      return true;
    }
    if (allowCast) {
      if (node.isA(SqlKind.CAST)) {
        RexCall call = (RexCall) node;
        if (isLiteral(call.operands.get(0), false)) {
          // node is "CAST(literal as type)"
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Returns whether a node represents an input reference or field access.
   *
   * @param node The node, never null.
   * @param allowCast whether to regard CAST(x) as true
   * @return Whether the node is a reference or access
   */
  public static boolean isReferenceOrAccess(RexNode node, boolean allowCast) {
    assert node != null;
    if (node instanceof RexInputRef || node instanceof RexFieldAccess) {
      return true;
    }
    if (allowCast) {
      if (node.isA(SqlKind.CAST)) {
        RexCall call = (RexCall) node;
        return isReferenceOrAccess(call.operands.get(0), false);
      }
    }
    return false;
  }

  /** Returns whether an expression is a cast just for the purposes of
   * nullability, not changing any other aspect of the type. */
  public static boolean isNullabilityCast(RelDataTypeFactory typeFactory,
      RexNode node) {
    switch (node.getKind()) {
    case CAST:
      final RexCall call = (RexCall) node;
      final RexNode arg0 = call.getOperands().get(0);
      return SqlTypeUtil.equalSansNullability(typeFactory, arg0.getType(),
          call.getType());
    }
    return false;
  }

  /** Creates a map containing each (e, constant) pair that occurs within
   * a predicate list.
   *
   * @param clazz Class of expression that is considered constant
   * @param rexBuilder Rex builder
   * @param predicates Predicate list
   * @param <C> what to consider a constant: {@link RexLiteral} to use a narrow
   *           definition of constant, or {@link RexNode} to use
   *           {@link RexUtil#isConstant(RexNode)}
   * @return Map from values to constants
   */
  public static <C extends RexNode> ImmutableMap<RexNode, C> predicateConstants(
      Class<C> clazz, RexBuilder rexBuilder, List<RexNode> predicates) {
    // We cannot use an ImmutableMap.Builder here. If there are multiple entries
    // with the same key (e.g. "WHERE deptno = 1 AND deptno = 2"), it doesn't
    // matter which we take, so the latter will replace the former.
    // The basic idea is to find all the pairs of RexNode = RexLiteral
    // (1) If 'predicates' contain a non-EQUALS, we bail out.
    // (2) It is OK if a RexNode is equal to the same RexLiteral several times,
    // (e.g. "WHERE deptno = 1 AND deptno = 1")
    // (3) It will return false if there are inconsistent constraints (e.g.
    // "WHERE deptno = 1 AND deptno = 2")
    final Map<RexNode, C> map = new HashMap<>();
    final Set<RexNode> excludeSet = new HashSet<>();
    for (RexNode predicate : predicates) {
      gatherConstraints(clazz, predicate, map, excludeSet, rexBuilder);
    }
    final ImmutableMap.Builder<RexNode, C> builder =
        ImmutableMap.builder();
    for (Map.Entry<RexNode, C> entry : map.entrySet()) {
      RexNode rexNode = entry.getKey();
      if (!overlap(rexNode, excludeSet)) {
        builder.put(rexNode, entry.getValue());
      }
    }
    return builder.build();
  }

  private static boolean overlap(RexNode rexNode, Set<RexNode> set) {
    if (rexNode instanceof RexCall) {
      for (RexNode r : ((RexCall) rexNode).getOperands()) {
        if (overlap(r, set)) {
          return true;
        }
      }
      return false;
    } else {
      return set.contains(rexNode);
    }
  }

  /** Tries to decompose the RexNode which is a RexCall into non-literal
   * RexNodes. */
  private static void decompose(Set<RexNode> set, RexNode rexNode) {
    if (rexNode instanceof RexCall) {
      for (RexNode r : ((RexCall) rexNode).getOperands()) {
        decompose(set, r);
      }
    } else if (!(rexNode instanceof RexLiteral)) {
      set.add(rexNode);
    }
  }

  private static <C extends RexNode> void gatherConstraints(Class<C> clazz,
      RexNode predicate, Map<RexNode, C> map, Set<RexNode> excludeSet,
      RexBuilder rexBuilder) {
    if (predicate.getKind() != SqlKind.EQUALS
            && predicate.getKind() != SqlKind.IS_NULL) {
      decompose(excludeSet, predicate);
      return;
    }
    final List<RexNode> operands = ((RexCall) predicate).getOperands();
    if (operands.size() != 2 && predicate.getKind() == SqlKind.EQUALS) {
      decompose(excludeSet, predicate);
      return;
    }
    // if it reaches here, we have rexNode equals rexNode
    final RexNode left;
    final RexNode right;
    if (predicate.getKind() == SqlKind.EQUALS) {
      left = operands.get(0);
      right = operands.get(1);
    } else {
      left = operands.get(0);
      right = rexBuilder.makeNullLiteral(left.getType());
    }
    // Note that literals are immutable too, and they can only be compared
    // through values.
    gatherConstraint(clazz, left, right, map, excludeSet, rexBuilder);
    gatherConstraint(clazz, right, left, map, excludeSet, rexBuilder);
  }

  private static <C extends RexNode> void gatherConstraint(Class<C> clazz,
      RexNode left, RexNode right, Map<RexNode, C> map, Set<RexNode> excludeSet,
      RexBuilder rexBuilder) {
    if (!clazz.isInstance(right)) {
      return;
    }
    if (!isConstant(right)) {
      return;
    }
    C constant = clazz.cast(right);
    if (excludeSet.contains(left)) {
      return;
    }
    final C existedValue = map.get(left);
    if (existedValue == null) {
      switch (left.getKind()) {
      case CAST:
        // Convert "CAST(c) = literal" to "c = literal", as long as it is a
        // widening cast.
        final RexNode operand = ((RexCall) left).getOperands().get(0);
        if (canAssignFrom(left.getType(), operand.getType())) {
          final RexNode castRight =
              rexBuilder.makeCast(operand.getType(), constant);
          if (castRight instanceof RexLiteral) {
            left = operand;
            constant = clazz.cast(castRight);
          }
        }
      }
      map.put(left, constant);
    } else {
      if (existedValue instanceof RexLiteral
          && constant instanceof RexLiteral
          && !((RexLiteral) existedValue).getValue()
              .equals(((RexLiteral) constant).getValue())) {
        // we found conflicting values, e.g. left = 10 and left = 20
        map.remove(left);
        excludeSet.add(left);
      }
    }
  }

  /** Returns whether a value of {@code type2} can be assigned to a variable
   * of {@code type1}.
   *
   * <p>For example:
   * <ul>
   *   <li>{@code canAssignFrom(BIGINT, TINYINT)} returns {@code true}</li>
   *   <li>{@code canAssignFrom(TINYINT, BIGINT)} returns {@code false}</li>
   *   <li>{@code canAssignFrom(BIGINT, VARCHAR)} returns {@code false}</li>
   * </ul>
   */
  private static boolean canAssignFrom(RelDataType type1, RelDataType type2) {
    final SqlTypeName name1 = type1.getSqlTypeName();
    final SqlTypeName name2 = type2.getSqlTypeName();
    if (name1.getFamily() == name2.getFamily()) {
      switch (name1.getFamily()) {
      case NUMERIC:
        return name1.compareTo(name2) >= 0;
      default:
        return true;
      }
    }
    return false;
  }

  /**
   * Walks over an expression and determines whether it is constant.
   */
  static class ConstantFinder implements RexVisitor<Boolean> {
    static final ConstantFinder INSTANCE = new ConstantFinder();

    public Boolean visitLiteral(RexLiteral literal) {
      return true;
    }

    public Boolean visitInputRef(RexInputRef inputRef) {
      return false;
    }

    public Boolean visitLocalRef(RexLocalRef localRef) {
      return false;
    }

    public Boolean visitOver(RexOver over) {
      return false;
    }

    public Boolean visitSubQuery(RexSubQuery subQuery) {
      return false;
    }

    public Boolean visitCorrelVariable(RexCorrelVariable correlVariable) {
      // Correlating variables change when there is an internal restart.
      // Not good enough for our purposes.
      return false;
    }

    public Boolean visitDynamicParam(RexDynamicParam dynamicParam) {
      // Dynamic parameters are constant WITHIN AN EXECUTION, so that's
      // good enough.
      return true;
    }

    public Boolean visitCall(RexCall call) {
      // Constant if operator is deterministic and all operands are
      // constant.
      return call.getOperator().isDeterministic()
          && RexVisitorImpl.visitArrayAnd(this, call.getOperands());
    }

    public Boolean visitRangeRef(RexRangeRef rangeRef) {
      return false;
    }

    public Boolean visitFieldAccess(RexFieldAccess fieldAccess) {
      // "<expr>.FIELD" is constant iff "<expr>" is constant.
      return fieldAccess.getReferenceExpr().accept(this);
    }
  }

  /**
   * Returns whether node is made up of constants.
   *
   * @param node Node to inspect
   * @return true if node is made up of constants, false otherwise
   */
  public static boolean isConstant(RexNode node) {
    return node.accept(ConstantFinder.INSTANCE);
  }

  /**
   * Returns whether a given expression is deterministic.
   *
   * @param e Expression
   * @return true if tree result is deterministic, false otherwise
   */
  public static boolean isDeterministic(RexNode e) {
    try {
      RexVisitor<Void> visitor =
          new RexVisitorImpl<Void>(true) {
            @Override public Void visitCall(RexCall call) {
              if (!call.getOperator().isDeterministic()) {
                throw Util.FoundOne.NULL;
              }
              return super.visitCall(call);
            }
          };
      e.accept(visitor);
      return true;
    } catch (Util.FoundOne ex) {
      Util.swallow(ex, null);
      return false;
    }
  }

   /**
   * Returns whether a given node contains a RexCall with a specified operator
   *
   * @param operator Operator to look for
   * @param node     a RexNode tree
   */
  public static RexCall findOperatorCall(
      final SqlOperator operator,
      RexNode node) {
    try {
      RexVisitor<Void> visitor =
          new RexVisitorImpl<Void>(true) {
            public Void visitCall(RexCall call) {
              if (call.getOperator().equals(operator)) {
                throw new Util.FoundOne(call);
              }
              return super.visitCall(call);
            }
          };
      node.accept(visitor);
      return null;
    } catch (Util.FoundOne e) {
      Util.swallow(e, null);
      return (RexCall) e.getNode();
    }
  }

  /**
   * Returns whether a given tree contains any {link RexInputRef} nodes.
   *
   * @param node a RexNode tree
   */
  public static boolean containsInputRef(
      RexNode node) {
    try {
      RexVisitor<Void> visitor =
          new RexVisitorImpl<Void>(true) {
            public Void visitInputRef(RexInputRef inputRef) {
              throw new Util.FoundOne(inputRef);
            }
          };
      node.accept(visitor);
      return false;
    } catch (Util.FoundOne e) {
      Util.swallow(e, null);
      return true;
    }
  }

  /**
   * Returns whether a given tree contains any
   * {@link org.apache.calcite.rex.RexFieldAccess} nodes.
   *
   * @param node a RexNode tree
   */
  public static boolean containsFieldAccess(RexNode node) {
    try {
      RexVisitor<Void> visitor =
          new RexVisitorImpl<Void>(true) {
            public Void visitFieldAccess(RexFieldAccess fieldAccess) {
              throw new Util.FoundOne(fieldAccess);
            }
          };
      node.accept(visitor);
      return false;
    } catch (Util.FoundOne e) {
      Util.swallow(e, null);
      return true;
    }
  }

  /**
   * Determines whether a {@link RexCall} requires decimal expansion. It
   * usually requires expansion if it has decimal operands.
   *
   * <p>Exceptions to this rule are:
   *
   * <ul>
   * <li>isNull doesn't require expansion
   * <li>It's okay to cast decimals to and from char types
   * <li>It's okay to cast nulls as decimals
   * <li>Casts require expansion if their return type is decimal
   * <li>Reinterpret casts can handle a decimal operand
   * </ul>
   *
   * @param expr    expression possibly in need of expansion
   * @param recurse whether to check nested calls
   * @return whether the expression requires expansion
   */
  public static boolean requiresDecimalExpansion(
      RexNode expr,
      boolean recurse) {
    if (!(expr instanceof RexCall)) {
      return false;
    }
    RexCall call = (RexCall) expr;

    boolean localCheck = true;
    switch (call.getKind()) {
    case REINTERPRET:
    case IS_NULL:
      localCheck = false;
      break;
    case CAST:
      RelDataType lhsType = call.getType();
      RelDataType rhsType = call.operands.get(0).getType();
      if (rhsType.getSqlTypeName() == SqlTypeName.NULL) {
        return false;
      }
      if (SqlTypeUtil.inCharFamily(lhsType)
          || SqlTypeUtil.inCharFamily(rhsType)) {
        localCheck = false;
      } else if (SqlTypeUtil.isDecimal(lhsType)
          && (lhsType != rhsType)) {
        return true;
      }
      break;
    default:
      localCheck = call.getOperator().requiresDecimalExpansion();
    }

    if (localCheck) {
      if (SqlTypeUtil.isDecimal(call.getType())) {
        // NOTE jvs 27-Mar-2007: Depending on the type factory, the
        // result of a division may be decimal, even though both inputs
        // are integer.
        return true;
      }
      for (int i = 0; i < call.operands.size(); i++) {
        if (SqlTypeUtil.isDecimal(call.operands.get(i).getType())) {
          return true;
        }
      }
    }
    return recurse && requiresDecimalExpansion(call.operands, true);
  }

  /**
   * Determines whether any operand of a set requires decimal expansion
   */
  public static boolean requiresDecimalExpansion(
      List<RexNode> operands,
      boolean recurse) {
    for (RexNode operand : operands) {
      if (operand instanceof RexCall) {
        RexCall call = (RexCall) operand;
        if (requiresDecimalExpansion(call, recurse)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Returns whether a {@link RexProgram} contains expressions which require
   * decimal expansion.
   */
  public static boolean requiresDecimalExpansion(
      RexProgram program,
      boolean recurse) {
    final List<RexNode> exprList = program.getExprList();
    for (RexNode expr : exprList) {
      if (requiresDecimalExpansion(expr, recurse)) {
        return true;
      }
    }
    return false;
  }

  public static boolean canReinterpretOverflow(RexCall call) {
    assert call.isA(SqlKind.REINTERPRET) : "call is not a reinterpret";
    return call.operands.size() > 1;
  }

  /**
   * Returns whether an array of expressions has any common sub-expressions.
   */
  public static boolean containNoCommonExprs(List<RexNode> exprs,
      Litmus litmus) {
    final ExpressionNormalizer visitor = new ExpressionNormalizer(false);
    for (RexNode expr : exprs) {
      try {
        expr.accept(visitor);
      } catch (ExpressionNormalizer.SubExprExistsException e) {
        Util.swallow(e, null);
        return litmus.fail(null);
      }
    }
    return litmus.succeed();
  }

  /**
   * Returns whether an array of expressions contains no forward references.
   * That is, if expression #i contains a {@link RexInputRef} referencing
   * field i or greater.
   *
   * @param exprs        Array of expressions
   * @param inputRowType Input row type
   * @param litmus       What to do if an error is detected (there is a
   *                     forward reference)
   *
   * @return Whether there is a forward reference
   */
  public static boolean containNoForwardRefs(List<RexNode> exprs,
      RelDataType inputRowType,
      Litmus litmus) {
    final ForwardRefFinder visitor = new ForwardRefFinder(inputRowType);
    for (int i = 0; i < exprs.size(); i++) {
      RexNode expr = exprs.get(i);
      visitor.setLimit(i); // field cannot refer to self or later field
      try {
        expr.accept(visitor);
      } catch (ForwardRefFinder.IllegalForwardRefException e) {
        Util.swallow(e, null);
        return litmus.fail("illegal forward reference in {}", expr);
      }
    }
    return litmus.succeed();
  }

  /**
   * Returns whether an array of exp contains no aggregate function calls whose
   * arguments are not {@link RexInputRef}s.
   *
   * @param exprs Expressions
   * @param litmus  Whether to assert if there is such a function call
   */
  static boolean containNoNonTrivialAggs(List<RexNode> exprs, Litmus litmus) {
    for (RexNode expr : exprs) {
      if (expr instanceof RexCall) {
        RexCall rexCall = (RexCall) expr;
        if (rexCall.getOperator() instanceof SqlAggFunction) {
          for (RexNode operand : rexCall.operands) {
            if (!(operand instanceof RexLocalRef)
                && !(operand instanceof RexLiteral)) {
              return litmus.fail("contains non trivial agg: {}", operand);
            }
          }
        }
      }
    }
    return litmus.succeed();
  }

  /**
   * Returns whether a list of expressions contains complex expressions, that
   * is, a call whose arguments are not {@link RexVariable} (or a subtype such
   * as {@link RexInputRef}) or {@link RexLiteral}.
   */
  public static boolean containComplexExprs(List<RexNode> exprs) {
    for (RexNode expr : exprs) {
      if (expr instanceof RexCall) {
        for (RexNode operand : ((RexCall) expr).operands) {
          if (!isAtomic(operand)) {
            return true;
          }
        }
      }
    }
    return false;
  }

  public static boolean isAtomic(RexNode expr) {
    return (expr instanceof RexLiteral) || (expr instanceof RexVariable);
  }

  /**
   * Returns whether a {@link RexNode node} is a {@link RexCall call} to a
   * given {@link SqlOperator operator}.
   */
  public static boolean isCallTo(RexNode expr, SqlOperator op) {
    return (expr instanceof RexCall)
        && (((RexCall) expr).getOperator() == op);
  }

  /**
   * Creates a record type with anonymous field names.
   *
   * @param typeFactory Type factory
   * @param exprs       Expressions
   * @return Record type
   */
  public static RelDataType createStructType(
      RelDataTypeFactory typeFactory,
      final List<RexNode> exprs) {
    return createStructType(typeFactory, exprs, null, null);
  }

  /**
   * Creates a record type with specified field names.
   *
   * <p>The array of field names may be null, or any of the names within it
   * can be null. We recommend using explicit names where possible, because it
   * makes it much easier to figure out the intent of fields when looking at
   * planner output.
   *
   * @param typeFactory Type factory
   * @param exprs       Expressions
   * @param names       Field names, may be null, or elements may be null
   * @param suggester   Generates alternative names if {@code names} is not
   *                    null and its elements are not unique
   * @return Record type
   */
  public static RelDataType createStructType(
      RelDataTypeFactory typeFactory,
      final List<? extends RexNode> exprs,
      List<String> names,
      SqlValidatorUtil.Suggester suggester) {
    if (names != null && suggester != null) {
      names = SqlValidatorUtil.uniquify(names, suggester,
          typeFactory.getTypeSystem().isSchemaCaseSensitive());
    }
    final RelDataTypeFactory.FieldInfoBuilder builder =
        typeFactory.builder();
    for (int i = 0; i < exprs.size(); i++) {
      String name;
      if (names == null || (name = names.get(i)) == null) {
        name = "$f" + i;
      }
      builder.add(name, exprs.get(i).getType());
    }
    return builder.build();
  }

  @Deprecated // to be removed before 2.0
  public static RelDataType createStructType(
      RelDataTypeFactory typeFactory,
      final List<? extends RexNode> exprs,
      List<String> names) {
    return createStructType(typeFactory, exprs, names, null);
  }

  /**
   * Returns whether the type of an array of expressions is compatible with a
   * struct type.
   *
   * @param exprs Array of expressions
   * @param type  Type
   * @param litmus What to do if an error is detected (there is a mismatch)
   *
   * @return Whether every expression has the same type as the corresponding
   * member of the struct type
   *
   * @see RelOptUtil#eq(String, RelDataType, String, RelDataType, org.apache.calcite.util.Litmus)
   */
  public static boolean compatibleTypes(
      List<RexNode> exprs,
      RelDataType type,
      Litmus litmus) {
    final List<RelDataTypeField> fields = type.getFieldList();
    if (exprs.size() != fields.size()) {
      return litmus.fail("rowtype mismatches expressions");
    }
    for (int i = 0; i < fields.size(); i++) {
      final RelDataType exprType = exprs.get(i).getType();
      final RelDataType fieldType = fields.get(i).getType();
      if (!RelOptUtil.eq("type1", exprType, "type2", fieldType, litmus)) {
        return litmus.fail(null);
      }
    }
    return litmus.succeed();
  }

  /**
   * Creates a key for {@link RexNode} which is the same as another key of
   * another RexNode only if the two have both the same type and textual
   * representation. For example, "10" integer and "10" bigint result in
   * different keys.
   */
  public static Pair<String, String> makeKey(RexNode expr) {
    return Pair.of(expr.toString(), expr.getType().getFullTypeString());
  }

  /**
   * Returns whether the leading edge of a given array of expressions is
   * wholly {@link RexInputRef} objects with types corresponding to the
   * underlying datatype.
   */
  public static boolean containIdentity(
      List<? extends RexNode> exprs,
      RelDataType rowType,
      Litmus litmus) {
    final List<RelDataTypeField> fields = rowType.getFieldList();
    if (exprs.size() < fields.size()) {
      return litmus.fail("exprs/rowType length mismatch");
    }
    for (int i = 0; i < fields.size(); i++) {
      if (!(exprs.get(i) instanceof RexInputRef)) {
        return litmus.fail("expr[{}] is not a RexInputRef", i);
      }
      RexInputRef inputRef = (RexInputRef) exprs.get(i);
      if (inputRef.getIndex() != i) {
        return litmus.fail("expr[{}] has ordinal {}", i, inputRef.getIndex());
      }
      if (!RelOptUtil.eq("type1",
          exprs.get(i).getType(),
          "type2",
          fields.get(i).getType(), litmus)) {
        return litmus.fail(null);
      }
    }
    return litmus.succeed();
  }

  /** Returns whether a list of expressions projects the incoming fields. */
  public static boolean isIdentity(List<? extends RexNode> exps,
      RelDataType inputRowType) {
    return inputRowType.getFieldCount() == exps.size()
        && containIdentity(exps, inputRowType, Litmus.IGNORE);
  }

  /**
   * Converts a collection of expressions into an AND.
   * If there are zero expressions, returns TRUE.
   * If there is one expression, returns just that expression.
   * Removes expressions that always evaluate to TRUE.
   * Returns null only if {@code nullOnEmpty} and expression is TRUE.
   */
  public static RexNode composeConjunction(RexBuilder rexBuilder,
      Iterable<? extends RexNode> nodes, boolean nullOnEmpty) {
    ImmutableList<RexNode> list = flattenAnd(nodes);
    switch (list.size()) {
    case 0:
      return nullOnEmpty
          ? null
          : rexBuilder.makeLiteral(true);
    case 1:
      return list.get(0);
    default:
      return rexBuilder.makeCall(SqlStdOperatorTable.AND, list);
    }
  }

  /** Flattens a list of AND nodes.
   *
   * <p>Treats null nodes as literal TRUE (i.e. ignores them). */
  public static ImmutableList<RexNode> flattenAnd(
      Iterable<? extends RexNode> nodes) {
    if (nodes instanceof Collection && ((Collection) nodes).isEmpty()) {
      // Optimize common case
      return ImmutableList.of();
    }
    final ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
    final Set<String> digests = Sets.newHashSet(); // to eliminate duplicates
    for (RexNode node : nodes) {
      if (node != null) {
        addAnd(builder, digests, node);
      }
    }
    return builder.build();
  }

  private static void addAnd(ImmutableList.Builder<RexNode> builder,
      Set<String> digests, RexNode node) {
    switch (node.getKind()) {
    case AND:
      for (RexNode operand : ((RexCall) node).getOperands()) {
        addAnd(builder, digests, operand);
      }
      return;
    default:
      if (!node.isAlwaysTrue() && digests.add(node.toString())) {
        builder.add(node);
      }
    }
  }

  /**
   * Converts a collection of expressions into an OR.
   * If there are zero expressions, returns FALSE.
   * If there is one expression, returns just that expression.
   * Removes expressions that always evaluate to FALSE.
   * Flattens expressions that are ORs.
   */
  public static RexNode composeDisjunction(RexBuilder rexBuilder,
      Iterable<? extends RexNode> nodes, boolean nullOnEmpty) {
    ImmutableList<RexNode> list = flattenOr(nodes);
    switch (list.size()) {
    case 0:
      return nullOnEmpty
          ? null
          : rexBuilder.makeLiteral(false);
    case 1:
      return list.get(0);
    default:
      return rexBuilder.makeCall(SqlStdOperatorTable.OR, list);
    }
  }

  /** Flattens a list of OR nodes. */
  public static ImmutableList<RexNode> flattenOr(
      Iterable<? extends RexNode> nodes) {
    if (nodes instanceof Collection && ((Collection) nodes).isEmpty()) {
      // Optimize common case
      return ImmutableList.of();
    }
    final ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
    final Set<String> digests = Sets.newHashSet(); // to eliminate duplicates
    for (RexNode node : nodes) {
      addOr(builder, digests, node);
    }
    return builder.build();
  }

  private static void addOr(ImmutableList.Builder<RexNode> builder,
      Set<String> digests, RexNode node) {
    switch (node.getKind()) {
    case OR:
      for (RexNode operand : ((RexCall) node).getOperands()) {
        addOr(builder, digests, operand);
      }
      return;
    default:
      if (!node.isAlwaysFalse() && digests.add(node.toString())) {
        builder.add(node);
      }
    }
  }

  /**
   * Applies a mapping to a collation list.
   *
   * @param mapping       Mapping
   * @param collationList Collation list
   * @return collation list with mapping applied to each field
   */
  public static List<RelCollation> apply(
      Mappings.TargetMapping mapping,
      List<RelCollation> collationList) {
    final List<RelCollation> newCollationList = new ArrayList<>();
    for (RelCollation collation : collationList) {
      final List<RelFieldCollation> newFieldCollationList = new ArrayList<>();
      for (RelFieldCollation fieldCollation
          : collation.getFieldCollations()) {
        final RelFieldCollation newFieldCollation =
            apply(mapping, fieldCollation);
        if (newFieldCollation == null) {
          // This field is not mapped. Stop here. The leading edge
          // of the collation is still valid (although it's useless
          // if it's empty).
          break;
        }
        newFieldCollationList.add(newFieldCollation);
      }
      // Truncation to collations to their leading edge creates empty
      // and duplicate collations. Ignore these.
      if (!newFieldCollationList.isEmpty()) {
        final RelCollation newCollation =
            RelCollations.of(newFieldCollationList);
        if (!newCollationList.contains(newCollation)) {
          newCollationList.add(newCollation);
        }
      }
    }

    // REVIEW: There might be redundant collations in the list. For example,
    // in {(x), (x, y)}, (x) is redundant because it is a leading edge of
    // another collation in the list. Could remove redundant collations.

    return newCollationList;
  }

  /**
   * Applies a mapping to a collation.
   *
   * @param mapping   Mapping
   * @param collation Collation
   * @return collation with mapping applied
   */
  public static RelCollation apply(
      Mappings.TargetMapping mapping,
      RelCollation collation) {
    List<RelFieldCollation> fieldCollations =
        applyFields(mapping, collation.getFieldCollations());
    return fieldCollations.equals(collation.getFieldCollations())
        ? collation
        : RelCollations.of(fieldCollations);
  }

  /**
   * Applies a mapping to a field collation.
   *
   * <p>If the field is not mapped, returns null.
   *
   * @param mapping        Mapping
   * @param fieldCollation Field collation
   * @return collation with mapping applied
   */
  public static RelFieldCollation apply(
      Mappings.TargetMapping mapping,
      RelFieldCollation fieldCollation) {
    final int target =
        mapping.getTargetOpt(fieldCollation.getFieldIndex());
    if (target < 0) {
      return null;
    }
    return fieldCollation.copy(target);
  }

  /**
   * Applies a mapping to a list of field collations.
   *
   * @param mapping         Mapping
   * @param fieldCollations Field collations
   * @return collations with mapping applied
   */
  public static List<RelFieldCollation> applyFields(
      Mappings.TargetMapping mapping,
      List<RelFieldCollation> fieldCollations) {
    final List<RelFieldCollation> newFieldCollations = new ArrayList<>();
    for (RelFieldCollation fieldCollation : fieldCollations) {
      newFieldCollations.add(apply(mapping, fieldCollation));
    }
    return newFieldCollations;
  }

  /**
   * Applies a mapping to an expression.
   */
  public static RexNode apply(Mappings.TargetMapping mapping, RexNode node) {
    return node.accept(RexPermuteInputsShuttle.of(mapping));
  }

  /**
   * Applies a mapping to an iterable over expressions.
   */
  public static Iterable<RexNode> apply(Mappings.TargetMapping mapping,
      Iterable<? extends RexNode> nodes) {
    final RexPermuteInputsShuttle shuttle = RexPermuteInputsShuttle.of(mapping);
    return Iterables.transform(
        nodes, new Function<RexNode, RexNode>() {
          public RexNode apply(RexNode input) {
            return input.accept(shuttle);
          }
        });
  }

  /**
   * Applies a shuttle to an array of expressions. Creates a copy first.
   *
   * @param shuttle Shuttle
   * @param exprs   Array of expressions
   */
  public static <T extends RexNode> T[] apply(
      RexVisitor<T> shuttle,
      T[] exprs) {
    T[] newExprs = exprs.clone();
    for (int i = 0; i < newExprs.length; i++) {
      final RexNode expr = newExprs[i];
      if (expr != null) {
        newExprs[i] = expr.accept(shuttle);
      }
    }
    return newExprs;
  }

  /**
   * Applies a visitor to an array of expressions and, if specified, a single
   * expression.
   *
   * @param visitor Visitor
   * @param exprs   Array of expressions
   * @param expr    Single expression, may be null
   */
  public static void apply(
      RexVisitor<Void> visitor,
      RexNode[] exprs,
      RexNode expr) {
    for (RexNode e : exprs) {
      e.accept(visitor);
    }
    if (expr != null) {
      expr.accept(visitor);
    }
  }

  /**
   * Applies a visitor to a list of expressions and, if specified, a single
   * expression.
   *
   * @param visitor Visitor
   * @param exprs   List of expressions
   * @param expr    Single expression, may be null
   */
  public static void apply(
      RexVisitor<Void> visitor,
      List<? extends RexNode> exprs,
      RexNode expr) {
    for (RexNode e : exprs) {
      e.accept(visitor);
    }
    if (expr != null) {
      expr.accept(visitor);
    }
  }

  /** Flattens an expression.
   *
   * <p>Returns the same expression if it is already flat. */
  public static RexNode flatten(RexBuilder rexBuilder, RexNode node) {
    if (node instanceof RexCall) {
      RexCall call = (RexCall) node;
      final SqlOperator op = call.getOperator();
      final List<RexNode> flattenedOperands = flatten(call.getOperands(), op);
      if (!isFlat(call.getOperands(), op)) {
        return rexBuilder.makeCall(call.getType(), op, flattenedOperands);
      }
    }
    return node;
  }

  /**
   * Converts a list of operands into a list that is flat with respect to
   * the given operator. The operands are assumed to be flat already.
   */
  public static List<RexNode> flatten(List<? extends RexNode> exprs,
      SqlOperator op) {
    if (isFlat(exprs, op)) {
      //noinspection unchecked
      return (List) exprs;
    }
    final List<RexNode> list = new ArrayList<>();
    flattenRecurse(list, exprs, op);
    return list;
  }

  /**
   * Returns whether a call to {@code op} with {@code exprs} as arguments
   * would be considered "flat".
   *
   * <p>For example, {@code isFlat([w, AND[x, y], z, AND)} returns false;
   * <p>{@code isFlat([w, x, y, z], AND)} returns true.</p>
   */
  private static boolean isFlat(
      List<? extends RexNode> exprs, final SqlOperator op) {
    return !isAssociative(op)
        || !exists(exprs,
            new Predicate1<RexNode>() {
              public boolean apply(RexNode expr) {
                return isCallTo(expr, op);
              }
            });
  }

  /**
   * Returns false if the expression can be optimized by flattening
   * calls to an associative operator such as AND and OR.
   */
  public static boolean isFlat(RexNode expr) {
    if (!(expr instanceof RexCall)) {
      return true;
    }
    final RexCall call = (RexCall) expr;
    return isFlat(call.getOperands(), call.getOperator())
        && all(call.getOperands(), IS_FLAT_PREDICATE);
  }

  private static void flattenRecurse(
      List<RexNode> list, List<? extends RexNode> exprs, SqlOperator op) {
    for (RexNode expr : exprs) {
      if (expr instanceof RexCall
          && ((RexCall) expr).getOperator() == op) {
        flattenRecurse(list, ((RexCall) expr).getOperands(), op);
      } else {
        list.add(expr);
      }
    }
  }

  /** Converts an expression to conjunctive normal form (CNF).
   *
   * <p>The following expression is in CNF:
   *
   * <blockquote>(a OR b) AND (c OR d)</blockquote>
   *
   * <p>The following expression is not in CNF:
   *
   * <blockquote>(a AND b) OR c</blockquote>
   *
   * but can be converted to CNF:
   *
   * <blockquote>(a OR c) AND (b OR c)</blockquote>
   *
   * <p>The following expression is not in CNF:
   *
   * <blockquote>NOT (a OR NOT b)</blockquote>
   *
   * but can be converted to CNF by applying de Morgan's theorem:
   *
   * <blockquote>NOT a AND b</blockquote>
   *
   * <p>Expressions not involving AND, OR or NOT at the top level are in CNF.
   */
  public static RexNode toCnf(RexBuilder rexBuilder, RexNode rex) {
    return new CnfHelper(rexBuilder, -1).toCnf(rex);
  }

  /**
   * Similar to {@link #toCnf(RexBuilder, RexNode)}; however, it lets you
   * specify a threshold in the number of nodes that can be created out of
   * the conversion.
   *
   * <p>If the number of resulting nodes exceeds that threshold,
   * stops conversion and returns the original expression.
   *
   * <p>If the threshold is negative it is ignored.
   *
   * <p>Leaf nodes in the expression do not count towards the threshold.
   */
  public static RexNode toCnf(RexBuilder rexBuilder, int maxCnfNodeCount,
      RexNode rex) {
    return new CnfHelper(rexBuilder, maxCnfNodeCount).toCnf(rex);
  }

  /** Converts an expression to disjunctive normal form (DNF).
   *
   * <p>DNF: It is a form of logical formula which is disjunction of conjunctive
   * clauses.
   *
   * <p>All logical formulas can be converted into DNF.
   *
   * <p>The following expression is in DNF:
   *
   * <blockquote>(a AND b) OR (c AND d)</blockquote>
   *
   * <p>The following expression is not in CNF:
   *
   * <blockquote>(a OR b) AND c</blockquote>
   *
   * but can be converted to DNF:
   *
   * <blockquote>(a AND c) OR (b AND c)</blockquote>
   *
   * <p>The following expression is not in CNF:
   *
   * <blockquote>NOT (a OR NOT b)</blockquote>
   *
   * but can be converted to DNF by applying de Morgan's theorem:
   *
   * <blockquote>NOT a AND b</blockquote>
   *
   * <p>Expressions not involving AND, OR or NOT at the top level are in DNF.
   */
  public static RexNode toDnf(RexBuilder rexBuilder, RexNode rex) {
    return new DnfHelper(rexBuilder).toDnf(rex);
  }

  /**
   * Returns whether an operator is associative. AND is associative,
   * which means that "(x AND y) and z" is equivalent to "x AND (y AND z)".
   * We might well flatten the tree, and write "AND(x, y, z)".
   */
  private static boolean isAssociative(SqlOperator op) {
    return op.getKind() == SqlKind.AND
        || op.getKind() == SqlKind.OR;
  }

  /**
   * Returns whether there is an element in {@code list} for which
   * {@code predicate} is true.
   */
  public static <E> boolean exists(
      List<? extends E> list, Predicate1<E> predicate) {
    for (E e : list) {
      if (predicate.apply(e)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns whether {@code predicate} is true for all elements of
   * {@code list}.
   */
  public static <E> boolean all(
      List<? extends E> list, Predicate1<E> predicate) {
    for (E e : list) {
      if (!predicate.apply(e)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Shifts every {@link RexInputRef} in an expression by {@code offset}.
   */
  public static RexNode shift(RexNode node, final int offset) {
    if (offset == 0) {
      return node;
    }
    return node.accept(new RexShiftShuttle(offset));
  }

  /**
   * Shifts every {@link RexInputRef} in an expression by {@code offset}.
   */
  public static Iterable<RexNode> shift(Iterable<RexNode> nodes, int offset) {
    return new RexShiftShuttle(offset).apply(nodes);
  }

  /**
   * Shifts every {@link RexInputRef} in an expression higher than {@code start}
   * by {@code offset}.
   */
  public static RexNode shift(RexNode node, final int start, final int offset) {
    return node.accept(
        new RexShuttle() {
          @Override public RexNode visitInputRef(RexInputRef input) {
            final int index = input.getIndex();
            if (index < start) {
              return input;
            }
            return new RexInputRef(index + offset, input.getType());
          }
        });
  }

  /** Creates an equivalent version of a node where common factors among ORs
   * are pulled up.
   *
   * <p>For example,
   *
   * <blockquote>(a AND b) OR (a AND c AND d)</blockquote>
   *
   * <p>becomes
   *
   * <blockquote>a AND (b OR (c AND d))</blockquote>
   *
   * <p>Note that this result is not in CNF
   * (see {@link #toCnf(RexBuilder, RexNode)}) because there is an AND inside an
   * OR.
   *
   * <p>This form is useful if, say, {@code a} contains columns from only the
   * left-hand side of a join, and can be pushed to the left input.
   *
   * @param rexBuilder Rex builder
   * @param node Expression to transform
   * @return Equivalent expression with common factors pulled up
   */
  public static RexNode pullFactors(RexBuilder rexBuilder, RexNode node) {
    return new CnfHelper(rexBuilder, -1).pull(node);
  }

  @Deprecated // to be removed before 2.0
  public static List<RexNode> fixUp(final RexBuilder rexBuilder,
      List<RexNode> nodes, final RelDataType rowType) {
    final List<RelDataType> typeList = RelOptUtil.getFieldTypeList(rowType);
    return fixUp(rexBuilder, nodes, typeList);
  }

  /** Fixes up the type of all {@link RexInputRef}s in an
   * expression to match differences in nullability.
   *
   * <p>Such differences in nullability occur when expressions are moved
   * through outer joins.
   *
   * <p>Throws if there any greater inconsistencies of type. */
  public static List<RexNode> fixUp(final RexBuilder rexBuilder,
      List<RexNode> nodes, final List<RelDataType> fieldTypes) {
    return new FixNullabilityShuttle(rexBuilder, fieldTypes).apply(nodes);
  }

  /** Transforms a list of expressions into a list of their types. */
  public static List<RelDataType> types(List<? extends RexNode> nodes) {
    return Lists.transform(nodes, TYPE_FN);
  }

  public static List<RelDataTypeFamily> families(List<RelDataType> types) {
    return Lists.transform(types, FAMILY_FN);
  }

  /** Removes all expressions from a list that are equivalent to a given
   * expression. Returns whether any were removed. */
  public static boolean removeAll(List<RexNode> targets, RexNode e) {
    int count = 0;
    Iterator<RexNode> iterator = targets.iterator();
    while (iterator.hasNext()) {
      RexNode next = iterator.next();
      if (eq(next, e)) {
        ++count;
        iterator.remove();
      }
    }
    return count > 0;
  }

  /** Returns whether two {@link RexNode}s are structurally equal.
   *
   * <p>This method considers structure, not semantics. 'x &lt; y' is not
   * equivalent to 'y &gt; x'.
   */
  public static boolean eq(RexNode e1, RexNode e2) {
    return e1 == e2 || e1.toString().equals(e2.toString());
  }

  /** Simplifies a boolean expression, always preserving its type and its
   * nullability.
   *
   * <p>This is useful if you are simplifying expressions in a
   * {@link Project}. */
  public static RexNode simplifyPreservingType(RexBuilder rexBuilder,
      RexNode e, RexExecutor executor) {
    final RexNode e2 = simplify(rexBuilder, e, false, executor);
    if (e2.getType() == e.getType()) {
      return e2;
    }
    final RexNode e3 = rexBuilder.makeCast(e.getType(), e2, true);
    if (e3.equals(e)) {
      return e;
    }
    return e3;
  }

  /**
   * Simplifies a boolean expression, leaving UNKNOWN values as UNKNOWN, and
   * using the default executor.
   */
  public static RexNode simplify(RexBuilder rexBuilder, RexNode e) {
    return simplify(rexBuilder, e, false, EXECUTOR);
  }

  /**
   * Simplifies a boolean expression,
   * using the default executor.
   */
  public static RexNode simplify(RexBuilder rexBuilder, RexNode e,
      boolean unknownAsFalse) {
    return simplify(rexBuilder, e, unknownAsFalse, EXECUTOR);
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
   *
   * <p>If the expression is a predicate in a WHERE clause, UNKNOWN values have
   * the same effect as FALSE. In situations like this, specify
   * {@code unknownAsFalse = true}, so and we can switch from 3-valued logic to
   * simpler 2-valued logic and make more optimizations.
   *
   * @param rexBuilder Rex builder
   * @param e Expression to simplify
   * @param unknownAsFalse Whether to convert UNKNOWN values to FALSE
   * @param executor Executor for constant reduction, not null
   */
  public static RexNode simplify(RexBuilder rexBuilder, RexNode e,
      boolean unknownAsFalse, RexExecutor executor) {
    Preconditions.checkNotNull(executor);
    switch (e.getKind()) {
    case AND:
      return simplifyAnd(rexBuilder, (RexCall) e, unknownAsFalse);
    case OR:
      return simplifyOr(rexBuilder, (RexCall) e);
    case NOT:
      return simplifyNot(rexBuilder, (RexCall) e);
    case CASE:
      return simplifyCase(rexBuilder, (RexCall) e, unknownAsFalse);
    case CAST:
      return simplifyCast(rexBuilder, (RexCall) e, executor);
    case IS_NULL:
    case IS_NOT_NULL:
    case IS_TRUE:
    case IS_NOT_TRUE:
    case IS_FALSE:
    case IS_NOT_FALSE:
      assert e instanceof RexCall;
      return simplifyIs(rexBuilder, (RexCall) e);
    case EQUALS:
    case GREATER_THAN:
    case GREATER_THAN_OR_EQUAL:
    case LESS_THAN:
    case LESS_THAN_OR_EQUAL:
    case NOT_EQUALS:
      final List<RexNode> operands = ((RexCall) e).getOperands();
      if (RexUtil.eq(operands.get(0), operands.get(1))
          && (unknownAsFalse
          || (!operands.get(0).getType().isNullable()
              && !operands.get(1).getType().isNullable()))) {
        switch (e.getKind()) {
        case EQUALS:
        case GREATER_THAN_OR_EQUAL:
        case LESS_THAN_OR_EQUAL:
          // "x = x" simplifies to "x is not null" (similarly <= and >=)
          return simplify(rexBuilder,
              rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL,
                  operands.get(0)));
        default:
          // "x != x" simplifies to "false" (similarly < and >)
          return rexBuilder.makeLiteral(false);
        }
      }
      return simplifyCall(rexBuilder, (RexCall) e);
    default:
      return e;
    }
  }

  private static RexNode simplifyCall(RexBuilder rexBuilder, RexCall e) {
    final List<RexNode> operands = new ArrayList<>(e.operands);
    simplifyList(rexBuilder, operands);
    if (operands.equals(e.operands)) {
      return e;
    }
    return rexBuilder.makeCall(e.op, operands);
  }

  /**
   * Simplifies a conjunction of boolean expressions.
   */
  public static RexNode simplifyAnds(RexBuilder rexBuilder,
      Iterable<? extends RexNode> nodes) {
    return simplifyAnds(rexBuilder, nodes, false);
  }

  public static RexNode simplifyAnds(RexBuilder rexBuilder,
      Iterable<? extends RexNode> nodes, boolean unknownAsFalse) {
    final List<RexNode> terms = new ArrayList<>();
    final List<RexNode> notTerms = new ArrayList<>();
    for (RexNode e : nodes) {
      RelOptUtil.decomposeConjunction(e, terms, notTerms);
    }
    simplifyList(rexBuilder, terms);
    simplifyList(rexBuilder, notTerms);
    if (unknownAsFalse) {
      return simplifyAnd2ForUnknownAsFalse(rexBuilder, terms, notTerms);
    }
    return simplifyAnd2(rexBuilder, terms, notTerms);
  }

  private static void simplifyList(RexBuilder rexBuilder, List<RexNode> terms) {
    for (int i = 0; i < terms.size(); i++) {
      terms.set(i, simplify(rexBuilder, terms.get(i)));
    }
  }

  private static RexNode simplifyNot(RexBuilder rexBuilder, RexCall call) {
    final RexNode a = call.getOperands().get(0);
    switch (a.getKind()) {
    case NOT:
      // NOT NOT x ==> x
      return simplify(rexBuilder, ((RexCall) a).getOperands().get(0));
    }
    final SqlKind negateKind = a.getKind().negate();
    if (a.getKind() != negateKind) {
      return simplify(rexBuilder,
          rexBuilder.makeCall(op(negateKind),
              ImmutableList.of(((RexCall) a).getOperands().get(0))));
    }
    final SqlKind negateKind2 = a.getKind().negateNullSafe();
    if (a.getKind() != negateKind2) {
      return simplify(rexBuilder,
          rexBuilder.makeCall(op(negateKind2), ((RexCall) a).getOperands()));
    }
    if (a.getKind() == SqlKind.AND) {
      // NOT distributivity for AND
      final List<RexNode> newOperands = new ArrayList<>();
      for (RexNode operand : ((RexCall) a).getOperands()) {
        newOperands.add(
            simplify(rexBuilder,
                rexBuilder.makeCall(SqlStdOperatorTable.NOT, operand)));
      }
      return simplify(rexBuilder,
          rexBuilder.makeCall(SqlStdOperatorTable.OR, newOperands));
    }
    if (a.getKind() == SqlKind.OR) {
      // NOT distributivity for OR
      final List<RexNode> newOperands = new ArrayList<>();
      for (RexNode operand : ((RexCall) a).getOperands()) {
        newOperands.add(
            simplify(rexBuilder,
                rexBuilder.makeCall(SqlStdOperatorTable.NOT, operand)));
      }
      return simplify(rexBuilder,
          rexBuilder.makeCall(SqlStdOperatorTable.AND, newOperands));
    }
    return call;
  }

  /** Negates a logical expression by adding or removing a NOT. */
  public static RexNode not(RexNode e) {
    switch (e.getKind()) {
    case NOT:
      return ((RexCall) e).getOperands().get(0);
    default:
      return new RexCall(e.getType(), SqlStdOperatorTable.NOT,
          ImmutableList.of(e));
    }
  }

  private static RexNode simplifyIs(RexBuilder rexBuilder, RexCall call) {
    final SqlKind kind = call.getKind();
    final RexNode a = call.getOperands().get(0);
    final RexNode simplified = simplifyIs2(rexBuilder, kind, a);
    if (simplified != null) {
      return simplified;
    }
    return call;
  }

  private static RexNode simplifyIs2(RexBuilder rexBuilder, SqlKind kind,
      RexNode a) {
    switch (kind) {
    case IS_NULL:
      // x IS NULL ==> FALSE (if x is not nullable)
      if (!a.getType().isNullable()) {
        return rexBuilder.makeLiteral(false);
      }
      break;
    case IS_NOT_NULL:
      // x IS NOT NULL ==> TRUE (if x is not nullable)
      RexNode simplified = simplifyIsNotNull(rexBuilder, a);
      if (simplified != null) {
        return simplified;
      }
      break;
    case IS_TRUE:
    case IS_NOT_FALSE:
      // x IS TRUE ==> x (if x is not nullable)
      // x IS NOT FALSE ==> x (if x is not nullable)
      if (!a.getType().isNullable()) {
        return simplify(rexBuilder, a);
      }
      break;
    case IS_FALSE:
    case IS_NOT_TRUE:
      // x IS NOT TRUE ==> NOT x (if x is not nullable)
      // x IS FALSE ==> NOT x (if x is not nullable)
      if (!a.getType().isNullable()) {
        return simplify(rexBuilder,
            rexBuilder.makeCall(SqlStdOperatorTable.NOT, a));
      }
      break;
    }
    switch (a.getKind()) {
    case NOT:
      // (NOT x) IS TRUE ==> x IS FALSE
      // Similarly for IS NOT TRUE, IS FALSE, etc.
      //
      // Note that
      //   (NOT x) IS TRUE !=> x IS FALSE
      // because of null values.
      return simplify(rexBuilder,
          rexBuilder.makeCall(op(kind.negate()),
              ((RexCall) a).getOperands().get(0)));
    }
    RexNode a2 = simplify(rexBuilder, a);
    if (a != a2) {
      return rexBuilder.makeCall(op(kind), ImmutableList.of(a2));
    }
    return null; // cannot be simplified
  }

  private static RexNode simplifyIsNotNull(RexBuilder rexBuilder, RexNode a) {
    if (!a.getType().isNullable()) {
      return rexBuilder.makeLiteral(true);
    }
    switch (Strong.policy(a.getKind())) {
    case ANY:
      final List<RexNode> operands = new ArrayList<>();
      for (RexNode operand : ((RexCall) a).getOperands()) {
        final RexNode simplified = simplifyIsNotNull(rexBuilder, operand);
        if (simplified == null) {
          operands.add(
              rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, operand));
        } else if (simplified.isAlwaysFalse()) {
          return rexBuilder.makeLiteral(false);
        } else {
          operands.add(simplified);
        }
      }
      return composeConjunction(rexBuilder, operands, false);
    case CUSTOM:
      switch (a.getKind()) {
      case LITERAL:
        return rexBuilder.makeLiteral(((RexLiteral) a).getValue() != null);
      default:
        throw new AssertionError("every CUSTOM policy needs a handler, "
            + a.getKind());
      }
    case AS_IS:
    default:
      return null;
    }
  }

  private static SqlOperator op(SqlKind kind) {
    switch (kind) {
    case IS_FALSE:
      return SqlStdOperatorTable.IS_FALSE;
    case IS_TRUE:
      return SqlStdOperatorTable.IS_TRUE;
    case IS_UNKNOWN:
      return SqlStdOperatorTable.IS_UNKNOWN;
    case IS_NULL:
      return SqlStdOperatorTable.IS_NULL;
    case IS_NOT_FALSE:
      return SqlStdOperatorTable.IS_NOT_FALSE;
    case IS_NOT_TRUE:
      return SqlStdOperatorTable.IS_NOT_TRUE;
    case IS_NOT_NULL:
      return SqlStdOperatorTable.IS_NOT_NULL;
    case EQUALS:
      return SqlStdOperatorTable.EQUALS;
    case NOT_EQUALS:
      return SqlStdOperatorTable.NOT_EQUALS;
    case LESS_THAN:
      return SqlStdOperatorTable.LESS_THAN;
    case GREATER_THAN:
      return SqlStdOperatorTable.GREATER_THAN;
    case LESS_THAN_OR_EQUAL:
      return SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
    case GREATER_THAN_OR_EQUAL:
      return SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
    default:
      throw new AssertionError(kind);
    }
  }

  private static RexNode simplifyCase(RexBuilder rexBuilder, RexCall call,
      boolean unknownAsFalse) {
    final List<RexNode> operands = call.getOperands();
    final List<RexNode> newOperands = new ArrayList<>();
    final Set<String> values = new HashSet<>();
    for (int i = 0; i < operands.size(); i++) {
      RexNode operand = operands.get(i);
      if (isCasePredicate(call, i)) {
        if (operand.isAlwaysTrue()) {
          // Predicate is always TRUE. Make value the ELSE and quit.
          newOperands.add(operands.get(++i));
          if (unknownAsFalse && isNull(operands.get(i))) {
            values.add(rexBuilder.makeLiteral(false).toString());
          } else {
            values.add(operands.get(i).toString());
          }
          break;
        } else if (operand.isAlwaysFalse() || isNull(operand)) {
          // Predicate is always FALSE or NULL. Skip predicate and value.
          ++i;
          continue;
        }
      } else {
        if (unknownAsFalse && isNull(operand)) {
          values.add(rexBuilder.makeLiteral(false).toString());
        } else {
          values.add(operand.toString());
        }
      }
      newOperands.add(operand);
    }
    assert newOperands.size() % 2 == 1;
    if (newOperands.size() == 1 || values.size() == 1) {
      final RexNode last = Util.last(newOperands);
      if (!call.getType().equals(last.getType())) {
        return rexBuilder.makeAbstractCast(call.getType(), last);
      }
      return last;
    }
  trueFalse:
    if (call.getType().getSqlTypeName() == SqlTypeName.BOOLEAN) {
      // Optimize CASE where every branch returns constant true or constant
      // false.
      final List<Pair<RexNode, RexNode>> pairs =
          casePairs(rexBuilder, newOperands);
      // 1) Possible simplification if unknown is treated as false:
      //   CASE
      //   WHEN p1 THEN TRUE
      //   WHEN p2 THEN TRUE
      //   ELSE FALSE
      //   END
      // can be rewritten to: (p1 or p2)
      if (unknownAsFalse) {
        final List<RexNode> terms = new ArrayList<>();
        int pos = 0;
        for (; pos < pairs.size(); pos++) {
          // True block
          Pair<RexNode, RexNode> pair = pairs.get(pos);
          if (!pair.getValue().isAlwaysTrue()) {
            break;
          }
          terms.add(pair.getKey());
        }
        for (; pos < pairs.size(); pos++) {
          // False block
          Pair<RexNode, RexNode> pair = pairs.get(pos);
          if (!pair.getValue().isAlwaysFalse() && !isNull(pair.getValue())) {
            break;
          }
        }
        if (pos == pairs.size()) {
          final RexNode disjunction = composeDisjunction(rexBuilder, terms, false);
          if (!call.getType().equals(disjunction.getType())) {
            return rexBuilder.makeCast(call.getType(), disjunction);
          }
          return disjunction;
        }
      }
      // 2) Another simplification
      //   CASE
      //   WHEN p1 THEN TRUE
      //   WHEN p2 THEN FALSE
      //   WHEN p3 THEN TRUE
      //   ELSE FALSE
      //   END
      // if p1...pn cannot be nullable
      for (Ord<Pair<RexNode, RexNode>> pair : Ord.zip(pairs)) {
        if (pair.e.getKey().getType().isNullable()) {
          break trueFalse;
        }
        if (!pair.e.getValue().isAlwaysTrue()
            && !pair.e.getValue().isAlwaysFalse()
            && (!unknownAsFalse || !isNull(pair.e.getValue()))) {
          break trueFalse;
        }
      }
      final List<RexNode> terms = new ArrayList<>();
      final List<RexNode> notTerms = new ArrayList<>();
      for (Ord<Pair<RexNode, RexNode>> pair : Ord.zip(pairs)) {
        if (pair.e.getValue().isAlwaysTrue()) {
          terms.add(andNot(rexBuilder, pair.e.getKey(), notTerms));
        } else {
          notTerms.add(pair.e.getKey());
        }
      }
      final RexNode disjunction = composeDisjunction(rexBuilder, terms, false);
      if (!call.getType().equals(disjunction.getType())) {
        return rexBuilder.makeCast(call.getType(), disjunction);
      }
      return disjunction;
    }
    if (newOperands.equals(operands)) {
      return call;
    }
    return call.clone(call.getType(), newOperands);
  }

  /** Given "CASE WHEN p1 THEN v1 ... ELSE e END"
   * returns [(p1, v1), ..., (true, e)]. */
  private static List<Pair<RexNode, RexNode>> casePairs(RexBuilder rexBuilder,
      List<RexNode> operands) {
    final ImmutableList.Builder<Pair<RexNode, RexNode>> builder =
        ImmutableList.builder();
    for (int i = 0; i < operands.size() - 1; i += 2) {
      builder.add(Pair.of(operands.get(i), operands.get(i + 1)));
    }
    builder.add(
        Pair.of((RexNode) rexBuilder.makeLiteral(true), Util.last(operands)));
    return builder.build();
  }

  public static RexNode simplifyAnd(RexBuilder rexBuilder, RexCall e,
      boolean unknownAsFalse) {
    final List<RexNode> terms = new ArrayList<>();
    final List<RexNode> notTerms = new ArrayList<>();
    RelOptUtil.decomposeConjunction(e, terms, notTerms);
    simplifyList(rexBuilder, terms);
    simplifyList(rexBuilder, notTerms);
    if (unknownAsFalse) {
      return simplifyAnd2ForUnknownAsFalse(rexBuilder, terms, notTerms);
    }
    return simplifyAnd2(rexBuilder, terms, notTerms);
  }

  public static RexNode simplifyAnd2(RexBuilder rexBuilder,
      List<RexNode> terms, List<RexNode> notTerms) {
    for (RexNode term : terms) {
      if (term.isAlwaysFalse()) {
        return rexBuilder.makeLiteral(false);
      }
    }
    if (terms.isEmpty() && notTerms.isEmpty()) {
      return rexBuilder.makeLiteral(true);
    }
    if (terms.size() == 1 && notTerms.isEmpty()) {
      // Make sure "x OR y OR x" (a single-term conjunction) gets simplified.
      return simplify(rexBuilder, terms.get(0));
    }
    // If one of the not-disjunctions is a disjunction that is wholly
    // contained in the disjunctions list, the expression is not
    // satisfiable.
    //
    // Example #1. x AND y AND z AND NOT (x AND y)  - not satisfiable
    // Example #2. x AND y AND NOT (x AND y)        - not satisfiable
    // Example #3. x AND y AND NOT (x AND y AND z)  - may be satisfiable
    for (RexNode notDisjunction : notTerms) {
      final List<RexNode> terms2 = RelOptUtil.conjunctions(notDisjunction);
      if (terms.containsAll(terms2)) {
        return rexBuilder.makeLiteral(false);
      }
    }
    // Add the NOT disjunctions back in.
    for (RexNode notDisjunction : notTerms) {
      terms.add(
          simplify(rexBuilder,
              rexBuilder.makeCall(SqlStdOperatorTable.NOT, notDisjunction)));
    }
    return composeConjunction(rexBuilder, terms, false);
  }

  /** As {@link #simplifyAnd2(RexBuilder, List, List)} but we assume that if the expression returns
   * UNKNOWN it will be interpreted as FALSE. */
  public static RexNode simplifyAnd2ForUnknownAsFalse(RexBuilder rexBuilder,
      List<RexNode> terms, List<RexNode> notTerms) {
    final RexExecutor executor = EXECUTOR;
    for (RexNode term : terms) {
      if (term.isAlwaysFalse()) {
        return rexBuilder.makeLiteral(false);
      }
    }
    if (terms.isEmpty() && notTerms.isEmpty()) {
      return rexBuilder.makeLiteral(true);
    }
    if (terms.size() == 1 && notTerms.isEmpty()) {
      // Make sure "x OR y OR x" (a single-term conjunction) gets simplified.
      return simplify(rexBuilder, terms.get(0), true);
    }
    // Try to simplify the expression
    final Multimap<String, Pair<String, RexNode>> equalityTerms = ArrayListMultimap.create();
    final Map<String, String> equalityConstantTerms = new HashMap<>();
    final Set<String> negatedTerms = new HashSet<>();
    final Set<String> nullOperands = new HashSet<>();
    final Set<RexNode> notNullOperands = new LinkedHashSet<>();
    final Set<String> comparedOperands = new HashSet<>();
    for (int i = 0; i < terms.size(); i++) {
      RexNode term = terms.get(i);
      if (!isDeterministic(term)) {
        continue;
      }
      // Simplify BOOLEAN expressions if possible
      while (term.getKind() == SqlKind.EQUALS) {
        RexCall call = (RexCall) term;
        if (call.getOperands().get(0).isAlwaysTrue()) {
          term = call.getOperands().get(1);
          terms.set(i, term);
          continue;
        } else if (call.getOperands().get(1).isAlwaysTrue()) {
          term = call.getOperands().get(0);
          terms.set(i, term);
          continue;
        }
        break;
      }
      switch (term.getKind()) {
      case EQUALS:
      case NOT_EQUALS:
      case LESS_THAN:
      case GREATER_THAN:
      case LESS_THAN_OR_EQUAL:
      case GREATER_THAN_OR_EQUAL:
        RexCall call = (RexCall) term;
        RexNode left = call.getOperands().get(0);
        comparedOperands.add(left.toString());
        // if it is a cast, we include the inner reference
        if (left.getKind() == SqlKind.CAST) {
          RexCall leftCast = (RexCall) left;
          comparedOperands.add(leftCast.getOperands().get(0).toString());
        }
        RexNode right = call.getOperands().get(1);
        comparedOperands.add(right.toString());
        // if it is a cast, we include the inner reference
        if (right.getKind() == SqlKind.CAST) {
          RexCall rightCast = (RexCall) right;
          comparedOperands.add(rightCast.getOperands().get(0).toString());
        }
        // Check for equality on different constants. If the same ref or CAST(ref)
        // is equal to different constants, this condition cannot be satisfied,
        // and hence it can be evaluated to FALSE
        if (term.getKind() == SqlKind.EQUALS) {
          final boolean leftRef = isReferenceOrAccess(left, true);
          final boolean rightRef = isReferenceOrAccess(right, true);
          if (right instanceof RexLiteral && leftRef) {
            final String literal = right.toString();
            final String prevLiteral = equalityConstantTerms.put(left.toString(), literal);
            if (prevLiteral != null && !literal.equals(prevLiteral)) {
              return rexBuilder.makeLiteral(false);
            }
          } else if (left instanceof RexLiteral && rightRef) {
            final String literal = left.toString();
            final String prevLiteral = equalityConstantTerms.put(right.toString(), literal);
            if (prevLiteral != null && !literal.equals(prevLiteral)) {
              return rexBuilder.makeLiteral(false);
            }
          } else if (leftRef && rightRef) {
            equalityTerms.put(left.toString(), Pair.of(right.toString(), term));
          }
        }
        // Assume the expression a > 5 is part of a Filter condition.
        // Then we can derive the negated term: a <= 5.
        // But as the comparison is string based and thus operands order dependent,
        // we should also add the inverted negated term: 5 >= a.
        // Observe that for creating the inverted term we invert the list of operands.
        RexNode negatedTerm = negate(rexBuilder, call);
        if (negatedTerm != null) {
          negatedTerms.add(negatedTerm.toString());
          RexNode invertNegatedTerm = invert(rexBuilder, (RexCall) negatedTerm);
          if (invertNegatedTerm != null) {
            negatedTerms.add(invertNegatedTerm.toString());
          }
        }
        break;
      case IN:
        comparedOperands.add(((RexCall) term).operands.get(0).toString());
        break;
      case BETWEEN:
        comparedOperands.add(((RexCall) term).operands.get(1).toString());
        break;
      case IS_NOT_NULL:
        notNullOperands.add(((RexCall) term).getOperands().get(0));
        terms.remove(i);
        --i;
        break;
      case IS_NULL:
        nullOperands.add(((RexCall) term).getOperands().get(0).toString());
      }
    }
    // If one column should be null and is in a comparison predicate,
    // it is not satisfiable.
    // Example. IS NULL(x) AND x < 5  - not satisfiable
    if (!Collections.disjoint(nullOperands, comparedOperands)) {
      return rexBuilder.makeLiteral(false);
    }
    // Check for equality of two refs wrt equality with constants
    // Example #1. x=5 AND y=5 AND x=y : x=5 AND y=5
    // Example #2. x=5 AND y=6 AND x=y - not satisfiable
    for (String ref1 : equalityTerms.keySet()) {
      final String literal1 = equalityConstantTerms.get(ref1);
      if (literal1 == null) {
        continue;
      }
      Collection<Pair<String, RexNode>> references = equalityTerms.get(ref1);
      for (Pair<String, RexNode> ref2 : references) {
        final String literal2 = equalityConstantTerms.get(ref2.left);
        if (literal2 == null) {
          continue;
        }
        if (!literal1.equals(literal2)) {
          // If an expression is equal to two different constants,
          // it is not satisfiable
          return rexBuilder.makeLiteral(false);
        }
        // Otherwise we can remove the term, as we already know that
        // the expression is equal to two constants
        terms.remove(ref2.right);
      }
    }
    // Remove not necessary IS NOT NULL expressions.
    //
    // Example. IS NOT NULL(x) AND x < 5  : x < 5
    for (RexNode operand : notNullOperands) {
      if (!comparedOperands.contains(operand.toString())) {
        terms.add(
            rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, operand));
      }
    }
    // If one of the not-disjunctions is a disjunction that is wholly
    // contained in the disjunctions list, the expression is not
    // satisfiable.
    //
    // Example #1. x AND y AND z AND NOT (x AND y)  - not satisfiable
    // Example #2. x AND y AND NOT (x AND y)        - not satisfiable
    // Example #3. x AND y AND NOT (x AND y AND z)  - may be satisfiable
    final Set<String> termsSet = new HashSet<String>(strings(terms));
    for (RexNode notDisjunction : notTerms) {
      if (!isDeterministic(notDisjunction)) {
        continue;
      }
      final List<String> terms2Set = strings(RelOptUtil.conjunctions(notDisjunction));
      if (termsSet.containsAll(terms2Set)) {
        return rexBuilder.makeLiteral(false);
      }
    }
    // Add the NOT disjunctions back in.
    for (RexNode notDisjunction : notTerms) {
      final RexNode call =
          rexBuilder.makeCall(SqlStdOperatorTable.NOT, notDisjunction);
      terms.add(simplify(rexBuilder, call, true, executor));
    }
    // The negated terms: only deterministic expressions
    for (String negatedTerm : negatedTerms) {
      if (termsSet.contains(negatedTerm)) {
        return rexBuilder.makeLiteral(false);
      }
    }
    return composeConjunction(rexBuilder, terms, false);
  }

  public static RexNode negate(RexBuilder rexBuilder, RexCall call) {
    switch (call.getKind()) {
    case EQUALS:
    case NOT_EQUALS:
    case LESS_THAN:
    case GREATER_THAN:
    case LESS_THAN_OR_EQUAL:
    case GREATER_THAN_OR_EQUAL:
      final SqlOperator op = op(call.getKind().negateNullSafe());
      return rexBuilder.makeCall(op, call.getOperands());
    }
    return null;
  }

  public static RexNode invert(RexBuilder rexBuilder, RexCall call) {
    switch (call.getKind()) {
    case EQUALS:
    case NOT_EQUALS:
    case LESS_THAN:
    case GREATER_THAN:
    case LESS_THAN_OR_EQUAL:
    case GREATER_THAN_OR_EQUAL:
      final SqlOperator op = op(call.getKind().reverse());
      return rexBuilder.makeCall(op, Lists.reverse(call.getOperands()));
    }
    return null;
  }

  /** Simplifies OR(x, x) into x, and similar. */
  public static RexNode simplifyOr(RexBuilder rexBuilder, RexCall call) {
    assert call.getKind() == SqlKind.OR;
    final List<RexNode> terms = RelOptUtil.disjunctions(call);
    return simplifyOrs(rexBuilder, terms);
  }

  /** Simplifies a list of terms and combines them into an OR.
   * Modifies the list in place. */
  public static RexNode simplifyOrs(RexBuilder rexBuilder,
      List<RexNode> terms) {
    for (int i = 0; i < terms.size(); i++) {
      final RexNode term = simplify(rexBuilder, terms.get(i));
      switch (term.getKind()) {
      case LITERAL:
        if (!RexLiteral.isNullLiteral(term)) {
          if (RexLiteral.booleanValue(term)) {
            return term; // true
          } else {
            terms.remove(i);
            --i;
            continue;
          }
        }
      }
      terms.set(i, term);
    }
    return composeDisjunction(rexBuilder, terms, false);
  }

  /**
   * Creates the expression {@code e1 AND NOT notTerm1 AND NOT notTerm2 ...}.
   */
  public static RexNode andNot(RexBuilder rexBuilder, RexNode e,
      RexNode... notTerms) {
    return andNot(rexBuilder, e, Arrays.asList(notTerms));
  }

  /**
   * Creates the expression {@code e1 AND NOT notTerm1 AND NOT notTerm2 ...}.
   *
   * <p>Examples:
   * <ul>
   *   <li>andNot(p) returns "p"
   *   <li>andNot(p, n1, n2) returns "p AND NOT n1 AND NOT n2"
   *   <li>andNot(x = 10, x = 20, y = 30, x = 30)
   *       returns "x = 10 AND NOT (y = 30)"
   * </ul>
   */
  public static RexNode andNot(final RexBuilder rexBuilder, RexNode e,
      Iterable<? extends RexNode> notTerms) {
    // If "e" is of the form "x = literal", remove all "x = otherLiteral"
    // terms from notTerms.
    switch (e.getKind()) {
    case EQUALS:
      final RexCall call = (RexCall) e;
      if (call.getOperands().get(1) instanceof RexLiteral) {
        notTerms = Iterables.filter(notTerms,
            new PredicateImpl<RexNode>() {
              public boolean test(RexNode input) {
                switch (input.getKind()) {
                case EQUALS:
                  RexCall call2 = (RexCall) input;
                  if (call2.getOperands().get(0)
                      .equals(call.getOperands().get(0))
                      && call2.getOperands().get(1) instanceof RexLiteral) {
                    return false;
                  }
                }
                return true;
              }
            });
      }
    }
    return composeConjunction(rexBuilder,
        Iterables.concat(ImmutableList.of(e),
            Iterables.transform(notTerms, notFn(rexBuilder))),
        false);
  }

  /** Returns whether a given operand of a CASE expression is a predicate.
   *
   * <p>A switched case (CASE x WHEN x1 THEN v1 ... ELSE e END) has an even
   * number of arguments and odd-numbered arguments are predicates.
   *
   * <p>A condition case (CASE WHEN p1 THEN v1 ... ELSE e END) has an odd
   * number of arguments and even-numbered arguments are predicates, except for
   * the last argument. */
  public static boolean isCasePredicate(RexCall call, int i) {
    assert call.getKind() == SqlKind.CASE;
    return i < call.operands.size() - 1
        && (call.operands.size() - i) % 2 == 1;
  }

  private static RexNode simplifyCast(RexBuilder rexBuilder, RexCall e,
      RexExecutor executor) {
    Preconditions.checkNotNull(executor);
    final RexNode operand = e.getOperands().get(0);
    switch (operand.getKind()) {
    case LITERAL:
      final RexLiteral literal = (RexLiteral) operand;
      final Comparable value = literal.getValue();
      final SqlTypeName typeName = literal.getTypeName();

      // First, try to remove the cast without changing the value.
      // makeCast and canRemoveCastFromLiteral have the same logic, so we are
      // sure to be able to remove the cast.
      if (rexBuilder.canRemoveCastFromLiteral(e.getType(), value, typeName)) {
        return rexBuilder.makeCast(e.getType(), operand);
      }

      // Next, try to convert the value to a different type,
      // e.g. CAST('123' as integer)
      switch (literal.getTypeName()) {
      case TIME:
        switch (e.getType().getSqlTypeName()) {
        case TIMESTAMP:
          return e;
        }
      }
      final List<RexNode> reducedValues = new ArrayList<>();
      executor.reduce(rexBuilder, ImmutableList.<RexNode>of(e), reducedValues);
      return Preconditions.checkNotNull(
          Iterables.getOnlyElement(reducedValues));
    default:
      return e;
    }
  }

  /** Returns a function that applies NOT to its argument. */
  public static Function<RexNode, RexNode> notFn(final RexBuilder rexBuilder) {
    return new Function<RexNode, RexNode>() {
      public RexNode apply(RexNode input) {
        return input.isAlwaysTrue()
            ? rexBuilder.makeLiteral(false)
            : input.isAlwaysFalse()
            ? rexBuilder.makeLiteral(true)
            : input.getKind() == SqlKind.NOT
            ? ((RexCall) input).operands.get(0)
            : rexBuilder.makeCall(SqlStdOperatorTable.NOT, input);
      }
    };
  }

  /** Returns whether an expression contains a {@link RexCorrelVariable}. */
  public static boolean containsCorrelation(RexNode condition) {
    try {
      condition.accept(CorrelationFinder.INSTANCE);
      return false;
    } catch (Util.FoundOne e) {
      return true;
    }
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Walks over expressions and builds a bank of common sub-expressions.
   */
  private static class ExpressionNormalizer extends RexVisitorImpl<RexNode> {
    final Map<String, RexNode> mapDigestToExpr = new HashMap<>();
    final boolean allowDups;

    protected ExpressionNormalizer(boolean allowDups) {
      super(true);
      this.allowDups = allowDups;
    }

    protected RexNode register(RexNode expr) {
      final String key = expr.toString();
      final RexNode previous = mapDigestToExpr.put(key, expr);
      if (!allowDups && (previous != null)) {
        throw new SubExprExistsException(expr);
      }
      return expr;
    }

    protected RexNode lookup(RexNode expr) {
      return mapDigestToExpr.get(expr.toString());
    }

    public RexNode visitInputRef(RexInputRef inputRef) {
      return register(inputRef);
    }

    public RexNode visitLiteral(RexLiteral literal) {
      return register(literal);
    }

    public RexNode visitCorrelVariable(RexCorrelVariable correlVariable) {
      return register(correlVariable);
    }

    public RexNode visitCall(RexCall call) {
      List<RexNode> normalizedOperands = new ArrayList<>();
      int diffCount = 0;
      for (RexNode operand : call.getOperands()) {
        operand.accept(this);
        final RexNode normalizedOperand = lookup(operand);
        normalizedOperands.add(normalizedOperand);
        if (normalizedOperand != operand) {
          ++diffCount;
        }
      }
      if (diffCount > 0) {
        call =
            call.clone(
                call.getType(),
                normalizedOperands);
      }
      return register(call);
    }

    public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
      return register(dynamicParam);
    }

    public RexNode visitRangeRef(RexRangeRef rangeRef) {
      return register(rangeRef);
    }

    public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
      final RexNode expr = fieldAccess.getReferenceExpr();
      expr.accept(this);
      final RexNode normalizedExpr = lookup(expr);
      if (normalizedExpr != expr) {
        fieldAccess =
            new RexFieldAccess(
                normalizedExpr,
                fieldAccess.getField());
      }
      return register(fieldAccess);
    }

    /**
     * Thrown if there is a sub-expression.
     */
    private static class SubExprExistsException extends ControlFlowException {
      SubExprExistsException(RexNode expr) {
        Util.discard(expr);
      }
    }
  }

  /**
   * Walks over an expression and throws an exception if it finds an
   * {@link RexInputRef} with an ordinal beyond the number of fields in the
   * input row type, or a {@link RexLocalRef} with ordinal greater than that set
   * using {@link #setLimit(int)}.
   */
  private static class ForwardRefFinder extends RexVisitorImpl<Void> {
    private int limit = -1;
    private final RelDataType inputRowType;

    public ForwardRefFinder(RelDataType inputRowType) {
      super(true);
      this.inputRowType = inputRowType;
    }

    public Void visitInputRef(RexInputRef inputRef) {
      super.visitInputRef(inputRef);
      if (inputRef.getIndex() >= inputRowType.getFieldCount()) {
        throw new IllegalForwardRefException();
      }
      return null;
    }

    public Void visitLocalRef(RexLocalRef inputRef) {
      super.visitLocalRef(inputRef);
      if (inputRef.getIndex() >= limit) {
        throw new IllegalForwardRefException();
      }
      return null;
    }

    public void setLimit(int limit) {
      this.limit = limit;
    }

    /** Thrown to abort a visit when we find an illegal forward reference.
     * It changes control flow but is not considered an error. */
    static class IllegalForwardRefException extends ControlFlowException {
    }
  }

  /**
   * Visitor which builds a bitmap of the inputs used by an expression.
   */
  public static class FieldAccessFinder extends RexVisitorImpl<Void> {
    private final List<RexFieldAccess> fieldAccessList;

    public FieldAccessFinder() {
      super(true);
      fieldAccessList = new ArrayList<>();
    }

    public Void visitFieldAccess(RexFieldAccess fieldAccess) {
      fieldAccessList.add(fieldAccess);
      return null;
    }

    public Void visitCall(RexCall call) {
      for (RexNode operand : call.operands) {
        operand.accept(this);
      }
      return null;
    }

    public List<RexFieldAccess> getFieldAccessList() {
      return fieldAccessList;
    }
  }

  /** Helps {@link org.apache.calcite.rex.RexUtil#toCnf}. */
  private static class CnfHelper {
    final RexBuilder rexBuilder;
    int currentCount;
    final int maxNodeCount; // negative means no limit

    private CnfHelper(RexBuilder rexBuilder, int maxNodeCount) {
      this.rexBuilder = rexBuilder;
      this.maxNodeCount = maxNodeCount;
    }

    public RexNode toCnf(RexNode rex) {
      try {
        this.currentCount = 0;
        return toCnf2(rex);
      } catch (OverflowError e) {
        Util.swallow(e, null);
        return rex;
      }
    }

    private RexNode toCnf2(RexNode rex) {
      final List<RexNode> operands;
      switch (rex.getKind()) {
      case AND:
        incrementAndCheck();
        operands = flattenAnd(((RexCall) rex).getOperands());
        final List<RexNode> cnfOperands = Lists.newArrayList();
        for (RexNode node : operands) {
          RexNode cnf = toCnf2(node);
          switch (cnf.getKind()) {
          case AND:
            incrementAndCheck();
            cnfOperands.addAll(((RexCall) cnf).getOperands());
            break;
          default:
            incrementAndCheck();
            cnfOperands.add(cnf);
          }
        }
        return and(cnfOperands);
      case OR:
        incrementAndCheck();
        operands = flattenOr(((RexCall) rex).getOperands());
        final RexNode head = operands.get(0);
        final RexNode headCnf = toCnf2(head);
        final List<RexNode> headCnfs = RelOptUtil.conjunctions(headCnf);
        final RexNode tail = or(Util.skip(operands));
        final RexNode tailCnf = toCnf2(tail);
        final List<RexNode> tailCnfs = RelOptUtil.conjunctions(tailCnf);
        final List<RexNode> list = Lists.newArrayList();
        for (RexNode h : headCnfs) {
          for (RexNode t : tailCnfs) {
            list.add(or(ImmutableList.of(h, t)));
          }
        }
        return and(list);
      case NOT:
        final RexNode arg = ((RexCall) rex).getOperands().get(0);
        switch (arg.getKind()) {
        case NOT:
          return toCnf2(((RexCall) arg).getOperands().get(0));
        case OR:
          operands = ((RexCall) arg).getOperands();
          return toCnf2(and(Lists.transform(flattenOr(operands), ADD_NOT)));
        case AND:
          operands = ((RexCall) arg).getOperands();
          return toCnf2(or(Lists.transform(flattenAnd(operands), ADD_NOT)));
        default:
          incrementAndCheck();
          return rex;
        }
      default:
        incrementAndCheck();
        return rex;
      }
    }

    private void incrementAndCheck() {
      if (maxNodeCount >= 0 && ++currentCount > maxNodeCount) {
        throw OverflowError.INSTANCE;
      }
    }

    /** Exception to catch when we pass the limit. */
    @SuppressWarnings("serial")
    private static class OverflowError extends ControlFlowException {
      @SuppressWarnings("ThrowableInstanceNeverThrown")
      protected static final OverflowError INSTANCE = new OverflowError();

      private OverflowError() {}
    }

    private RexNode pull(RexNode rex) {
      final List<RexNode> operands;
      switch (rex.getKind()) {
      case AND:
        operands = flattenAnd(((RexCall) rex).getOperands());
        return and(pullList(operands));
      case OR:
        operands = flattenOr(((RexCall) rex).getOperands());
        final Map<String, RexNode> factors = commonFactors(operands);
        if (factors.isEmpty()) {
          return or(operands);
        }
        final List<RexNode> list = Lists.newArrayList();
        for (RexNode operand : operands) {
          list.add(removeFactor(factors, operand));
        }
        return and(Iterables.concat(factors.values(), ImmutableList.of(or(list))));
      default:
        return rex;
      }
    }

    private List<RexNode> pullList(List<RexNode> nodes) {
      final List<RexNode> list = Lists.newArrayList();
      for (RexNode node : nodes) {
        RexNode pulled = pull(node);
        switch (pulled.getKind()) {
        case AND:
          list.addAll(((RexCall) pulled).getOperands());
          break;
        default:
          list.add(pulled);
        }
      }
      return list;
    }

    private Map<String, RexNode> commonFactors(List<RexNode> nodes) {
      final Map<String, RexNode> map = Maps.newHashMap();
      int i = 0;
      for (RexNode node : nodes) {
        if (i++ == 0) {
          for (RexNode conjunction : RelOptUtil.conjunctions(node)) {
            map.put(conjunction.toString(), conjunction);
          }
        } else {
          map.keySet().retainAll(strings(RelOptUtil.conjunctions(node)));
        }
      }
      return map;
    }

    private RexNode removeFactor(Map<String, RexNode> factors, RexNode node) {
      List<RexNode> list = Lists.newArrayList();
      for (RexNode operand : RelOptUtil.conjunctions(node)) {
        if (!factors.containsKey(operand.toString())) {
          list.add(operand);
        }
      }
      return and(list);
    }

    private RexNode and(Iterable<? extends RexNode> nodes) {
      return composeConjunction(rexBuilder, nodes, false);
    }

    private RexNode or(Iterable<? extends RexNode> nodes) {
      return composeDisjunction(rexBuilder, nodes, false);
    }
  }

  /** Transforms a list of expressions to the list of digests. */
  public static List<String> strings(List<RexNode> list) {
    return Lists.transform(list, TO_STRING);
  }

  /** Helps {@link org.apache.calcite.rex.RexUtil#toDnf}. */
  private static class DnfHelper {
    final RexBuilder rexBuilder;

    private DnfHelper(RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }

    public RexNode toDnf(RexNode rex) {
      final List<RexNode> operands;
      switch (rex.getKind()) {
      case AND:
        operands = flattenAnd(((RexCall) rex).getOperands());
        final RexNode head = operands.get(0);
        final RexNode headDnf = toDnf(head);
        final List<RexNode> headDnfs = RelOptUtil.disjunctions(headDnf);
        final RexNode tail = and(Util.skip(operands));
        final RexNode tailDnf = toDnf(tail);
        final List<RexNode> tailDnfs = RelOptUtil.disjunctions(tailDnf);
        final List<RexNode> list = Lists.newArrayList();
        for (RexNode h : headDnfs) {
          for (RexNode t : tailDnfs) {
            list.add(and(ImmutableList.of(h, t)));
          }
        }
        return or(list);
      case OR:
        operands = flattenOr(((RexCall) rex).getOperands());
        return or(toDnfs(operands));
      case NOT:
        final RexNode arg = ((RexCall) rex).getOperands().get(0);
        switch (arg.getKind()) {
        case NOT:
          return toDnf(((RexCall) arg).getOperands().get(0));
        case OR:
          operands = ((RexCall) arg).getOperands();
          return toDnf(and(Lists.transform(flattenOr(operands), ADD_NOT)));
        case AND:
          operands = ((RexCall) arg).getOperands();
          return toDnf(or(Lists.transform(flattenAnd(operands), ADD_NOT)));
        default:
          return rex;
        }
      default:
        return rex;
      }
    }

    private List<RexNode> toDnfs(List<RexNode> nodes) {
      final List<RexNode> list = Lists.newArrayList();
      for (RexNode node : nodes) {
        RexNode dnf = toDnf(node);
        switch (dnf.getKind()) {
        case OR:
          list.addAll(((RexCall) dnf).getOperands());
          break;
        default:
          list.add(dnf);
        }
      }
      return list;
    }

    private RexNode and(Iterable<? extends RexNode> nodes) {
      return composeConjunction(rexBuilder, nodes, false);
    }

    private RexNode or(Iterable<? extends RexNode> nodes) {
      return composeDisjunction(rexBuilder, nodes, false);
    }
  }


  /** Shuttle that adds {@code offset} to each {@link RexInputRef} in an
   * expression. */
  private static class RexShiftShuttle extends RexShuttle {
    private final int offset;

    public RexShiftShuttle(int offset) {
      this.offset = offset;
    }

    @Override public RexNode visitInputRef(RexInputRef input) {
      return new RexInputRef(input.getIndex() + offset, input.getType());
    }
  }

  /** Visitor that throws {@link org.apache.calcite.util.Util.FoundOne} if
   * applied to an expression that contains a {@link RexCorrelVariable}. */
  private static class CorrelationFinder extends RexVisitorImpl<Void> {
    static final CorrelationFinder INSTANCE = new CorrelationFinder();

    private CorrelationFinder() {
      super(true);
    }

    @Override public Void visitCorrelVariable(RexCorrelVariable var) {
      throw Util.FoundOne.NULL;
    }
  }

  /** Shuttle that fixes up an expression to match changes in nullability of
   * input fields. */
  public static class FixNullabilityShuttle extends RexShuttle {
    private final List<RelDataType> typeList;
    private final RexBuilder rexBuilder;

    public FixNullabilityShuttle(RexBuilder rexBuilder,
        List<RelDataType> typeList) {
      this.typeList = typeList;
      this.rexBuilder = rexBuilder;
    }

    @Override public RexNode visitInputRef(RexInputRef ref) {
      final RelDataType rightType = typeList.get(ref.getIndex());
      final RelDataType refType = ref.getType();
      if (refType == rightType) {
        return ref;
      }
      final RelDataType refType2 =
          rexBuilder.getTypeFactory().createTypeWithNullability(refType,
              rightType.isNullable());
      if (refType2 == rightType) {
        return new RexInputRef(ref.getIndex(), refType2);
      }
      throw new AssertionError("mismatched type " + ref + " " + rightType);
    }
  }

  /** Visitor that throws {@link org.apache.calcite.util.Util.FoundOne} if
   * applied to an expression that contains a {@link RexSubQuery}. */
  public static class SubQueryFinder extends RexVisitorImpl<Void> {
    public static final SubQueryFinder INSTANCE = new SubQueryFinder();

    /** Returns whether a {@link Project} contains a sub-query. */
    public static final Predicate<Project> PROJECT_PREDICATE =
        new PredicateImpl<Project>() {
          public boolean test(Project project) {
            for (RexNode node : project.getProjects()) {
              try {
                node.accept(INSTANCE);
              } catch (Util.FoundOne e) {
                return true;
              }
            }
            return false;
          }
        };

    /** Returns whether a {@link Filter} contains a sub-query. */
    public static final Predicate<Filter> FILTER_PREDICATE =
        new PredicateImpl<Filter>() {
          public boolean test(Filter filter) {
            try {
              filter.getCondition().accept(INSTANCE);
              return false;
            } catch (Util.FoundOne e) {
              return true;
            }
          }
        };

    /** Returns whether a {@link Join} contains a sub-query. */
    public static final Predicate<Join> JOIN_PREDICATE =
        new PredicateImpl<Join>() {
          public boolean test(Join join) {
            try {
              join.getCondition().accept(INSTANCE);
              return false;
            } catch (Util.FoundOne e) {
              return true;
            }
          }
        };

    private SubQueryFinder() {
      super(true);
    }

    @Override public Void visitSubQuery(RexSubQuery subQuery) {
      throw new Util.FoundOne(subQuery);
    }

    public static RexSubQuery find(Iterable<RexNode> nodes) {
      for (RexNode node : nodes) {
        try {
          node.accept(INSTANCE);
        } catch (Util.FoundOne e) {
          return (RexSubQuery) e.getNode();
        }
      }
      return null;
    }

    public static RexSubQuery find(RexNode node) {
      try {
        node.accept(INSTANCE);
        return null;
      } catch (Util.FoundOne e) {
        return (RexSubQuery) e.getNode();
      }
    }
  }

  /** Deep expressions simplifier. */
  public static class ExprSimplifier extends RexShuttle {
    private final RexBuilder rexBuilder;
    private final RexExecutor executor;
    private final boolean unknownAsFalse;
    private final Map<RexNode, Boolean> unknownAsFalseMap;

    public ExprSimplifier(RexBuilder rexBuilder, boolean unknownAsFalse,
        RexExecutor executor) {
      this.rexBuilder = Preconditions.checkNotNull(rexBuilder);
      this.executor = Preconditions.checkNotNull(executor);
      this.unknownAsFalse = unknownAsFalse;
      this.unknownAsFalseMap = new HashMap<>();
    }

    @Override public RexNode visitCall(RexCall call) {
      Boolean unknownAsFalseCall = unknownAsFalse;
      if (unknownAsFalseCall) {
        switch (call.getKind()) {
        case AND:
        case CASE:
          unknownAsFalseCall = this.unknownAsFalseMap.get(call);
          if (unknownAsFalseCall == null) {
            // Top operator
            unknownAsFalseCall = true;
          }
          break;
        default:
          unknownAsFalseCall = false;
        }
        for (RexNode operand : call.operands) {
          this.unknownAsFalseMap.put(operand, unknownAsFalseCall);
        }
      }
      RexNode node = super.visitCall(call);
      RexNode simplifiedNode =
          simplify(rexBuilder, node, unknownAsFalseCall, executor);
      if (node == simplifiedNode) {
        return node;
      }
      if (simplifiedNode.getType().equals(call.getType())) {
        return simplifiedNode;
      }
      return rexBuilder.makeCast(call.getType(), simplifiedNode, true);
    }
  }
}

// End RexUtil.java
