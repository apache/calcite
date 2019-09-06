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
package org.apache.calcite.plan;

import org.apache.calcite.DataContext;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexExecutable;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.trace.CalciteLogger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Checks whether one condition logically implies another.
 *
 * <p>If A &rArr; B, whenever A is true, B will be true also.
 *
 * <p>For example:
 * <ul>
 * <li>(x &gt; 10) &rArr; (x &gt; 5)
 * <li>(x = 10) &rArr; (x &lt; 30 OR y &gt; 30)
 * <li>(x = 10) &rArr; (x IS NOT NULL)
 * <li>(x &gt; 10 AND y = 20) &rArr; (x &gt; 5)
 * </ul>
 */
public class RexImplicationChecker {
  private static final CalciteLogger LOGGER =
      new CalciteLogger(LoggerFactory.getLogger(RexImplicationChecker.class));

  final RexBuilder builder;
  final RexExecutorImpl executor;
  final RelDataType rowType;

  public RexImplicationChecker(
      RexBuilder builder,
      RexExecutorImpl executor,
      RelDataType rowType) {
    this.builder = Objects.requireNonNull(builder);
    this.executor = Objects.requireNonNull(executor);
    this.rowType = Objects.requireNonNull(rowType);
  }

  /**
   * Checks if condition first implies (&rArr;) condition second.
   *
   * <p>This reduces to SAT problem which is NP-Complete.
   * When this method says first implies second then it is definitely true.
   * But it cannot prove that first does not imply second.
   *
   * @param first first condition
   * @param second second condition
   * @return true if it can prove first &rArr; second; otherwise false i.e.,
   * it doesn't know if implication holds
   */
  public boolean implies(RexNode first, RexNode second) {
    // Validation
    if (!validate(first, second)) {
      return false;
    }

    LOGGER.debug("Checking if {} => {}", first.toString(), second.toString());

    // Get DNF
    RexNode firstDnf = RexUtil.toDnf(builder, first);
    RexNode secondDnf = RexUtil.toDnf(builder, second);

    // Check Trivial Cases
    if (firstDnf.isAlwaysFalse()
        || secondDnf.isAlwaysTrue()) {
      return true;
    }

    // Decompose DNF into a list of conditions, each of which is a conjunction.
    // For example,
    //   (x > 10 AND y > 30) OR (z > 90)
    // is converted to list of 2 conditions:
    //   (x > 10 AND y > 30)
    //   z > 90
    //
    // Similarly, decompose CNF into a list of conditions, each of which is a
    // disjunction.
    List<RexNode> firsts = RelOptUtil.disjunctions(firstDnf);
    List<RexNode> seconds = RelOptUtil.disjunctions(secondDnf);

    for (RexNode f : firsts) {
      // Check if f implies at least
      // one of the conjunctions in list secondDnfs.
      // If f could not imply even one conjunction in
      // secondDnfs, then final implication may be false.
      if (!impliesAny(f, seconds)) {
        LOGGER.debug("{} does not imply {}", first, second);
        return false;
      }
    }

    LOGGER.debug("{} implies {}", first, second);
    return true;
  }

  /** Returns whether the predicate {@code first} implies (&rArr;)
   * at least one predicate in {@code seconds}. */
  private boolean impliesAny(RexNode first, List<RexNode> seconds) {
    for (RexNode second : seconds) {
      if (impliesConjunction(first, second)) {
        return true;
      }
    }
    return false;
  }

  /** Returns whether the predicate {@code first} implies {@code second} (both
   * may be conjunctions). */
  private boolean impliesConjunction(RexNode first, RexNode second) {
    if (implies2(first, second)) {
      return true;
    }
    switch (first.getKind()) {
    case AND:
      for (RexNode f : RelOptUtil.conjunctions(first)) {
        if (implies2(f, second)) {
          return true;
        }
      }
    }
    return false;
  }

  /** Returns whether the predicate {@code first} (not a conjunction)
   * implies {@code second}. */
  private boolean implies2(RexNode first, RexNode second) {
    if (second.isAlwaysFalse()) { // f cannot imply s
      return false;
    }

    // E.g. "x is null" implies "x is null".
    if (first.equals(second)) {
      return true;
    }

    // Several things imply "IS NOT NULL"
    switch (second.getKind()) {
    case IS_NOT_NULL:
      // Suppose we know that first is strong in second; that is,
      // the if second is null, then first will be null.
      // Then, first being not null implies that second is not null.
      //
      // For example, first is "x > y", second is "x".
      // If we know that "x > y" is not null, we know that "x" is not null.
      final RexNode operand = ((RexCall) second).getOperands().get(0);
      final Strong strong = new Strong() {
        @Override public boolean isNull(RexNode node) {
          return node.equals(operand)
              || super.isNull(node);
        }
      };
      if (strong.isNull(first)) {
        return true;
      }
    }

    final InputUsageFinder firstUsageFinder = new InputUsageFinder();
    final InputUsageFinder secondUsageFinder = new InputUsageFinder();

    RexUtil.apply(firstUsageFinder, ImmutableList.of(), first);
    RexUtil.apply(secondUsageFinder, ImmutableList.of(), second);

    // Check Support
    if (!checkSupport(firstUsageFinder, secondUsageFinder)) {
      LOGGER.warn("Support for checking {} => {} is not there", first, second);
      return false;
    }

    ImmutableList.Builder<Set<Pair<RexInputRef, RexNode>>> usagesBuilder =
        ImmutableList.builder();
    for (Map.Entry<RexInputRef, InputRefUsage<SqlOperator, RexNode>> entry
        : firstUsageFinder.usageMap.entrySet()) {
      ImmutableSet.Builder<Pair<RexInputRef, RexNode>> usageBuilder =
          ImmutableSet.builder();
      if (entry.getValue().usageList.size() > 0) {
        for (final Pair<SqlOperator, RexNode> pair
            : entry.getValue().usageList) {
          usageBuilder.add(Pair.of(entry.getKey(), pair.getValue()));
        }
        usagesBuilder.add(usageBuilder.build());
      }
    }

    final Set<List<Pair<RexInputRef, RexNode>>> usages =
        Sets.cartesianProduct(usagesBuilder.build());

    for (List<Pair<RexInputRef, RexNode>> usageList : usages) {
      // Get the literals from first conjunction and executes second conjunction
      // using them.
      //
      // E.g., for
      //   x > 30 &rArr; x > 10,
      // we will replace x by 30 in second expression and execute it i.e.,
      //   30 > 10
      //
      // If it's true then we infer implication.
      final DataContext dataValues =
          VisitorDataContext.of(rowType, usageList);

      if (!isSatisfiable(second, dataValues)) {
        return false;
      }
    }

    return true;
  }

  private boolean isSatisfiable(RexNode second, DataContext dataValues) {
    if (dataValues == null) {
      return false;
    }

    ImmutableList<RexNode> constExps = ImmutableList.of(second);
    final RexExecutable exec =
        executor.getExecutable(builder, constExps, rowType);

    Object[] result;
    exec.setDataContext(dataValues);
    try {
      result = exec.execute();
    } catch (Exception e) {
      // TODO: CheckSupport should not allow this exception to be thrown
      // Need to monitor it and handle all the cases raising them.
      LOGGER.warn("Exception thrown while checking if => {}: {}", second, e.getMessage());
      return false;
    }
    return result != null
        && result.length == 1
        && result[0] instanceof Boolean
        && (Boolean) result[0];
  }

  /**
   * Looks at the usage of variables in first and second conjunction to decide
   * whether this kind of expression is currently supported for proving first
   * implies second.
   *
   * <ol>
   * <li>Variables should be used only once in both the conjunction against
   * given set of operations only: &gt;, &lt;, &le;, &ge;, =; &ne;.
   *
   * <li>All the variables used in second condition should be used even in the
   * first.
   *
   * <li>If operator used for variable in first is op1 and op2 for second, then
   * we support these combination for conjunction (op1, op2) then op1, op2
   * belongs to one of the following sets:
   *
   * <ul>
   *    <li>(&lt;, &le;) X (&lt;, &le;) <i>note: X represents cartesian product</i>
   *    <li>(&gt; / &ge;) X (&gt;, &ge;)
   *    <li>(=) X (&gt;, &ge;, &lt;, &le;, =, &ne;)
   *    <li>(&ne;, =)
   * </ul>
   *
   * <li>We support at most 2 operators to be be used for a variable in first
   * and second usages.
   *
   * </ol>
   *
   * @return whether input usage pattern is supported
   */
  private boolean checkSupport(InputUsageFinder firstUsageFinder,
      InputUsageFinder secondUsageFinder) {
    final Map<RexInputRef, InputRefUsage<SqlOperator, RexNode>> firstUsageMap =
        firstUsageFinder.usageMap;
    final Map<RexInputRef, InputRefUsage<SqlOperator, RexNode>> secondUsageMap =
        secondUsageFinder.usageMap;

    for (Map.Entry<RexInputRef, InputRefUsage<SqlOperator, RexNode>> entry
        : secondUsageMap.entrySet()) {
      final InputRefUsage<SqlOperator, RexNode> secondUsage = entry.getValue();
      final List<Pair<SqlOperator, RexNode>> secondUsageList = secondUsage.usageList;
      final int secondLen = secondUsageList.size();

      if (secondUsage.usageCount != secondLen || secondLen > 2) {
        return false;
      }

      final InputRefUsage<SqlOperator, RexNode> firstUsage =
          firstUsageMap.get(entry.getKey());

      if (firstUsage == null
          || firstUsage.usageList.size() != firstUsage.usageCount
          || firstUsage.usageCount > 2) {
        return false;
      }

      final List<Pair<SqlOperator, RexNode>> firstUsageList = firstUsage.usageList;
      final int firstLen = firstUsageList.size();

      final SqlKind fKind = firstUsageList.get(0).getKey().getKind();
      final SqlKind sKind = secondUsageList.get(0).getKey().getKind();
      final SqlKind fKind2 =
          (firstUsageList.size() == 2) ? firstUsageList.get(1).getKey().getKind() : null;
      final SqlKind sKind2 =
          (secondUsageList.size() == 2) ? secondUsageList.get(1).getKey().getKind() : null;

      if (firstLen == 2 && secondLen == 2
          && !(isEquivalentOp(fKind, sKind) && isEquivalentOp(fKind2, sKind2))
          && !(isEquivalentOp(fKind, sKind2) && isEquivalentOp(fKind2, sKind))) {
        return false;
      } else if (firstLen == 1 && secondLen == 1
          && fKind != SqlKind.EQUALS && !isSupportedUnaryOperators(sKind)
          && !isEquivalentOp(fKind, sKind)) {
        return false;
      } else if (firstLen == 1 && secondLen == 2 && fKind != SqlKind.EQUALS) {
        return false;
      } else if (firstLen == 2 && secondLen == 1) {
        // Allow only cases like
        // x < 30 and x < 40 implies x < 70
        // x > 30 and x < 40 implies x < 70
        // But disallow cases like
        // x > 30 and x > 40 implies x < 70
        if (!isOppositeOp(fKind, fKind2) && !isSupportedUnaryOperators(sKind)
            && !(isEquivalentOp(fKind, fKind2) && isEquivalentOp(fKind, sKind))) {
          return false;
        }
      }
    }

    return true;
  }

  private boolean isSupportedUnaryOperators(SqlKind kind) {
    switch (kind) {
    case IS_NOT_NULL:
    case IS_NULL:
      return true;
    default:
      return false;
    }
  }

  private boolean isEquivalentOp(SqlKind fKind, SqlKind sKind) {
    switch (sKind) {
    case GREATER_THAN:
    case GREATER_THAN_OR_EQUAL:
      if (!(fKind == SqlKind.GREATER_THAN)
          && !(fKind == SqlKind.GREATER_THAN_OR_EQUAL)) {
        return false;
      }
      break;
    case LESS_THAN:
    case LESS_THAN_OR_EQUAL:
      if (!(fKind == SqlKind.LESS_THAN)
          && !(fKind == SqlKind.LESS_THAN_OR_EQUAL)) {
        return false;
      }
      break;
    default:
      return false;
    }

    return true;
  }

  private boolean isOppositeOp(SqlKind fKind, SqlKind sKind) {
    switch (sKind) {
    case GREATER_THAN:
    case GREATER_THAN_OR_EQUAL:
      if (!(fKind == SqlKind.LESS_THAN)
          && !(fKind == SqlKind.LESS_THAN_OR_EQUAL)) {
        return false;
      }
      break;
    case LESS_THAN:
    case LESS_THAN_OR_EQUAL:
      if (!(fKind == SqlKind.GREATER_THAN)
          && !(fKind == SqlKind.GREATER_THAN_OR_EQUAL)) {
        return false;
      }
      break;
    default:
      return false;
    }
    return true;
  }

  private boolean validate(RexNode first, RexNode second) {
    return first instanceof RexCall && second instanceof RexCall;
  }

  /**
   * Visitor that builds a usage map of inputs used by an expression.
   *
   * <p>E.g: for x &gt; 10 AND y &lt; 20 AND x = 40, usage map is as follows:
   * <ul>
   * <li>key: x value: {(&gt;, 10),(=, 40), usageCount = 2}
   * <li>key: y value: {(&gt;, 20), usageCount = 1}
   * </ul>
   */
  private static class InputUsageFinder extends RexVisitorImpl<Void> {
    final Map<RexInputRef, InputRefUsage<SqlOperator, RexNode>> usageMap =
        new HashMap<>();

    InputUsageFinder() {
      super(true);
    }

    public Void visitInputRef(RexInputRef inputRef) {
      InputRefUsage<SqlOperator, RexNode> inputRefUse = getUsageMap(inputRef);
      inputRefUse.usageCount++;
      return null;
    }

    @Override public Void visitCall(RexCall call) {
      switch (call.getOperator().getKind()) {
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
      case EQUALS:
      case NOT_EQUALS:
        updateBinaryOpUsage(call);
        break;
      case IS_NULL:
      case IS_NOT_NULL:
        updateUnaryOpUsage(call);
        break;
      default:
      }
      return super.visitCall(call);
    }

    private void updateUnaryOpUsage(RexCall call) {
      final List<RexNode> operands = call.getOperands();
      RexNode first = RexUtil.removeCast(operands.get(0));

      if (first.isA(SqlKind.INPUT_REF)) {
        updateUsage(call.getOperator(), (RexInputRef) first, null);
      }
    }

    private void updateBinaryOpUsage(RexCall call) {
      final List<RexNode> operands = call.getOperands();
      RexNode first = RexUtil.removeCast(operands.get(0));
      RexNode second = RexUtil.removeCast(operands.get(1));

      if (first.isA(SqlKind.INPUT_REF)
          && second.isA(SqlKind.LITERAL)) {
        updateUsage(call.getOperator(), (RexInputRef) first, second);
      }

      if (first.isA(SqlKind.LITERAL)
          && second.isA(SqlKind.INPUT_REF)) {
        updateUsage(reverse(call.getOperator()), (RexInputRef) second, first);
      }
    }

    private SqlOperator reverse(SqlOperator op) {
      return RelOptUtil.op(op.getKind().reverse(), op);
    }

    private void updateUsage(SqlOperator op, RexInputRef inputRef,
        RexNode literal) {
      final InputRefUsage<SqlOperator, RexNode> inputRefUse =
          getUsageMap(inputRef);
      Pair<SqlOperator, RexNode> use = Pair.of(op, literal);
      inputRefUse.usageList.add(use);
    }

    private InputRefUsage<SqlOperator, RexNode> getUsageMap(RexInputRef rex) {
      InputRefUsage<SqlOperator, RexNode> inputRefUse = usageMap.get(rex);
      if (inputRefUse == null) {
        inputRefUse = new InputRefUsage<>();
        usageMap.put(rex, inputRefUse);
      }

      return inputRefUse;
    }
  }

  /**
   * Usage of a {@link RexInputRef} in an expression.
   *
   * @param <T1> left type
   * @param <T2> right type */
  private static class InputRefUsage<T1, T2> {
    private final List<Pair<T1, T2>> usageList = new ArrayList<>();
    private int usageCount = 0;
  }
}

// End RexImplicationChecker.java
