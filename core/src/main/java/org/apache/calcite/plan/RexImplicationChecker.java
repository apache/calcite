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

package plan;

import org.apache.calcite.DataContext;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.VisitorDataContext;
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
import org.apache.calcite.sql.fun.SqlCastFunction;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;



/** Checks if Condition X logically implies Condition Y
 *
 * <p>(x > 10) implies (x > 5)</p>
 *
 * <p>(y = 10) implies (y < 30 AND x > 30)</p>
 */
public class RexImplicationChecker {
  final RexBuilder builder;
  final RexExecutorImpl rexImpl;
  final RelDataType rowType;

  public RexImplicationChecker(RexBuilder builder,
                                RexExecutorImpl rexImpl,
                                RelDataType rowType) {
    this.builder = builder;
    this.rexImpl = rexImpl;
    this.rowType = rowType;
  }

  /**
   * Checks if condition first implies (=>) condition second
   * This reduces to SAT problem which is NP-Complete
   * When func says first => second then it is definitely true
   * It cannot prove if first doesnot imply second.
   * @param first
   * @param second
   * @return true if it can prove first => second, otherwise false i.e.,
   * it doesn't know if implication holds .
   */
  public boolean implies(RexNode first, RexNode second) {

    // Validation
    if (!validate(first, second)) {
      return false;
    }

    RexCall firstCond = (RexCall) first;
    RexCall secondCond = (RexCall) second;

    // Get DNF
    RexNode firstDnf = RexUtil.toDnf(builder, first);
    RexNode secondDnf = RexUtil.toDnf(builder, second);

    // Check Trivial Cases
    if (firstDnf.isAlwaysFalse()
            || secondDnf.isAlwaysTrue()) {
      return true;
    }

    /** Decompose DNF into List of Conjunctions
     *
     * (x > 10 AND y > 30) OR (z > 90) will be converted to
     * list of 2 conditions:
     * 1. (x > 10 AND y > 30)
     * 2. (z > 90)
     *
     */
    List<RexNode> firstDnfs = RelOptUtil.disjunctions(firstDnf);
    List<RexNode> secondDnfs = RelOptUtil.disjunctions(secondDnf);

    for (RexNode f : firstDnfs) {
      if (!f.isAlwaysFalse()) {
        //Check if f implies atleast
        // one of the conjunctions in list secondDnfs
        boolean implyOneConjuntion = false;
        for (RexNode s : secondDnfs) {
          if (s.isAlwaysFalse()) { // f cannot imply s
            continue;
          }

          if (impliesConjunction(f, s)) {
            // Satisfies one of the condition, so lets
            // move to next conjunction in firstDnfs
            implyOneConjuntion = true;
            break;
          }
        } //end of inner loop

        // If f couldnot imply even one conjunction in
        // secondDnfs, then final implication may be false
        if (!implyOneConjuntion) {
          return false;
        }
      }
    } //end of outer loop

    return true;
  }

  /** Checks if conjunction first => Conjunction second**/
  private boolean impliesConjunction(RexNode first, RexNode second) {

    InputUsageFinder firstUsgFinder = new InputUsageFinder();
    InputUsageFinder secondUsgFinder = new InputUsageFinder();

    RexUtil.apply(firstUsgFinder, new ArrayList<RexNode>(), first);
    RexUtil.apply(secondUsgFinder, new ArrayList<RexNode>(), second);

    // Check Support

    if (!checkSupport(firstUsgFinder, secondUsgFinder)) {
      return false;
    }

    List<Pair<RexInputRef, RexNode>> usgList = new ArrayList<>();
    for (Map.Entry<RexInputRef, InputRefUsage<SqlOperator,
            RexNode>> entry: firstUsgFinder.usageMap.entrySet()) {
      final List<Pair<SqlOperator, RexNode>> list = entry.getValue().getUsageList();
      usgList.add(Pair.of(entry.getKey(), list.get(0).getValue()));
    }

    final DataContext dataValues = VisitorDataContext.getDataContext(rowType, usgList);

    if (dataValues == null) {
      return false;
    }

    ImmutableList<RexNode> constExps = ImmutableList.of(second);
    final RexExecutable exec = rexImpl.getExecutable(builder,
            constExps, rowType);

    exec.setDataContext(dataValues);
    Object[] result = exec.execute();

    return result != null && result.length == 1 && result[0] instanceof Boolean
            && (Boolean) result[0];
  }

  private boolean checkSupport(InputUsageFinder firstUsgFinder, InputUsageFinder secondUsgFinder) {
    Map<RexInputRef, InputRefUsage<SqlOperator,
            RexNode>> firstUsgMap = firstUsgFinder.usageMap;
    Map<RexInputRef, InputRefUsage<SqlOperator,
            RexNode>> secondUsgMap = secondUsgFinder.usageMap;

    for (Map.Entry<RexInputRef, InputRefUsage<SqlOperator,
            RexNode>> entry: firstUsgMap.entrySet()) {
      if (entry.getValue().usageCount > 1) {
        return false;
      }
    }

    for (Map.Entry<RexInputRef, InputRefUsage<SqlOperator,
            RexNode>> entry: secondUsgMap.entrySet()) {
      final InputRefUsage<SqlOperator, RexNode> secondUsage = entry.getValue();
      if (secondUsage.getUsageCount() > 1
              || secondUsage.getUsageList().size() != 1) {
        return false;
      }

      final InputRefUsage<SqlOperator, RexNode> firstUsage = firstUsgMap.get(entry.getKey());
      if (firstUsage == null
              || firstUsage.getUsageList().size() != 1) {
        return false;
      }

      final Pair<SqlOperator, RexNode> fUse = firstUsage.getUsageList().get(0);
      final Pair<SqlOperator, RexNode> sUse = secondUsage.getUsageList().get(0);

      final SqlKind fkind = fUse.getKey().getKind();

      if (fkind == SqlKind.EQUALS) {
        return true;
      }

      switch (sUse.getKey().getKind()) {
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
        if (!(fkind == SqlKind.GREATER_THAN)
                && !(fkind == SqlKind.GREATER_THAN_OR_EQUAL)) {
          return false;
        }
        break;
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
        if (!(fkind == SqlKind.LESS_THAN)
                && !(fkind == SqlKind.LESS_THAN_OR_EQUAL)) {
          return false;
        }
        break;
      default:
        return false;
      }
    }
    return true;
  }

  private boolean validate(RexNode first, RexNode second) {
    if (first == null || second == null) {
      return false;
    }
    if (!(first instanceof RexCall)
            || !(second instanceof RexCall)) {
      return false;
    }
    return true;
  }


  /**
   * Visitor which builds a Usage Map of inputs used by an expression.
   */
  private static class InputUsageFinder extends RexVisitorImpl<Void> {
    public final Map<RexInputRef, InputRefUsage<SqlOperator,
            RexNode>> usageMap = new HashMap<>();

    public InputUsageFinder() {
      super(true);
    }

    public Void visitInputRef(RexInputRef inputRef) {
      InputRefUsage<SqlOperator,
              RexNode> inputRefUse = getUsageMap(inputRef);
      inputRefUse.incrUsage();
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
        updateUsage(call);
        break;
      default:
      }
      return super.visitCall(call);
    }

    private void updateUsage(RexCall call) {
      final List<RexNode> operands = call.getOperands();
      RexNode first = removeCast(operands.get(0));
      RexNode second = removeCast(operands.get(1));

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
      return RelOptUtil.op(
              RelOptUtil.reverse(op.getKind()), op);
    }

    private static RexNode removeCast(RexNode inputRef) {
      if (inputRef instanceof RexCall) {
        final RexCall castedRef = (RexCall) inputRef;
        final SqlOperator operator = castedRef.getOperator();
        if (operator instanceof SqlCastFunction) {
          inputRef = castedRef.getOperands().get(0);
        }
      }
      return inputRef;
    }

    private void updateUsage(SqlOperator op, RexInputRef inputRef, RexNode literal) {
      InputRefUsage<SqlOperator,
              RexNode> inputRefUse = getUsageMap(inputRef);
      Pair<SqlOperator, RexNode> use = Pair.of(op, literal);
      inputRefUse.getUsageList().add(use);
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
   * DataStructure to store usage of InputRefs in expression
   * @param <T1>
   * @param <T2>
   */

  private static class InputRefUsage<T1, T2> {
    private final List<Pair<T1, T2>> usageList =
            new ArrayList<Pair<T1, T2>>();
    private int usageCount = 0;

    public InputRefUsage() {}

    public int getUsageCount() {
      return usageCount;
    }

    public void incrUsage() {
      usageCount++;
    }

    public List<Pair<T1, T2>> getUsageList() {
      return usageList;
    }
  }
}
