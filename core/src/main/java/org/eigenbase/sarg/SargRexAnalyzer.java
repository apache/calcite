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
package org.eigenbase.sarg;

import java.util.*;

import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;

/**
 * SargRexAnalyzer attempts to translate a rex predicate into a {@link
 * SargBinding}. It assumes that the predicate expression is already
 * well-formed.
 */
public class SargRexAnalyzer {
  //~ Instance fields --------------------------------------------------------

  private final SargFactory factory;

  /**
   * If true, conjuntions on the same input reference are disallowed, as well
   * as all disjunctions. Also, only a single range predicate is allowed.
   */
  private final boolean simpleMode;

  private final Map<SqlOperator, CallConvertlet> convertletMap;

  private boolean failed;

  private RexInputRef boundInputRef;

  private RexNode coordinate;

  private boolean variableSeen;

  private boolean reverse;

  private List<SargExpr> exprStack;

  private List<RexNode> nonSargFilterList;

  private List<SargBinding> sargBindingList;

  private Map<SargExpr, RexNode> sarg2RexMap;

  /**
   * If >= 0, treat RexInputRefs whose index is within the range
   * [lowerRexInputIdx, upperRexInputIdx) as coordinates in expressions
   */
  private int lowerRexInputIdx;

  /**
   * If >= 0, treat RexInputRefs whose index is within the range
   * [lowerRexInputIdx, upperRexInputIdx) as coordinates in expressions
   */
  private int upperRexInputIdx;

  //~ Constructors -----------------------------------------------------------

  SargRexAnalyzer(
      SargFactory factory,
      boolean simpleMode) {
    this(factory, simpleMode, -1, -1);
  }

  SargRexAnalyzer(
      SargFactory factory,
      boolean simpleMode,
      int lowerRexInputRef,
      int upperRexInputRef) {
    this.factory = factory;
    this.simpleMode = simpleMode;
    this.lowerRexInputIdx = lowerRexInputRef;
    this.upperRexInputIdx = upperRexInputRef;
    assert (lowerRexInputIdx < 0 && upperRexInputIdx < 0)
        || (lowerRexInputIdx >= 0 && upperRexInputIdx >= 0);

    convertletMap = new HashMap<SqlOperator, CallConvertlet>();

    registerConvertlet(
        SqlStdOperatorTable.EQUALS,
        new ComparisonConvertlet(
            null,
            SargStrictness.CLOSED));

    registerConvertlet(
        SqlStdOperatorTable.IS_NULL,
        new ComparisonConvertlet(
            null,
            SargStrictness.CLOSED));

    registerConvertlet(
        SqlStdOperatorTable.IS_TRUE,
        new ComparisonConvertlet(
            null,
            SargStrictness.CLOSED));

    registerConvertlet(
        SqlStdOperatorTable.IS_FALSE,
        new ComparisonConvertlet(
            null,
            SargStrictness.CLOSED));

    registerConvertlet(
        SqlStdOperatorTable.IS_UNKNOWN,
        new ComparisonConvertlet(
            null,
            SargStrictness.CLOSED));

    registerConvertlet(
        SqlStdOperatorTable.LESS_THAN,
        new ComparisonConvertlet(
            SargBoundType.UPPER,
            SargStrictness.OPEN));

    registerConvertlet(
        SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
        new ComparisonConvertlet(
            SargBoundType.UPPER,
            SargStrictness.CLOSED));

    registerConvertlet(
        SqlStdOperatorTable.GREATER_THAN,
        new ComparisonConvertlet(
            SargBoundType.LOWER,
            SargStrictness.OPEN));

    registerConvertlet(
        SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
        new ComparisonConvertlet(
            SargBoundType.LOWER,
            SargStrictness.CLOSED));

    registerConvertlet(
        SqlStdOperatorTable.AND,
        new BooleanConvertlet(
            SargSetOperator.INTERSECTION));

    if (!simpleMode) {
      registerConvertlet(
          SqlStdOperatorTable.OR,
          new BooleanConvertlet(
              SargSetOperator.UNION));
    }

    registerConvertlet(
        SqlStdOperatorTable.NOT,
        new BooleanConvertlet(
            SargSetOperator.COMPLEMENT));

    // TODO: likeOperator (via complement notLikeOperator)

    // TODO:  non-literal constants (e.g. CURRENT_USER)
  }

  //~ Methods ----------------------------------------------------------------

  private void registerConvertlet(
      SqlOperator op,
      CallConvertlet convertlet) {
    convertletMap.put(op, convertlet);
  }

  // REVIEW jvs 18-Mar-2006:  rename these two to decomposeConjunction
  // and recomposeConjunction so "one not skilled in the art" will
  // still have a chance of figuring out what they're for just
  // from the names.  Also, some explanation of the state maintained
  // by SargRexAnalyzer across the various calls is in order.
  // (It used to be one-shot, but no longer.)

  /**
   * Reconstructs a rex predicate from a list of SargExprs which will be
   * AND'ed together.
   */
  private void recomposeConjunction() {
    for (int i = 0; i < sargBindingList.size(); i++) {
      final SargBinding currBinding = sargBindingList.get(i);
      final RexInputRef currRef = currBinding.getInputRef();
      SargExpr currSargExpr = currBinding.getExpr();
      RexNode currAndNode = sarg2RexMap.get(currSargExpr);

      // don't need this anymore
      // will be have new mapping put back if currSargExpr remain
      // unchanged.
      sarg2RexMap.remove(currSargExpr);
      boolean recomp = false;

      // search the rest of the list to find SargExpr on the same col.
      ListIterator<SargBinding> iter =
          sargBindingList.listIterator(i + 1);

      while (iter.hasNext()) {
        final SargBinding nextBinding = iter.next();
        final RexInputRef nextRef = nextBinding.getInputRef();
        final SargExpr nextSargExpr = nextBinding.getExpr();

        if (nextRef.getIndex() == currRef.getIndex()) {
          // build new SargExpr
          SargSetExpr expr =
              factory.newSetExpr(
                  currSargExpr.getDataType(),
                  SargSetOperator.INTERSECTION);
          expr.addChild(currSargExpr);
          expr.addChild(nextSargExpr);

          // build new RexNode
          currAndNode =
              factory.getRexBuilder().makeCall(
                  SqlStdOperatorTable.AND,
                  currAndNode,
                  sarg2RexMap.get(nextSargExpr));

          currSargExpr = expr;

          sarg2RexMap.remove(nextSargExpr);
          iter.remove();

          recomp = true;
        }
      }

      if (recomp) {
        assert !simpleMode;
        if (!testDynamicParamSupport(currSargExpr)) {
          // Oops, we can't actually support the conjunction we
          // recomposed.  Toss it.  (We could do a better job by at
          // least using part of it, but the effort might be better
          // spent on implementing deferred expression evaluation.)
          nonSargFilterList.add(currAndNode);
          sargBindingList.remove(i);
          continue;
        }
      }

      if (recomp) {
        SargBinding newBinding = new SargBinding(currSargExpr, currRef);
        sargBindingList.remove(i);
        sargBindingList.add(i, newBinding);
      }

      sarg2RexMap.put(currSargExpr, currAndNode);
    }
  }

  /**
   * Analyzes a rex predicate.
   *
   * @param rexPredicate predicate to be analyzed
   * @return a list of SargBindings contained in the input rex predicate
   */
  public List<SargBinding> analyzeAll(RexNode rexPredicate) {
    sargBindingList = new ArrayList<SargBinding>();
    sarg2RexMap = new HashMap<SargExpr, RexNode>();
    nonSargFilterList = new ArrayList<RexNode>();

    // Flatten out the RexNode tree into a list of terms that
    // are AND'ed together
    final List<RexNode> rexCFList = RelOptUtil.conjunctions(rexPredicate);

    // In simple mode, each input ref can only be referenced once, so
    // keep a list of them.  We also only allow one non-point expression.
    List<Integer> boundRefList = new ArrayList<Integer>();
    boolean rangeFound = false;

    for (RexNode rexPred : rexCFList) {
      final SargBinding sargBinding = analyze(rexPred);
      if (sargBinding != null) {
        if (simpleMode) {
          RexInputRef inputRef = sargBinding.getInputRef();
          if (boundRefList.contains(inputRef.getIndex())) {
            nonSargFilterList.add(rexPred);
            continue;
          } else {
            boundRefList.add(inputRef.getIndex());
          }
          SargIntervalSequence sargSeq =
              sargBinding.getExpr().evaluate();
          if (sargSeq.isRange()) {
            if (rangeFound) {
              nonSargFilterList.add(rexPred);
              continue;
            } else {
              rangeFound = true;
            }
          }
        }
        sargBindingList.add(sargBinding);
        sarg2RexMap.put(
            sargBinding.getExpr(),
            rexPred);
      } else {
        nonSargFilterList.add(rexPred);
      }
    }

    // Reset the state variables used during analyze, just for sanity sake.
    failed = false;
    boundInputRef = null;
    clearLeaf();

    // Combine the AND terms back together.
    recomposeConjunction();

    return sargBindingList;
  }

  /**
   * Tests whether we can support the usage of dynamic parameters in a given
   * SargExpr.
   *
   * @param sargExpr expression to test
   * @return true if supported
   */
  private boolean testDynamicParamSupport(SargExpr sargExpr) {
    // NOTE jvs 18-Mar-2006: we don't currently support boolean operators
    // over predicates on dynamic parameters.  The reason is that we
    // evaluate sarg expressions at prepare time rather than at execution
    // time.  To support them, we would need a way to defer evaluation
    // until execution time (Broadbase used to do that).

    Set<RexDynamicParam> dynamicParams = new HashSet<RexDynamicParam>();
    sargExpr.collectDynamicParams(dynamicParams);
    if (dynamicParams.isEmpty()) {
      // no dynamic params, no problem
      return true;
    }
    if (sargExpr instanceof SargIntervalExpr) {
      // We can support a single point or ray, which is the
      // most that would have been generated by the time this is
      // called.  Should probably assert that.
      return true;
    }

    // Anything else is unsupported.
    return false;
  }

  /**
   * Reconstructs a rex predicate from the non-sargable filter predicates
   * which are AND'ed together.
   *
   * @return the rex predicate reconstructed from the non-sargable predicates.
   */
  public RexNode getNonSargFilterRexNode() {
    if (nonSargFilterList.isEmpty()) {
      return null;
    }

    RexNode newAndNode = nonSargFilterList.get(0);

    for (int i = 1; i < nonSargFilterList.size(); i++) {
      newAndNode =
          factory.getRexBuilder().makeCall(
              SqlStdOperatorTable.AND,
              newAndNode,
              nonSargFilterList.get(i));
    }

    return newAndNode;
  }

  /**
   * @deprecated use {@link #getNonSargFilterRexNode()}
   */
  public RexNode getPostFilterRexNode() {
    return getNonSargFilterRexNode();
  }

  /**
   * Reconstructs a rex predicate from a list of SargBindings which are AND'ed
   * together.
   *
   * @param sargBindingList list of SargBindings to be converted.
   * @return the rex predicate reconstructed from the list of SargBindings.
   */
  public RexNode getSargBindingListToRexNode(
      List<SargBinding> sargBindingList) {
    if (sargBindingList.isEmpty()) {
      return null;
    }

    RexNode newAndNode = sarg2RexMap.get(sargBindingList.get(0).getExpr());

    for (int i = 1; i < sargBindingList.size(); i++) {
      RexNode nextNode =
          sarg2RexMap.get(sargBindingList.get(i).getExpr());
      newAndNode =
          factory.getRexBuilder().makeCall(
              SqlStdOperatorTable.AND,
              newAndNode,
              nextNode);
    }
    return newAndNode;
  }

  /**
   * @deprecated use {@link #getSargBindingListToRexNode(List)}
   */
  public RexNode getResidualSargRexNode(List<SargBinding> residualSargList) {
    return getSargBindingListToRexNode(residualSargList);
  }

  /**
   * Analyzes a rex predicate.
   *
   * @param rexPredicate predicate to be analyzed
   * @return corresponding bound sarg expression, or null if analysis failed
   */
  public SargBinding analyze(RexNode rexPredicate) {
    NodeVisitor visitor = new NodeVisitor();

    // Initialize analysis state.
    exprStack = new ArrayList<SargExpr>();
    failed = false;
    boundInputRef = null;
    clearLeaf();

    // Walk the predicate.
    rexPredicate.accept(visitor);

    if (boundInputRef == null) {
      // No variable references at all, so not sargable.
      failed = true;
    }

    if (exprStack.isEmpty()) {
      failed = true;
    }

    if (failed) {
      return null;
    }

    // well-formedness assumption
    assert exprStack.size() == 1;

    SargExpr expr = exprStack.get(0);

    if (!testDynamicParamSupport(expr)) {
      failed = true;
      return null;
    }

    return new SargBinding(expr, boundInputRef);
  }

  private void clearLeaf() {
    coordinate = null;
    variableSeen = false;
    reverse = false;
  }

  //~ Inner Classes ----------------------------------------------------------

  private abstract class CallConvertlet {
    public abstract void convert(RexCall call);
  }

  private class ComparisonConvertlet extends CallConvertlet {
    private final SargBoundType boundType;

    private final SargStrictness strictness;

    ComparisonConvertlet(
        SargBoundType boundType,
        SargStrictness strictness) {
      this.boundType = boundType;
      this.strictness = strictness;
    }

    // implement CallConvertlet
    public void convert(RexCall call) {
      if (!variableSeen) {
        failed = true;
      }

      SqlOperator op = call.getOperator();
      switch (op.getKind()) {
      case IS_NULL:
        coordinate = factory.getRexBuilder().constantNull();
        break;
      case IS_TRUE:
        coordinate = factory.getRexBuilder().makeLiteral(true);
        break;
      case IS_FALSE:
        coordinate = factory.getRexBuilder().makeLiteral(false);
        break;
      default:
        if (coordinate == null) {
          failed = true;
        }
      }

      if (failed) {
        return;
      }

      SargIntervalExpr expr =
          factory.newIntervalExpr(boundInputRef.getType());

      if (boundType == null) {
        expr.setPoint(coordinate);
      } else {
        SargBoundType actualBound = boundType;
        if (reverse) {
          if (actualBound == SargBoundType.LOWER) {
            actualBound = SargBoundType.UPPER;
          } else {
            actualBound = SargBoundType.LOWER;
          }
        }
        if (actualBound == SargBoundType.LOWER) {
          expr.setLower(coordinate, strictness);
        } else {
          expr.setUpper(coordinate, strictness);
        }
      }
      exprStack.add(expr);

      clearLeaf();
    }
  }

  private class BooleanConvertlet extends CallConvertlet {
    private final SargSetOperator setOp;

    BooleanConvertlet(SargSetOperator setOp) {
      this.setOp = setOp;
    }

    // implement CallConvertlet
    public void convert(RexCall call) {
      // REVIEW jvs 18-Mar-2006:  The test for variableSeen
      // here and elsewhere precludes predicates like where
      // (boolean_col AND (int_col > 10)).  Should probably
      // support bare boolean columns as predicates with
      // coordinate TRUE.

      if (variableSeen || (coordinate != null)) {
        failed = true;
      }

      if (failed) {
        return;
      }

      int nOperands = call.getOperands().size();
      assert exprStack.size() >= nOperands;

      SargSetExpr expr =
          factory.newSetExpr(
              boundInputRef.getType(),
              setOp);

      // Pop the correct number of operands off the stack
      // and transfer them to the new set expression.
      ListIterator<SargExpr> iter =
          exprStack.listIterator(
              exprStack.size() - nOperands);
      while (iter.hasNext()) {
        expr.addChild(iter.next());
        iter.remove();
      }

      exprStack.add(expr);
    }
  }

  private class NodeVisitor extends RexVisitorImpl<Void> {
    NodeVisitor() {
      // go deep
      super(true);
    }

    public Void visitInputRef(RexInputRef inputRef) {
      boolean coordinate = !isRealRexInputRef(inputRef);
      if (coordinate) {
        visitCoordinate(inputRef);
        return null;
      }
      variableSeen = true;
      if (boundInputRef == null) {
        boundInputRef = inputRef;
        return null;
      }
      if (inputRef.getIndex() != boundInputRef.getIndex()) {
        // sargs can only be over a single variable
        failed = true;
        return null;
      }
      return null;
    }

    private boolean isRealRexInputRef(RexInputRef inputRef) {
      if (lowerRexInputIdx < 0 && upperRexInputIdx < 0) {
        return true;
      }
      int idx = inputRef.getIndex();
      return idx < lowerRexInputIdx || idx >= upperRexInputIdx;
    }

    public Void visitLiteral(RexLiteral literal) {
      visitCoordinate(literal);
      return null;
    }

    public Void visitOver(RexOver over) {
      failed = true;
      return null;
    }

    public Void visitCorrelVariable(RexCorrelVariable correlVariable) {
      failed = true;
      return null;
    }

    public Void visitCall(RexCall call) {
      CallConvertlet convertlet = convertletMap.get(call.getOperator());
      if (convertlet == null) {
        failed = true;
        return null;
      }

      // visit operands first
      super.visitCall(call);

      convertlet.convert(call);
      return null;
    }

    public Void visitDynamicParam(RexDynamicParam dynamicParam) {
      if (simpleMode) {
        failed = true;
      } else {
        visitCoordinate(dynamicParam);
      }
      return null;
    }

    private void visitCoordinate(RexNode node) {
      if (!variableSeen) {
        // We may be looking at an expression like (1 < x).
        reverse = true;
      }
      if (coordinate != null) {
        // e.g. constants on both sides of comparison
        failed = true;
        return;
      }
      coordinate = node;
    }

    public Void visitRangeRef(RexRangeRef rangeRef) {
      failed = true;
      return null;
    }

    public Void visitFieldAccess(RexFieldAccess fieldAccess) {
      failed = true;
      return null;
    }
  }
}

// End SargRexAnalyzer.java
