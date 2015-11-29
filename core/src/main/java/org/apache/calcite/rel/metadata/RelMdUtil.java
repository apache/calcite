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
package org.apache.calcite.rel.metadata;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.NumberUtil;

import com.google.common.collect.ImmutableList;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * RelMdUtil provides utility methods used by the metadata provider methods.
 */
public class RelMdUtil {
  //~ Static fields/initializers ---------------------------------------------

  public static final SqlFunction ARTIFICIAL_SELECTIVITY_FUNC =
      new SqlFunction("ARTIFICIAL_SELECTIVITY",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.BOOLEAN, // returns boolean since we'll AND it
          null,
          OperandTypes.NUMERIC, // takes a numeric param
          SqlFunctionCategory.SYSTEM);

  //~ Methods ----------------------------------------------------------------

  private RelMdUtil() {
  }

  /**
   * Creates a RexNode that stores a selectivity value corresponding to the
   * selectivity of a semijoin. This can be added to a filter to simulate the
   * effect of the semijoin during costing, but should never appear in a real
   * plan since it has no physical implementation.
   *
   * @param rel the semijoin of interest
   * @return constructed rexnode
   */
  public static RexNode makeSemiJoinSelectivityRexNode(SemiJoin rel) {
    RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
    double selectivity =
        computeSemiJoinSelectivity(
            rel.getLeft(),
            rel.getRight(),
            rel);
    RexNode selec =
        rexBuilder.makeApproxLiteral(new BigDecimal(selectivity));
    return rexBuilder.makeCall(ARTIFICIAL_SELECTIVITY_FUNC, selec);
  }

  /**
   * Returns the selectivity value stored in the rexnode
   *
   * @param artificialSelecFuncNode rexnode containing the selectivity value
   * @return selectivity value
   */
  public static double getSelectivityValue(RexNode artificialSelecFuncNode) {
    assert artificialSelecFuncNode instanceof RexCall;
    RexCall call = (RexCall) artificialSelecFuncNode;
    assert call.getOperator() == ARTIFICIAL_SELECTIVITY_FUNC;
    RexNode operand = call.getOperands().get(0);
    BigDecimal bd = (BigDecimal) ((RexLiteral) operand).getValue();
    return bd.doubleValue();
  }

  /**
   * Computes the selectivity of a semijoin filter if it is applied on a fact
   * table. The computation is based on the selectivity of the dimension
   * table/columns and the number of distinct values in the fact table
   * columns.
   *
   * @param rel semijoin rel
   * @return calculated selectivity
   */
  public static double computeSemiJoinSelectivity(SemiJoin rel) {
    return computeSemiJoinSelectivity(
        rel.getLeft(),
        rel.getRight(),
        rel.getLeftKeys(),
        rel.getRightKeys());
  }

  /**
   * Computes the selectivity of a semijoin filter if it is applied on a fact
   * table. The computation is based on the selectivity of the dimension
   * table/columns and the number of distinct values in the fact table
   * columns.
   *
   * @param factRel fact table participating in the semijoin
   * @param dimRel  dimension table participating in the semijoin
   * @param rel     semijoin rel
   * @return calculated selectivity
   */
  public static double computeSemiJoinSelectivity(
      RelNode factRel,
      RelNode dimRel,
      SemiJoin rel) {
    return computeSemiJoinSelectivity(
        factRel,
        dimRel,
        rel.getLeftKeys(),
        rel.getRightKeys());
  }

  /**
   * Computes the selectivity of a semijoin filter if it is applied on a fact
   * table. The computation is based on the selectivity of the dimension
   * table/columns and the number of distinct values in the fact table
   * columns.
   *
   * @param factRel     fact table participating in the semijoin
   * @param dimRel      dimension table participating in the semijoin
   * @param factKeyList LHS keys used in the filter
   * @param dimKeyList  RHS keys used in the filter
   * @return calculated selectivity
   */
  public static double computeSemiJoinSelectivity(
      RelNode factRel,
      RelNode dimRel,
      List<Integer> factKeyList,
      List<Integer> dimKeyList) {
    ImmutableBitSet.Builder factKeys = ImmutableBitSet.builder();
    for (int factCol : factKeyList) {
      factKeys.set(factCol);
    }
    ImmutableBitSet.Builder dimKeyBuilder = ImmutableBitSet.builder();
    for (int dimCol : dimKeyList) {
      dimKeyBuilder.set(dimCol);
    }
    final ImmutableBitSet dimKeys = dimKeyBuilder.build();

    Double factPop =
        RelMetadataQuery.getPopulationSize(factRel, factKeys.build());
    if (factPop == null) {
      // use the dimension population if the fact population is
      // unavailable; since we're filtering the fact table, that's
      // the population we ideally want to use
      factPop = RelMetadataQuery.getPopulationSize(dimRel, dimKeys);
    }

    // if cardinality and population are available, use them; otherwise
    // use percentage original rows
    Double selectivity;
    Double dimCard =
        RelMetadataQuery.getDistinctRowCount(
            dimRel,
            dimKeys,
            null);
    if ((dimCard != null) && (factPop != null)) {
      // to avoid division by zero
      if (factPop < 1.0) {
        factPop = 1.0;
      }
      selectivity = dimCard / factPop;
    } else {
      selectivity = RelMetadataQuery.getPercentageOriginalRows(dimRel);
    }

    if (selectivity == null) {
      // set a default selectivity based on the number of semijoin keys
      selectivity =
          Math.pow(
              0.1,
              dimKeys.cardinality());
    } else if (selectivity > 1.0) {
      selectivity = 1.0;
    }
    return selectivity;
  }

  /**
   * Returns true if the columns represented in a bit mask are definitely
   * known to form a unique column set.
   *
   * @param rel     the relational expression that the column mask corresponds
   *                to
   * @param colMask bit mask containing columns that will be tested for
   *                uniqueness
   * @return true if bit mask represents a unique column set; false if not (or
   * if no metadata is available)
   */
  public static boolean areColumnsDefinitelyUnique(
      RelNode rel,
      ImmutableBitSet colMask) {
    Boolean b = RelMetadataQuery.areColumnsUnique(rel, colMask, false);
    return b != null && b;
  }

  public static Boolean areColumnsUnique(
      RelNode rel,
      List<RexInputRef> columnRefs) {
    ImmutableBitSet.Builder colMask = ImmutableBitSet.builder();
    for (RexInputRef columnRef : columnRefs) {
      colMask.set(columnRef.getIndex());
    }
    return RelMetadataQuery.areColumnsUnique(rel, colMask.build());
  }

  public static boolean areColumnsDefinitelyUnique(RelNode rel,
      List<RexInputRef> columnRefs) {
    Boolean b = areColumnsUnique(rel, columnRefs);
    return b != null && b;
  }

  /**
   * Returns true if the columns represented in a bit mask are definitely
   * known to form a unique column set, when nulls have been filtered from
   * the columns.
   *
   * @param rel     the relational expression that the column mask corresponds
   *                to
   * @param colMask bit mask containing columns that will be tested for
   *                uniqueness
   * @return true if bit mask represents a unique column set; false if not (or
   * if no metadata is available)
   */
  public static boolean areColumnsDefinitelyUniqueWhenNullsFiltered(RelNode rel,
      ImmutableBitSet colMask) {
    Boolean b = RelMetadataQuery.areColumnsUnique(rel, colMask, true);
    if (b == null) {
      return false;
    }
    return b;
  }

  public static Boolean areColumnsUniqueWhenNullsFiltered(
      RelNode rel,
      List<RexInputRef> columnRefs) {
    ImmutableBitSet.Builder colMask = ImmutableBitSet.builder();

    for (RexInputRef columnRef : columnRefs) {
      colMask.set(columnRef.getIndex());
    }

    return RelMetadataQuery.areColumnsUnique(rel, colMask.build(), true);
  }

  public static boolean areColumnsDefinitelyUniqueWhenNullsFiltered(
      RelNode rel,
      List<RexInputRef> columnRefs) {
    Boolean b = areColumnsUniqueWhenNullsFiltered(rel, columnRefs);
    if (b == null) {
      return false;
    }
    return b;
  }

  /**
   * Separates a bit-mask representing a join into masks representing the left
   * and right inputs into the join.
   *
   * @param groupKey      original bit-mask
   * @param leftMask      left bit-mask to be set
   * @param rightMask     right bit-mask to be set
   * @param nFieldsOnLeft number of fields in the left input
   */
  public static void setLeftRightBitmaps(
      ImmutableBitSet groupKey,
      ImmutableBitSet.Builder leftMask,
      ImmutableBitSet.Builder rightMask,
      int nFieldsOnLeft) {
    for (int bit : groupKey) {
      if (bit < nFieldsOnLeft) {
        leftMask.set(bit);
      } else {
        rightMask.set(bit - nFieldsOnLeft);
      }
    }
  }

  /**
   * Returns the number of distinct values provided numSelected are selected
   * where there are domainSize distinct values.
   *
   * <p>Note that in the case where domainSize == numSelected, it's not true
   * that the return value should be domainSize. If you pick 100 random values
   * between 1 and 100, you'll most likely end up with fewer than 100 distinct
   * values, because you'll pick some values more than once.
   *
   * @param domainSize  number of distinct values in the domain
   * @param numSelected number selected from the domain
   * @return number of distinct values for subset selected
   */
  public static Double numDistinctVals(
      Double domainSize,
      Double numSelected) {
    if ((domainSize == null) || (numSelected == null)) {
      return null;
    }

    // Cap the input sizes at MAX_VALUE to ensure that the calculations
    // using these values return meaningful values
    double dSize = capInfinity(domainSize);
    double numSel = capInfinity(numSelected);

    // The formula for this is:
    // 1. Assume we pick 80 random values between 1 and 100.
    // 2. The chance we skip any given value is .99 ^ 80
    // 3. Thus on average we will skip .99 ^ 80 percent of the values
    //    in the domain
    // 4. Generalized, we skip ( (n-1)/n ) ^ k values where n is the
    //    number of possible values and k is the number we are selecting
    // 5. This can be rewritten via approximation (if you want to
    //    know why approximation is called for here, ask Bill Keese):
    //  ((n-1)/n) ^ k
    //  = e ^ ln( ((n-1)/n) ^ k )
    //  = e ^ (k * ln ((n-1)/n))
    //  = e ^ (k * ln (1-1/n))
    // ~= e ^ (k * (-1/n))  because ln(1+x) ~= x for small x
    //  = e ^ (-k/n)
    // 6. Flipping it from number skipped to number visited, we get:
    double res =
        (dSize > 0) ? ((1.0 - Math.exp(-1 * numSel / dSize)) * dSize) : 0;

    // fix the boundary cases
    if (res > dSize) {
      res = dSize;
    }

    if (res > numSel) {
      res = numSel;
    }

    if (res < 0) {
      res = 0;
    }

    return res;
  }

  /**
   * Caps a double value at Double.MAX_VALUE if it's currently infinity
   *
   * @param d the Double object
   * @return the double value if it's not infinity; else Double.MAX_VALUE
   */
  public static double capInfinity(Double d) {
    return d.isInfinite() ? Double.MAX_VALUE : d;
  }

  /**
   * Returns default estimates for selectivities, in the absence of stats.
   *
   * @param predicate predicate for which selectivity will be computed; null
   *                  means true, so gives selectity of 1.0
   * @return estimated selectivity
   */
  public static double guessSelectivity(RexNode predicate) {
    return guessSelectivity(predicate, false);
  }

  /**
   * Returns default estimates for selectivities, in the absence of stats.
   *
   * @param predicate      predicate for which selectivity will be computed;
   *                       null means true, so gives selectity of 1.0
   * @param artificialOnly return only the selectivity contribution from
   *                       artificial nodes
   * @return estimated selectivity
   */
  public static double guessSelectivity(
      RexNode predicate,
      boolean artificialOnly) {
    double sel = 1.0;
    if ((predicate == null) || predicate.isAlwaysTrue()) {
      return sel;
    }

    double artificialSel = 1.0;

    for (RexNode pred : RelOptUtil.conjunctions(predicate)) {
      if (pred.getKind() == SqlKind.IS_NOT_NULL) {
        sel *= .9;
      } else if (
          (pred instanceof RexCall)
              && (((RexCall) pred).getOperator()
              == RelMdUtil.ARTIFICIAL_SELECTIVITY_FUNC)) {
        artificialSel *= RelMdUtil.getSelectivityValue(pred);
      } else if (pred.isA(SqlKind.EQUALS)) {
        sel *= .15;
      } else if (pred.isA(SqlKind.COMPARISON)) {
        sel *= .5;
      } else {
        sel *= .25;
      }
    }

    if (artificialOnly) {
      return artificialSel;
    } else {
      return sel * artificialSel;
    }
  }

  /**
   * AND's two predicates together, either of which may be null, removing
   * redundant filters.
   *
   * @param rexBuilder rexBuilder used to construct AND'd RexNode
   * @param pred1      first predicate
   * @param pred2      second predicate
   * @return AND'd predicate or individual predicates if one is null
   */
  public static RexNode unionPreds(
      RexBuilder rexBuilder,
      RexNode pred1,
      RexNode pred2) {
    final List<RexNode> unionList = new ArrayList<RexNode>();
    final Set<String> strings = new HashSet<String>();

    for (RexNode rex : RelOptUtil.conjunctions(pred1)) {
      if (strings.add(rex.toString())) {
        unionList.add(rex);
      }
    }
    for (RexNode rex2 : RelOptUtil.conjunctions(pred2)) {
      if (strings.add(rex2.toString())) {
        unionList.add(rex2);
      }
    }

    return RexUtil.composeConjunction(rexBuilder, unionList, true);
  }

  /**
   * Takes the difference between two predicates, removing from the first any
   * predicates also in the second
   *
   * @param rexBuilder rexBuilder used to construct AND'd RexNode
   * @param pred1      first predicate
   * @param pred2      second predicate
   * @return MINUS'd predicate list
   */
  public static RexNode minusPreds(
      RexBuilder rexBuilder,
      RexNode pred1,
      RexNode pred2) {
    List<RexNode> list1 = RelOptUtil.conjunctions(pred1);
    List<RexNode> list2 = RelOptUtil.conjunctions(pred2);
    List<RexNode> minusList = new ArrayList<RexNode>();

    for (RexNode rex1 : list1) {
      boolean add = true;
      for (RexNode rex2 : list2) {
        if (rex2.toString().compareTo(rex1.toString()) == 0) {
          add = false;
          break;
        }
      }
      if (add) {
        minusList.add(rex1);
      }
    }

    return RexUtil.composeConjunction(rexBuilder, minusList, true);
  }

  /**
   * Takes a bitmap representing a set of input references and extracts the
   * ones that reference the group by columns in an aggregate.
   *
   * @param groupKey the original bitmap
   * @param aggRel   the aggregate
   * @param childKey sets bits from groupKey corresponding to group by columns
   */
  public static void setAggChildKeys(
      ImmutableBitSet groupKey,
      Aggregate aggRel,
      ImmutableBitSet.Builder childKey) {
    List<AggregateCall> aggCalls = aggRel.getAggCallList();
    for (int bit : groupKey) {
      if (bit < aggRel.getGroupCount()) {
        // group by column
        childKey.set(bit);
      } else {
        // aggregate column -- set a bit for each argument being
        // aggregated
        AggregateCall agg = aggCalls.get(bit
            - (aggRel.getGroupCount() + aggRel.getIndicatorCount()));
        for (Integer arg : agg.getArgList()) {
          childKey.set(arg);
        }
      }
    }
  }

  /**
   * Forms two bitmaps by splitting the columns in a bitmap according to
   * whether or not the column references the child input or is an expression
   * @param projExprs Project expressions
   * @param groupKey  Bitmap whose columns will be split
   * @param baseCols  Bitmap representing columns from the child input
   * @param projCols  Bitmap representing non-child columns
   */
  public static void splitCols(
      List<RexNode> projExprs,
      ImmutableBitSet groupKey,
      ImmutableBitSet.Builder baseCols,
      ImmutableBitSet.Builder projCols) {
    for (int bit : groupKey) {
      final RexNode e = projExprs.get(bit);
      if (e instanceof RexInputRef) {
        baseCols.set(((RexInputRef) e).getIndex());
      } else {
        projCols.set(bit);
      }
    }
  }

  /**
   * Computes the cardinality of a particular expression from the projection
   * list
   *
   * @param rel  RelNode corresponding to the project
   * @param expr projection expression
   * @return cardinality
   */
  public static Double cardOfProjExpr(Project rel, RexNode expr) {
    return expr.accept(new CardOfProjExpr(rel));
  }

  /**
   * Computes the population size for a set of keys returned from a join
   *
   * @param joinRel  the join rel
   * @param groupKey keys to compute the population for
   * @return computed population size
   */
  public static Double getJoinPopulationSize(
      RelNode joinRel,
      ImmutableBitSet groupKey) {
    ImmutableBitSet.Builder leftMask = ImmutableBitSet.builder();
    ImmutableBitSet.Builder rightMask = ImmutableBitSet.builder();
    RelNode left = joinRel.getInputs().get(0);
    RelNode right = joinRel.getInputs().get(1);

    // separate the mask into masks for the left and right
    RelMdUtil.setLeftRightBitmaps(
        groupKey,
        leftMask,
        rightMask,
        left.getRowType().getFieldCount());

    Double population =
        NumberUtil.multiply(
            RelMetadataQuery.getPopulationSize(
                left,
                leftMask.build()),
            RelMetadataQuery.getPopulationSize(
                right,
                rightMask.build()));

    return RelMdUtil.numDistinctVals(
        population,
        RelMetadataQuery.getRowCount(joinRel));
  }

  /**
   * Computes the number of distinct rows for a set of keys returned from a
   * join. Also known as NDV (number of distinct values).
   *
   * @param joinRel   RelNode representing the join
   * @param joinType  type of join
   * @param groupKey  keys that the distinct row count will be computed for
   * @param predicate join predicate
   * @param useMaxNdv If true use formula <code>max(left NDV, right NDV)</code>,
   *                  otherwise use <code>left NDV * right NDV</code>.
   * @return number of distinct rows
   */
  public static Double getJoinDistinctRowCount(
      RelNode joinRel,
      JoinRelType joinType,
      ImmutableBitSet groupKey,
      RexNode predicate,
      boolean useMaxNdv) {
    Double distRowCount;
    ImmutableBitSet.Builder leftMask = ImmutableBitSet.builder();
    ImmutableBitSet.Builder rightMask = ImmutableBitSet.builder();
    RelNode left = joinRel.getInputs().get(0);
    RelNode right = joinRel.getInputs().get(1);

    RelMdUtil.setLeftRightBitmaps(
        groupKey,
        leftMask,
        rightMask,
        left.getRowType().getFieldCount());

    // determine which filters apply to the left vs right
    RexNode leftPred = null;
    RexNode rightPred = null;
    if (predicate != null) {
      List<RexNode> leftFilters = new ArrayList<RexNode>();
      List<RexNode> rightFilters = new ArrayList<RexNode>();
      List<RexNode> joinFilters = new ArrayList<RexNode>();
      List<RexNode> predList = RelOptUtil.conjunctions(predicate);

      RelOptUtil.classifyFilters(
          joinRel,
          predList,
          joinType,
          joinType == JoinRelType.INNER,
          !joinType.generatesNullsOnLeft(),
          !joinType.generatesNullsOnRight(),
          joinFilters,
          leftFilters,
          rightFilters);

      RexBuilder rexBuilder = joinRel.getCluster().getRexBuilder();
      leftPred =
          RexUtil.composeConjunction(rexBuilder, leftFilters, true);
      rightPred =
          RexUtil.composeConjunction(rexBuilder, rightFilters, true);
    }

    if (useMaxNdv) {
      distRowCount = Math.max(
          RelMetadataQuery.getDistinctRowCount(left, leftMask.build(),
              leftPred),
          RelMetadataQuery.getDistinctRowCount(right, rightMask.build(),
              rightPred));
    } else {
      distRowCount =
        NumberUtil.multiply(
            RelMetadataQuery.getDistinctRowCount(
                left,
                leftMask.build(),
                leftPred),
            RelMetadataQuery.getDistinctRowCount(
                right,
                rightMask.build(),
                rightPred));
    }

    return RelMdUtil.numDistinctVals(
        distRowCount,
        RelMetadataQuery.getRowCount(joinRel));
  }

  //~ Inner Classes ----------------------------------------------------------

  /** Visitor that walks over a scalar expression and computes the
   * cardinality of its result. */
  private static class CardOfProjExpr extends RexVisitorImpl<Double> {
    private Project rel;

    public CardOfProjExpr(Project rel) {
      super(true);
      this.rel = rel;
    }

    public Double visitInputRef(RexInputRef var) {
      int index = var.getIndex();
      ImmutableBitSet col = ImmutableBitSet.of(index);
      Double distinctRowCount =
          RelMetadataQuery.getDistinctRowCount(
              rel.getInput(),
              col,
              null);
      if (distinctRowCount == null) {
        return null;
      } else {
        return RelMdUtil.numDistinctVals(
            distinctRowCount,
            RelMetadataQuery.getRowCount(rel));
      }
    }

    public Double visitLiteral(RexLiteral literal) {
      return RelMdUtil.numDistinctVals(
          1.0,
          RelMetadataQuery.getRowCount(rel));
    }

    public Double visitCall(RexCall call) {
      Double distinctRowCount;
      Double rowCount = RelMetadataQuery.getRowCount(rel);
      if (call.isA(SqlKind.MINUS_PREFIX)) {
        distinctRowCount =
            cardOfProjExpr(rel, call.getOperands().get(0));
      } else if (call.isA(ImmutableList.of(SqlKind.PLUS, SqlKind.MINUS))) {
        Double card0 = cardOfProjExpr(rel, call.getOperands().get(0));
        if (card0 == null) {
          return null;
        }
        Double card1 = cardOfProjExpr(rel, call.getOperands().get(1));
        if (card1 == null) {
          return null;
        }
        distinctRowCount = Math.max(card0, card1);
      } else if (call.isA(
          ImmutableList.of(SqlKind.TIMES, SqlKind.DIVIDE))) {
        distinctRowCount =
            NumberUtil.multiply(
                cardOfProjExpr(rel, call.getOperands().get(0)),
                cardOfProjExpr(rel, call.getOperands().get(1)));

        // TODO zfong 6/21/06 - Broadbase has code to handle date
        // functions like year, month, day; E.g., cardinality of Month()
        // is 12
      } else {
        if (call.getOperands().size() == 1) {
          distinctRowCount =
              cardOfProjExpr(
                  rel,
                  call.getOperands().get(0));
        } else {
          distinctRowCount = rowCount / 10;
        }
      }

      return numDistinctVals(distinctRowCount, rowCount);
    }
  }

  /** Returns whether a relational expression is already sorted and has fewer
   * rows than the sum of offset and limit.
   *
   * <p>If this is the case, it is safe to push down a
   * {@link org.apache.calcite.rel.core.Sort} with limit and optional offset. */
  public static boolean checkInputForCollationAndLimit(RelNode input,
      RelCollation collation, RexNode offset, RexNode fetch) {
    // Check if the input is already sorted
    ImmutableList<RelCollation> inputCollation =
        RelMetadataQuery.collations(input);
    final boolean alreadySorted = RelCollations.equal(
        ImmutableList.of(collation), inputCollation);
    // Check if we are not reducing the number of tuples
    boolean alreadySmaller = true;
    final Double rowCount = RelMetadataQuery.getMaxRowCount(input);
    if (rowCount != null && fetch != null) {
      final int offsetVal = offset == null ? 0 : RexLiteral.intValue(offset);
      final int limit = RexLiteral.intValue(fetch);
      if ((double) offsetVal + (double) limit < rowCount) {
        alreadySmaller = false;
      }
    }
    return alreadySorted && alreadySmaller;
  }
}

// End RelMdUtil.java
