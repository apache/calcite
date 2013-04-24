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
package org.eigenbase.rel.metadata;

import java.math.*;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.rules.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util14.*;


/**
 * RelMdUtil provides utility methods used by the metadata provider methods.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class RelMdUtil
{
    //~ Static fields/initializers ---------------------------------------------

    public static final SqlFunction artificialSelectivityFunc =
        new SqlFunction(
            "ARTIFICIAL_SELECTIVITY",
            SqlKind.OTHER_FUNCTION,
            SqlTypeStrategies.rtiBoolean, // returns boolean since we'll AND it
            null,
            SqlTypeStrategies.otcNumeric, // takes a numeric param
            SqlFunctionCategory.System);

    //~ Methods ----------------------------------------------------------------

    /**
     * Creates a RexNode that stores a selectivity value corresponding to the
     * selectivity of a semijoin. This can be added to a filter to simulate the
     * effect of the semijoin during costing, but should never appear in a real
     * plan since it has no physical implementation.
     *
     * @param rel the semijoin of interest
     *
     * @return constructed rexnode
     */
    public static RexNode makeSemiJoinSelectivityRexNode(SemiJoinRel rel)
    {
        RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
        double selectivity =
            computeSemiJoinSelectivity(
                rel.getLeft(),
                rel.getRight(),
                rel);
        RexNode selec =
            rexBuilder.makeApproxLiteral(new BigDecimal(selectivity));
        return rexBuilder.makeCall(artificialSelectivityFunc, selec);
    }

    /**
     * Returns the selectivity value stored in the rexnode
     *
     * @param artificialSelecFuncNode rexnode containing the selectivity value
     *
     * @return selectivity value
     */
    public static double getSelectivityValue(RexNode artificialSelecFuncNode)
    {
        assert (artificialSelecFuncNode instanceof RexCall);
        RexCall call = (RexCall) artificialSelecFuncNode;
        assert (call.getOperator() == artificialSelectivityFunc);
        RexNode operand = call.getOperands()[0];
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
     *
     * @return calculated selectivity
     */
    public static double computeSemiJoinSelectivity(
        SemiJoinRel rel)
    {
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
     * @param dimRel dimension table participating in the semijoin
     * @param rel semijoin rel
     *
     * @return calculated selectivity
     */
    public static double computeSemiJoinSelectivity(
        RelNode factRel,
        RelNode dimRel,
        SemiJoinRel rel)
    {
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
     * @param factRel fact table participating in the semijoin
     * @param dimRel dimension table participating in the semijoin
     * @param factKeyList LHS keys used in the filter
     * @param dimKeyList RHS keys used in the filter
     *
     * @return calculated selectivity
     */
    public static double computeSemiJoinSelectivity(
        RelNode factRel,
        RelNode dimRel,
        List<Integer> factKeyList,
        List<Integer> dimKeyList)
    {
        BitSet factKeys = new BitSet();
        for (int factCol : factKeyList) {
            factKeys.set(factCol);
        }
        BitSet dimKeys = new BitSet();
        for (int dimCol : dimKeyList) {
            dimKeys.set(dimCol);
        }

        Double factPop = RelMetadataQuery.getPopulationSize(factRel, factKeys);
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
     * @param rel the relnode that the column mask correponds to
     * @param colMask bit mask containing columns that will be tested for
     * uniqueness
     *
     * @return true if bit mask represents a unique column set; false if not (or
     * if no metadata is available)
     */
    public static boolean areColumnsDefinitelyUnique(
        RelNode rel,
        BitSet colMask)
    {
        Boolean b = RelMetadataQuery.areColumnsUnique(rel, colMask);
        if (b == null) {
            return false;
        }
        return b;
    }

    public static Boolean areColumnsUnique(
        RelNode rel,
        List<RexInputRef> columnRefs)
    {
        BitSet colMask = new BitSet();

        for (int i = 0; i < columnRefs.size(); i++) {
            colMask.set(columnRefs.get(i).getIndex());
        }

        return RelMetadataQuery.areColumnsUnique(rel, colMask);
    }

    public static boolean areColumnsDefinitelyUnique(
        RelNode rel,
        List<RexInputRef> columnRefs)
    {
        Boolean b = areColumnsUnique(rel, columnRefs);
        if (b == null) {
            return false;
        }
        return b;
    }

    /**
     * Returns true if the columns represented in a bit mask are definitely
     * known to form a unique column set, when nulls have been filtered from
     * the columns.
     *
     * @param rel the relnode that the column mask correponds to
     * @param colMask bit mask containing columns that will be tested for
     * uniqueness
     *
     * @return true if bit mask represents a unique column set; false if not (or
     * if no metadata is available)
     */
    public static boolean areColumnsDefinitelyUniqueWhenNullsFiltered(
        RelNode rel,
        BitSet colMask)
    {
        Boolean b =
            RelMetadataQuery.areColumnsUnique(rel, colMask, true);
        if (b == null) {
            return false;
        }
        return b;
    }

    public static Boolean areColumnsUniqueWhenNullsFiltered(
        RelNode rel,
        List<RexInputRef> columnRefs)
    {
        BitSet colMask = new BitSet();

        for (int i = 0; i < columnRefs.size(); i++) {
            colMask.set(columnRefs.get(i).getIndex());
        }

        return RelMetadataQuery.areColumnsUnique(rel, colMask, true);
    }

    public static boolean areColumnsDefinitelyUniqueWhenNullsFiltered(
        RelNode rel,
        List<RexInputRef> columnRefs)
    {
        Boolean b = areColumnsUniqueWhenNullsFiltered(rel, columnRefs);
        if (b == null) {
            return false;
        }
        return b;
    }

    /**
     * Sets a bitmap corresponding to a list of keys.
     *
     * @param keys list of keys
     *
     * @return the bitmap
     */
    public static BitSet setBitKeys(List<Integer> keys)
    {
        BitSet bits = new BitSet();
        for (Integer key : keys) {
            bits.set(key);
        }
        return bits;
    }

    /**
     * Separates a bitmask representing a join into masks representing the left
     * and right inputs into the join
     *
     * @param groupKey original bitmask
     * @param leftMask left bitmask to be set
     * @param rightMask right bitmask to be set
     * @param nFieldsOnLeft number of fields in the left input
     */
    public static void setLeftRightBitmaps(
        BitSet groupKey,
        BitSet leftMask,
        BitSet rightMask,
        int nFieldsOnLeft)
    {
        for (
            int bit = groupKey.nextSetBit(0);
            bit >= 0;
            bit = groupKey.nextSetBit(bit + 1))
        {
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
     * @param domainSize number of distinct values in the domain
     * @param numSelected number selected from the domain
     *
     * @return number of distinct values for subset selected
     */
    public static Double numDistinctVals(
        Double domainSize,
        Double numSelected)
    {
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
     *
     * @return the double value if it's not infinity; else Double.MAX_VALUE
     */
    public static double capInfinity(Double d)
    {
        return (d.isInfinite() ? Double.MAX_VALUE : d.doubleValue());
    }

    /**
     * Returns default estimates for selectivities, in the absence of stats.
     *
     * @param predicate predicate for which selectivity will be computed; null
     * means true, so gives selectity of 1.0
     *
     * @return estimated selectivity
     */
    public static double guessSelectivity(RexNode predicate)
    {
        return guessSelectivity(predicate, false);
    }

    /**
     * Returns default estimates for selectivities, in the absence of stats.
     *
     * @param predicate predicate for which selectivity will be computed; null
     * means true, so gives selectity of 1.0
     * @param artificialOnly return only the selectivity contribution from
     * artificial nodes
     *
     * @return estimated selectivity
     */
    public static double guessSelectivity(
        RexNode predicate,
        boolean artificialOnly)
    {
        double sel = 1.0;
        if ((predicate == null) || predicate.isAlwaysTrue()) {
            return sel;
        }

        double artificialSel = 1.0;

        List<RexNode> predList = new ArrayList<RexNode>();
        RelOptUtil.decomposeConjunction(predicate, predList);

        for (RexNode pred : predList) {
            if ((pred instanceof RexCall)
                && (((RexCall) pred).getOperator()
                    == SqlStdOperatorTable.isNotNullOperator))
            {
                sel *= .9;
            } else if (
                (pred instanceof RexCall)
                && (((RexCall) pred).getOperator()
                    == RelMdUtil.artificialSelectivityFunc))
            {
                artificialSel *= RelMdUtil.getSelectivityValue(pred);
            } else if (pred.isA(RexKind.Equals)) {
                sel *= .15;
            } else if (pred.isA(RexKind.Comparison)) {
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
     * Locates the columns corresponding to equijoins within a joinrel.
     *
     * @param leftChild left input into the join
     * @param rightChild right input into the join
     * @param predicate join predicate
     * @param leftJoinCols bitmap that will be set with the columns on the LHS
     * of the join that participate in equijoins
     * @param rightJoinCols bitmap that will be set with the columns on the RHS
     * of the join that participate in equijoins
     *
     * @return remaining join filters that are not equijoins; may return a
     * {@link RexLiteral} true, but never null
     */
    public static RexNode findEquiJoinCols(
        RelNode leftChild,
        RelNode rightChild,
        RexNode predicate,
        BitSet leftJoinCols,
        BitSet rightJoinCols)
    {
        // locate the equijoin conditions
        List<Integer> leftKeys = new ArrayList<Integer>();
        List<Integer> rightKeys = new ArrayList<Integer>();
        RexNode nonEquiJoin =
            RelOptUtil.splitJoinCondition(
                leftChild,
                rightChild,
                predicate,
                leftKeys,
                rightKeys);
        assert nonEquiJoin != null;

        // mark the columns referenced on each side of the equijoin filters
        for (int i = 0; i < leftKeys.size(); i++) {
            leftJoinCols.set(leftKeys.get(i));
            rightJoinCols.set(rightKeys.get(i));
        }

        return nonEquiJoin;
    }

    /**
     * AND's two predicates together, either of which may be null, removing
     * redundant filters.
     *
     * @param rexBuilder rexBuilder used to construct AND'd RexNode
     * @param pred1 first predicate
     * @param pred2 second predicate
     *
     * @return AND'd predicate or individual predicates if one is null
     */
    public static RexNode unionPreds(
        RexBuilder rexBuilder,
        RexNode pred1,
        RexNode pred2)
    {
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

        return RexUtil.andRexNodeList(rexBuilder, unionList);
    }

    /**
     * Takes the difference between two predicates, removing from the first any
     * predicates also in the second
     *
     * @param rexBuilder rexBuilder used to construct AND'd RexNode
     * @param pred1 first predicate
     * @param pred2 second predicate
     *
     * @return MINUS'd predicate list
     */
    public static RexNode minusPreds(
        RexBuilder rexBuilder,
        RexNode pred1,
        RexNode pred2)
    {
        List<RexNode> list1 = new ArrayList<RexNode>();
        List<RexNode> list2 = new ArrayList<RexNode>();
        List<RexNode> minusList = new ArrayList<RexNode>();
        RelOptUtil.decomposeConjunction(pred1, list1);
        RelOptUtil.decomposeConjunction(pred2, list2);

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

        return RexUtil.andRexNodeList(rexBuilder, minusList);
    }

    /**
     * Takes a bitmap representing a set of input references and extracts the
     * ones that reference the group by columns in an aggregate
     *
     * @param groupKey the original bitmap
     * @param aggRel the aggregate
     * @param childKey sets bits from groupKey corresponding to group by columns
     */
    public static void setAggChildKeys(
        BitSet groupKey,
        AggregateRelBase aggRel,
        BitSet childKey)
    {
        List<AggregateCall> aggCalls = aggRel.getAggCallList();
        for (
            int bit = groupKey.nextSetBit(0);
            bit >= 0;
            bit = groupKey.nextSetBit(bit + 1))
        {
            if (bit < aggRel.getGroupCount()) {
                // group by column
                childKey.set(bit);
            } else {
                // aggregate column -- set a bit for each argument being
                // aggregated
                AggregateCall agg = aggCalls.get(bit - aggRel.getGroupCount());
                for (Integer arg : agg.getArgList()) {
                    childKey.set(arg);
                }
            }
        }
    }

    /**
     * Forms two bitmaps by splitting the columns in a bitmap according to
     * whether or not the column references the child input or is an expression
     *
     * @param projExprs Project expressions
     * @param groupKey Bitmap whose columns will be split
     * @param baseCols Bitmap representing columns from the child input
     * @param projCols Bitmap representing non-child columns
     */
    public static void splitCols(
        RexNode [] projExprs,
        BitSet groupKey,
        BitSet baseCols,
        BitSet projCols)
    {
        for (
            int bit = groupKey.nextSetBit(0);
            bit >= 0;
            bit = groupKey.nextSetBit(bit + 1))
        {
            if (projExprs[bit] instanceof RexInputRef) {
                baseCols.set(((RexInputRef) projExprs[bit]).getIndex());
            } else {
                projCols.set(bit);
            }
        }
    }

    /**
     * Computes the cardinality of a particular expression from the projection
     * list
     *
     * @param rel RelNode corresponding to the project
     * @param expr projection expression
     *
     * @return cardinality
     */
    public static Double cardOfProjExpr(ProjectRelBase rel, RexNode expr)
    {
        return expr.accept(new CardOfProjExpr(rel));
    }

    /**
     * Computes the population size for a set of keys returned from a join
     *
     * @param joinRel the join rel
     * @param groupKey keys to compute the population for
     *
     * @return computed population size
     */
    public static Double getJoinPopulationSize(
        RelNode joinRel,
        BitSet groupKey)
    {
        BitSet leftMask = new BitSet();
        BitSet rightMask = new BitSet();
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
                    leftMask),
                RelMetadataQuery.getPopulationSize(
                    right,
                    rightMask));

        return RelMdUtil.numDistinctVals(
            population,
            RelMetadataQuery.getRowCount(joinRel));
    }

    /**
     * Computes the number of distinct rows for a set of keys returned from a
     * join
     *
     * @param joinRel RelNode representing the join
     * @param joinType type of join
     * @param groupKey keys that the distinct row count will be computed for
     * @param predicate join predicate
     *
     * @return number of distinct rows
     */
    public static Double getJoinDistinctRowCount(
        RelNode joinRel,
        JoinRelType joinType,
        BitSet groupKey,
        RexNode predicate)
    {
        Double distRowCount;
        BitSet leftMask = new BitSet();
        BitSet rightMask = new BitSet();
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
            List<RexNode> predList = new ArrayList<RexNode>();
            RelOptUtil.decomposeConjunction(predicate, predList);

            RelOptUtil.classifyFilters(
                joinRel,
                predList,
                (joinType == JoinRelType.INNER),
                !joinType.generatesNullsOnLeft(),
                !joinType.generatesNullsOnRight(),
                joinFilters,
                leftFilters,
                rightFilters);

            RexBuilder rexBuilder = joinRel.getCluster().getRexBuilder();
            leftPred = RexUtil.andRexNodeList(rexBuilder, leftFilters);
            rightPred = RexUtil.andRexNodeList(rexBuilder, rightFilters);
        }

        distRowCount =
            NumberUtil.multiply(
                RelMetadataQuery.getDistinctRowCount(
                    left,
                    leftMask,
                    leftPred),
                RelMetadataQuery.getDistinctRowCount(
                    right,
                    rightMask,
                    rightPred));

        return RelMdUtil.numDistinctVals(
            distRowCount,
            RelMetadataQuery.getRowCount(joinRel));
    }

    //~ Inner Classes ----------------------------------------------------------

    private static class CardOfProjExpr
        extends RexVisitorImpl<Double>
    {
        private ProjectRelBase rel;

        public CardOfProjExpr(ProjectRelBase rel)
        {
            super(true);
            this.rel = rel;
        }

        public Double visitInputRef(RexInputRef var)
        {
            int index = var.getIndex();
            BitSet col = new BitSet(index);
            col.set(index);
            Double distinctRowCount =
                RelMetadataQuery.getDistinctRowCount(
                    rel.getChild(),
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

        public Double visitLiteral(RexLiteral literal)
        {
            return RelMdUtil.numDistinctVals(
                1.0,
                RelMetadataQuery.getRowCount(rel));
        }

        public Double visitCall(RexCall call)
        {
            Double distinctRowCount;
            Double rowCount = RelMetadataQuery.getRowCount(rel);
            if (call.isA(RexKind.MinusPrefix)) {
                distinctRowCount = cardOfProjExpr(rel, call.getOperands()[0]);
            } else if (call.isA(RexKind.Plus) || call.isA(RexKind.Minus)) {
                Double card0 = cardOfProjExpr(rel, call.getOperands()[0]);
                if (card0 == null) {
                    return null;
                }
                Double card1 = cardOfProjExpr(rel, call.getOperands()[1]);
                if (card1 == null) {
                    return null;
                }
                distinctRowCount = Math.max(card0, card1);
            } else if (call.isA(RexKind.Times) || call.isA(RexKind.Divide)) {
                distinctRowCount =
                    NumberUtil.multiply(
                        cardOfProjExpr(rel, call.getOperands()[0]),
                        cardOfProjExpr(rel, call.getOperands()[1]));

                // TODO zfong 6/21/06 - Broadbase has code to handle date
                // functions like year, month, day; E.g., cardinality of Month()
                // is 12
            } else {
                if (call.getOperands().length == 1) {
                    distinctRowCount =
                        cardOfProjExpr(
                            rel,
                            call.getOperands()[0]);
                } else {
                    distinctRowCount = rowCount / 10;
                }
            }

            return numDistinctVals(distinctRowCount, rowCount);
        }
    }
}

// End RelMdUtil.java
