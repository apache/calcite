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
package net.hydromatic.optiq.impl.jdbc;

import net.hydromatic.linq4j.Ord;
import net.hydromatic.linq4j.expressions.*;

import net.hydromatic.optiq.ModifiableTable;
import net.hydromatic.optiq.prepare.Prepare;

import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.ConverterRule;
import org.eigenbase.rel.metadata.RelMetadataQuery;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.*;
import org.eigenbase.sql.util.SqlBuilder;
import org.eigenbase.sql.util.SqlString;
import org.eigenbase.trace.EigenbaseTrace;
import org.eigenbase.util.ImmutableIntList;
import org.eigenbase.util.Pair;
import org.eigenbase.util.Util;

import java.util.*;
import java.util.logging.Logger;

/**
 * Rules and relational operators for
 * {@link JdbcConvention}
 * calling convention.
 */
public class JdbcRules {
    protected static final Logger tracer = EigenbaseTrace.getPlannerTracer();

    public static List<RelOptRule> rules(JdbcConvention out) {
        return Arrays.<RelOptRule>asList(
            new JdbcToEnumerableConverterRule(out),
            new JdbcJoinRule(out),
            new JdbcCalcRule(out),
            new JdbcAggregateRule(out),
            new JdbcSortRule(out),
            new JdbcUnionRule(out),
            new JdbcIntersectRule(out),
            new JdbcMinusRule(out),
            new JdbcTableModificationRule(out),
            new JdbcValuesRule(out));
    }

    private static void alias(SqlBuilder buf, String field, String s) {
        if (field == null || !field.equals(s)) {
            buf.append(" AS ").identifier(s);
        }
    }

    static abstract class JdbcConverterRule extends ConverterRule {
        protected final JdbcConvention out;

        public JdbcConverterRule(
            Class<? extends RelNode> clazz,
            RelTrait in,
            JdbcConvention out,
            String description)
        {
            super(clazz, in, out, description);
            this.out = out;
        }
    }

    private static class JdbcJoinRule extends JdbcConverterRule {
        private JdbcJoinRule(JdbcConvention out) {
            super(
                JoinRel.class,
                Convention.NONE,
                out,
                "JdbcJoinRule");
        }

        @Override
        public RelNode convert(RelNode rel) {
            JoinRel join = (JoinRel) rel;
            List<RelNode> newInputs = new ArrayList<RelNode>();
            for (RelNode input : join.getInputs()) {
                if (!(input.getConvention() == getOutTrait())) {
                    input =
                        convert(
                            input,
                            input.getTraitSet()
                                .replace(out));
                }
                newInputs.add(input);
            }
            try {
                return new JdbcJoinRel(
                    join.getCluster(),
                    join.getTraitSet().replace(out),
                    newInputs.get(0),
                    newInputs.get(1),
                    join.getCondition(),
                    join.getJoinType(),
                    join.getVariablesStopped());
            } catch (InvalidRelException e) {
                tracer.warning(e.toString());
                return null;
            }
        }
    }

    public static class JdbcJoinRel
        extends JoinRelBase
        implements JdbcRel
    {
        final ImmutableIntList leftKeys;
        final ImmutableIntList rightKeys;

        protected JdbcJoinRel(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode left,
            RelNode right,
            RexNode condition,
            JoinRelType joinType,
            Set<String> variablesStopped)
            throws InvalidRelException
        {
            super(
                cluster, traits, left, right, condition, joinType,
                variablesStopped);
            final List<Integer> leftKeys = new ArrayList<Integer>();
            final List<Integer> rightKeys = new ArrayList<Integer>();
            RexNode remaining =
                RelOptUtil.splitJoinCondition(
                    left, right, condition, leftKeys, rightKeys);
            if (!remaining.isAlwaysTrue()) {
                throw new InvalidRelException(
                    "JdbcJoinRel only supports equi-join");
            }
            this.leftKeys = ImmutableIntList.of(leftKeys);
            this.rightKeys = ImmutableIntList.of(rightKeys);
        }

        @Override
        public JdbcJoinRel copy(
            RelTraitSet traitSet,
            RexNode conditionExpr,
            RelNode left,
            RelNode right)
        {
            try {
                return new JdbcJoinRel(
                    getCluster(),
                    traitSet,
                    left,
                    right,
                    conditionExpr,
                    joinType,
                    variablesStopped);
            } catch (InvalidRelException e) {
                // Semantic error not possible. Must be a bug. Convert to
                // internal error.
                throw new AssertionError(e);
            }
        }

        @Override
        public RelOptCost computeSelfCost(RelOptPlanner planner) {
            // We always "build" the
            double rowCount = RelMetadataQuery.getRowCount(this);

            return planner.makeCost(rowCount, 0, 0);
        }

        @Override
        public double getRows() {
            final boolean leftKey = left.isKey(Util.bitSetOf(leftKeys));
            final boolean rightKey = right.isKey(Util.bitSetOf(rightKeys));
            final double leftRowCount = left.getRows();
            final double rightRowCount = right.getRows();
            if (leftKey && rightKey) {
                return Math.min(leftRowCount, rightRowCount);
            }
            if (leftKey) {
                return rightRowCount;
            }
            if (rightKey) {
                return leftRowCount;
            }
            return leftRowCount * rightRowCount;
        }

        public SqlString implement(JdbcImplementor implementor) {
            final SqlBuilder buf = new SqlBuilder(implementor.dialect);
            buf.append("SELECT ");
            int i = 0;
            List<String> fields = getRowType().getFieldNames();
            for (Ord<RelNode> input : Ord.zip(getInputs())) {
                String t = "t" + input.i;
                final List<String> inFields =
                    input.e.getRowType().getFieldNames();
                for (String inField : inFields) {
                    buf.append(i > 0 ? ", " : "");
                    buf.identifier(t, inField);
                    alias(buf, inField, fields.get(i));
                    i++;
                }
            }
            buf.append(" FROM ");
            for (Ord<RelNode> input : Ord.zip(getInputs())) {
                if (input.i > 0) {
                    implementor.newline(buf)
                        .append("JOIN ");
                }
                implementor.subquery(buf, input.i, input.e, "t" + input.i);
            }
            final List<String> leftFields =
                getInput(0).getRowType().getFieldNames();
            final List<String> rightFields =
                getInput(1).getRowType().getFieldNames();
            for (Ord<Pair<Integer, Integer>> pair
                : Ord.zip(Pair.zip(leftKeys, rightKeys)))
            {
                implementor.newline(buf)
                    .append(pair.i == 0 ? "ON " : "AND ")
                    .identifier("t0", leftFields.get(pair.e.left))
                    .append(" = ")
                    .identifier("t1", rightFields.get(pair.e.right));
            }
            return buf.toSqlString();
        }
    }

    /**
     * Rule to convert a {@link CalcRel} to an
     * {@link JdbcCalcRel}.
     */
    private static class JdbcCalcRule
        extends JdbcConverterRule
    {
        private JdbcCalcRule(JdbcConvention out) {
            super(
                CalcRel.class,
                Convention.NONE,
                out,
                "JdbcCalcRule");
        }

        public RelNode convert(RelNode rel)
        {
            final CalcRel calc = (CalcRel) rel;

            // If there's a multiset, let FarragoMultisetSplitter work on it
            // first.
            if (RexMultisetUtil.containsMultiset(calc.getProgram())) {
                return null;
            }

            return new JdbcCalcRel(
                rel.getCluster(),
                rel.getTraitSet().replace(out),
                convert(
                    calc.getChild(),
                    calc.getTraitSet().replace(out)),
                calc.getProgram(),
                ProjectRelBase.Flags.Boxed);
        }
    }

    public static class JdbcCalcRel extends SingleRel implements JdbcRel {
        private final RexProgram program;

        /**
         * Values defined in {@link org.eigenbase.rel.ProjectRelBase.Flags}.
         */
        protected int flags;

        public JdbcCalcRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode child,
            RexProgram program,
            int flags)
        {
            super(cluster, traitSet, child);
            assert getConvention() instanceof JdbcConvention;
            this.flags = flags;
            this.program = program;
            this.rowType = program.getOutputRowType();
        }

        public RelOptPlanWriter explainTerms(RelOptPlanWriter pw) {
            return program.explainCalc(super.explainTerms(pw));
        }

        public double getRows() {
            return FilterRel.estimateFilteredRows(
                getChild(), program);
        }

        public RelOptCost computeSelfCost(RelOptPlanner planner) {
            double dRows = RelMetadataQuery.getRowCount(this);
            double dCpu =
                RelMetadataQuery.getRowCount(getChild())
                * program.getExprCount();
            double dIo = 0;
            return planner.makeCost(dRows, dCpu, dIo);
        }

        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            return new JdbcCalcRel(
                getCluster(),
                traitSet,
                sole(inputs),
                program.copy(),
                getFlags());
        }

        public int getFlags() {
            return flags;
        }

        public RexProgram getProgram() {
            return program;
        }

        public SqlString implement(JdbcImplementor implementor) {
            final SqlBuilder buf = new SqlBuilder(implementor.dialect);
            buf.append("SELECT ");
            if (isStar(program)) {
                buf.append("*");
            } else {
                for (Ord<RexLocalRef> ref : Ord.zip(program.getProjectList())) {
                    buf.append(ref.i == 0 ? "" : ", ");
                    expr(buf, program, ref.e);
                    alias(buf, null, getRowType().getFieldNames().get(ref.i));
                }
            }
            implementor.newline(buf)
                .append("FROM ");
            implementor.subquery(buf, 0, getChild(), "t");
            if (program.getCondition() != null) {
                implementor.newline(buf);
                buf.append("WHERE ");
                expr(buf, program, program.getCondition());
            }
            return buf.toSqlString();
        }

        private static boolean isStar(RexProgram program) {
            int i = 0;
            for (RexLocalRef ref : program.getProjectList()) {
                if (ref.getIndex() != i++) {
                    return false;
                }
            }
            if (i != program.getInputRowType().getFieldCount()) {
                return false;
            }
            return true;
        }

        private static void expr(
            SqlBuilder buf, RexProgram program, RexNode rex)
        {
            if (rex instanceof RexLocalRef) {
                final int index = ((RexLocalRef) rex).getIndex();
                expr(buf, program, program.getExprList().get(index));
            } else if (rex instanceof RexInputRef) {
                buf.identifier(
                    program.getInputRowType().getFieldNames().get(
                        ((RexInputRef) rex).getIndex()));
            } else if (rex instanceof RexLiteral) {
                RexLiteral rexLiteral = (RexLiteral) rex;
                buf.append(rexLiteral.getValue2().toString());
            } else if (rex instanceof RexCall) {
                final RexCall call = (RexCall) rex;
                switch (call.getOperator().getSyntax()) {
                case Binary:
                    expr(buf, program, call.getOperandList().get(0));
                    buf.append(' ')
                        .append(call.getOperator().toString())
                        .append(' ');
                    expr(buf, program, call.getOperandList().get(1));
                    break;
                default:
                    throw new AssertionError(call.getOperator());
                }
            } else {
                throw new AssertionError(rex);
            }
        }
    }

    /**
     * Rule to convert an {@link org.eigenbase.rel.AggregateRel} to an
     * {@link JdbcAggregateRel}.
     */
    private static class JdbcAggregateRule extends JdbcConverterRule {
        private JdbcAggregateRule(JdbcConvention out) {
            super(
                AggregateRel.class,
                Convention.NONE,
                out,
                "JdbcAggregateRule");
        }

        public RelNode convert(RelNode rel) {
            final AggregateRel agg = (AggregateRel) rel;
            final RelTraitSet traitSet =
                agg.getTraitSet().replace(out);
            try {
                return new JdbcAggregateRel(
                    rel.getCluster(),
                    traitSet,
                    convert(agg.getChild(), traitSet),
                    agg.getGroupSet(),
                    agg.getAggCallList());
            } catch (InvalidRelException e) {
                tracer.warning(e.toString());
                return null;
            }
        }
    }

    public static class JdbcAggregateRel
        extends AggregateRelBase
        implements JdbcRel
    {
        public JdbcAggregateRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode child,
            BitSet groupSet,
            List<AggregateCall> aggCalls)
            throws InvalidRelException
        {
            super(cluster, traitSet, child, groupSet, aggCalls);
            assert getConvention() instanceof JdbcConvention;

            for (AggregateCall aggCall : aggCalls) {
                if (aggCall.isDistinct()) {
                    throw new InvalidRelException(
                        "distinct aggregation not supported");
                }
            }
        }

        @Override
        public JdbcAggregateRel copy(
            RelTraitSet traitSet, List<RelNode> inputs)
        {
            try {
                return new JdbcAggregateRel(
                    getCluster(),
                    traitSet,
                    sole(inputs),
                    groupSet,
                    aggCalls);
            } catch (InvalidRelException e) {
                // Semantic error not possible. Must be a bug. Convert to
                // internal error.
                throw new AssertionError(e);
            }
        }

        public SqlString implement(JdbcImplementor implementor) {
            // "select a, b, sum(x) from ( ... ) group by a, b"
            final SqlBuilder buf = new SqlBuilder(implementor.dialect);
            final List<String> inFields =
                getChild().getRowType().getFieldNames();
            final List<String> fields = getRowType().getFieldNames();
            buf.append("SELECT ");
            int i = 0;
            for (int group : Util.toIter(groupSet)) {
                buf.append(i > 0 ? ", " : "");
                final String inField = inFields.get(group);
                buf.identifier(inField);
                alias(buf, inField, fields.get(i));
                i++;
            }
            for (AggregateCall aggCall : aggCalls) {
                buf.append(i > 0 ? ", " : "");
                buf.append(aggCall.getAggregation().getName());
                buf.append("(");
                if (aggCall.getArgList().isEmpty()) {
                    buf.append("*");
                } else {
                    for (Ord<Integer> call : Ord.zip(aggCall.getArgList())) {
                        buf.append(call.i > 0 ? ", " : "");
                        buf.append(inFields.get(call.e));
                    }
                }
                buf.append(")");
                alias(buf, null, fields.get(i));
                i++;
            }
            implementor.newline(buf)
                .append(" FROM ");
            implementor.subquery(buf, 0, getChild(), "t");
            if (!groupSet.isEmpty()) {
                implementor.newline(buf)
                    .append("GROUP BY ");
                i = 0;
                for (int group : Util.toIter(groupSet)) {
                    buf.append(i > 0 ? ", " : "");
                    final String inField = inFields.get(group);
                    buf.identifier(inField);
                    i++;
                }
            }
            return buf.toSqlString();
        }
    }

    /**
     * Rule to convert an {@link org.eigenbase.rel.SortRel} to an
     * {@link JdbcSortRel}.
     */
    private static class JdbcSortRule
        extends JdbcConverterRule
    {
        private JdbcSortRule(JdbcConvention out) {
            super(
                SortRel.class,
                Convention.NONE,
                out,
                "JdbcSortRule");
        }

        public RelNode convert(RelNode rel)
        {
            final SortRel sort = (SortRel) rel;
            final RelTraitSet traitSet =
                sort.getTraitSet().replace(out);
            return new JdbcSortRel(
                rel.getCluster(),
                traitSet,
                convert(sort.getChild(), traitSet),
                sort.getCollations());
        }
    }

    public static class JdbcSortRel
        extends SortRel
        implements JdbcRel
    {
        public JdbcSortRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode child,
            List<RelFieldCollation> collations)
        {
            super(cluster, traitSet, child, collations);
            assert getConvention() instanceof JdbcConvention;
            assert getConvention() == child.getConvention();
        }

        @Override
        public JdbcSortRel copy(
            RelTraitSet traitSet,
            RelNode newInput,
            List<RelFieldCollation> newCollations)
        {
            return new JdbcSortRel(
                getCluster(),
                traitSet,
                newInput,
                newCollations);
        }

        public SqlString implement(JdbcImplementor implementor) {
            throw new AssertionError(); // TODO:
        }
    }

    /**
     * Rule to convert an {@link org.eigenbase.rel.UnionRel} to a
     * {@link JdbcUnionRel}.
     */
    private static class JdbcUnionRule
        extends JdbcConverterRule
    {
        private JdbcUnionRule(JdbcConvention out) {
            super(
                UnionRel.class,
                Convention.NONE,
                out,
                "JdbcUnionRule");
        }

        public RelNode convert(RelNode rel)
        {
            final UnionRel union = (UnionRel) rel;
            final RelTraitSet traitSet =
                union.getTraitSet().replace(out);
            return new JdbcUnionRel(
                rel.getCluster(),
                traitSet,
                convertList(union.getInputs(), traitSet),
                union.all);
        }
    }

    public static class JdbcUnionRel
        extends UnionRelBase
        implements JdbcRel
    {
        public JdbcUnionRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelNode> inputs,
            boolean all)
        {
            super(cluster, traitSet, inputs, all);
        }

        public JdbcUnionRel copy(
            RelTraitSet traitSet, List<RelNode> inputs, boolean all)
        {
            return new JdbcUnionRel(getCluster(), traitSet, inputs, all);
        }

        @Override
        public RelOptCost computeSelfCost(RelOptPlanner planner) {
            return super.computeSelfCost(planner).multiplyBy(.1);
        }

        public SqlString implement(JdbcImplementor implementor) {
            return setOpSql(this, implementor, "UNION");
        }
    }

    private static SqlString setOpSql(
        SetOpRel setOpRel, JdbcImplementor implementor, String op)
    {
        final SqlBuilder buf = new SqlBuilder(implementor.dialect);
        for (Ord<RelNode> input : Ord.zip(setOpRel.getInputs())) {
            if (input.i > 0) {
                implementor.newline(buf)
                    .append(op + (setOpRel.all ? " ALL " : ""));
                implementor.newline(buf);
            }
            buf.append(implementor.visitChild(input.i, input.e));
        }
        return buf.toSqlString();
    }

    /**
     * Rule to convert an {@link org.eigenbase.rel.IntersectRel} to an
     * {@link JdbcIntersectRel}.
     */
    private static class JdbcIntersectRule
        extends JdbcConverterRule
    {
        private JdbcIntersectRule(JdbcConvention out) {
            super(
                IntersectRel.class,
                Convention.NONE,
                out,
                "JdbcIntersectRule");
        }

        public RelNode convert(RelNode rel)
        {
            final IntersectRel intersect = (IntersectRel) rel;
            if (intersect.all) {
                return null; // INTERSECT ALL not implemented
            }
            final RelTraitSet traitSet =
                intersect.getTraitSet().replace(out);
            return new JdbcIntersectRel(
                rel.getCluster(),
                traitSet,
                convertList(intersect.getInputs(), traitSet),
                intersect.all);
        }
    }

    public static class JdbcIntersectRel
        extends IntersectRelBase
        implements JdbcRel
    {
        public JdbcIntersectRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelNode> inputs,
            boolean all)
        {
            super(cluster, traitSet, inputs, all);
            assert !all;
        }

        public JdbcIntersectRel copy(
            RelTraitSet traitSet, List<RelNode> inputs, boolean all)
        {
            return new JdbcIntersectRel(getCluster(), traitSet, inputs, all);
        }

        public SqlString implement(JdbcImplementor implementor) {
            return setOpSql(this, implementor, " intersect ");
        }
    }

    /**
     * Rule to convert an {@link org.eigenbase.rel.MinusRel} to an
     * {@link JdbcMinusRel}.
     */
    private static class JdbcMinusRule
        extends JdbcConverterRule
    {
        private JdbcMinusRule(JdbcConvention out) {
            super(
                MinusRel.class,
                Convention.NONE,
                out,
                "JdbcMinusRule");
        }

        public RelNode convert(RelNode rel)
        {
            final MinusRel minus = (MinusRel) rel;
            if (minus.all) {
                return null; // EXCEPT ALL not implemented
            }
            final RelTraitSet traitSet =
                rel.getTraitSet().replace(out);
            return new JdbcMinusRel(
                rel.getCluster(),
                traitSet,
                convertList(minus.getInputs(), traitSet),
                minus.all);
        }
    }

    public static class JdbcMinusRel
        extends MinusRelBase
        implements JdbcRel
    {
        public JdbcMinusRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelNode> inputs,
            boolean all)
        {
            super(cluster, traitSet, inputs, all);
            assert !all;
        }

        public JdbcMinusRel copy(
            RelTraitSet traitSet, List<RelNode> inputs, boolean all)
        {
            return new JdbcMinusRel(getCluster(), traitSet, inputs, all);
        }

        public SqlString implement(JdbcImplementor implementor) {
            return setOpSql(this, implementor, " minus ");
        }
    }

    public static class JdbcTableModificationRule extends JdbcConverterRule {
        private JdbcTableModificationRule(JdbcConvention out) {
            super(
                TableModificationRel.class,
                Convention.NONE,
                out,
                "JdbcTableModificationRule");
        }

        @Override
        public RelNode convert(RelNode rel) {
            final TableModificationRel modify =
                (TableModificationRel) rel;
            final ModifiableTable modifiableTable =
                modify.getTable().unwrap(ModifiableTable.class);
            if (modifiableTable == null
                || modifiableTable.getExpression() == null)
            {
                return null;
            }
            final RelTraitSet traitSet =
                modify.getTraitSet().replace(out);
            return new JdbcTableModificationRel(
                modify.getCluster(), traitSet,
                modify.getTable(),
                modify.getCatalogReader(),
                convert(modify.getChild(), traitSet),
                modify.getOperation(),
                modify.getUpdateColumnList(),
                modify.isFlattened());
        }
    }

    public static class JdbcTableModificationRel
        extends TableModificationRelBase
        implements JdbcRel
    {
        private final Expression expression;

        public JdbcTableModificationRel(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelOptTable table,
            Prepare.CatalogReader catalogReader,
            RelNode child,
            Operation operation,
            List<String> updateColumnList,
            boolean flattened)
        {
            super(
                cluster, traits, table, catalogReader, child, operation,
                updateColumnList, flattened);
            assert child.getConvention() instanceof JdbcConvention;
            assert getConvention() instanceof JdbcConvention;
            final ModifiableTable modifiableTable =
                table.unwrap(ModifiableTable.class);
            if (modifiableTable == null) {
                throw new AssertionError(); // TODO: user error in validator
            }
            this.expression = modifiableTable.getExpression();
            if (expression == null) {
                throw new AssertionError(); // TODO: user error in validator
            }
        }

        @Override
        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            return new JdbcTableModificationRel(
                getCluster(), traitSet, getTable(), getCatalogReader(),
                sole(inputs), getOperation(), getUpdateColumnList(),
                isFlattened());
        }

        public SqlString implement(JdbcImplementor implementor) {
            throw new AssertionError(); // TODO:
        }
    }

    public static class JdbcValuesRule extends JdbcConverterRule {
        private JdbcValuesRule(JdbcConvention out) {
            super(
                ValuesRel.class,
                Convention.NONE,
                out,
                "JdbcValuesRule");
        }

        @Override
        public RelNode convert(RelNode rel) {
            ValuesRel valuesRel = (ValuesRel) rel;
            return new JdbcValuesRel(
                valuesRel.getCluster(),
                valuesRel.getRowType(),
                valuesRel.getTuples(),
                valuesRel.getTraitSet().plus(out));
        }
    }

    public static class JdbcValuesRel
        extends ValuesRelBase
        implements JdbcRel
    {
        JdbcValuesRel(
            RelOptCluster cluster,
            RelDataType rowType,
            List<List<RexLiteral>> tuples,
            RelTraitSet traitSet)
        {
            super(cluster, rowType, tuples, traitSet);
        }

        @Override
        public RelNode copy(
            RelTraitSet traitSet, List<RelNode> inputs)
        {
            assert inputs.isEmpty();
            return new JdbcValuesRel(
                getCluster(), rowType, tuples, traitSet);
        }

        public SqlString implement(JdbcImplementor implementor) {
            throw new AssertionError(); // TODO:
        }
    }
}

// End JdbcRules.java
