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
package net.hydromatic.optiq.impl.jdbc;

import net.hydromatic.linq4j.Ord;
import net.hydromatic.linq4j.Queryable;
import net.hydromatic.linq4j.expressions.*;

import net.hydromatic.optiq.ModifiableTable;
import net.hydromatic.optiq.prepare.Prepare;
import net.hydromatic.optiq.util.BitSets;

import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.ConverterRule;
import org.eigenbase.rel.metadata.RelMetadataQuery;
import org.eigenbase.rel.rules.EquiJoinRel;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.SqlValidatorUtil;
import org.eigenbase.trace.EigenbaseTrace;
import org.eigenbase.util.*;

import com.google.common.collect.ImmutableList;

import java.util.*;
import java.util.logging.Logger;

/**
 * Rules and relational operators for
 * {@link JdbcConvention}
 * calling convention.
 */
public class JdbcRules {
  private JdbcRules() {
  }

  protected static final Logger LOGGER = EigenbaseTrace.getPlannerTracer();

  private static final SqlParserPos POS = SqlParserPos.ZERO;

  public static List<RelOptRule> rules(JdbcConvention out) {
    return ImmutableList.<RelOptRule>of(
        new JdbcToEnumerableConverterRule(out),
        new JdbcJoinRule(out),
        new JdbcCalcRule(out),
        new JdbcProjectRule(out),
        new JdbcFilterRule(out),
        new JdbcAggregateRule(out),
        new JdbcSortRule(out),
        new JdbcUnionRule(out),
        new JdbcIntersectRule(out),
        new JdbcMinusRule(out),
        new JdbcTableModificationRule(out),
        new JdbcValuesRule(out));
  }

  private static void addSelect(
      List<SqlNode> selectList, SqlNode node, RelDataType rowType) {
    String name = rowType.getFieldNames().get(selectList.size());
    String alias = SqlValidatorUtil.getAlias(node, -1);
    if (alias == null || !alias.equals(name)) {
      node = SqlStdOperatorTable.AS.createCall(
          POS, node, new SqlIdentifier(name, POS));
    }
    selectList.add(node);
  }

  private static JdbcImplementor.Result setOpToSql(JdbcImplementor implementor,
      SqlSetOperator operator, JdbcRel rel) {
    List<SqlNode> list = Expressions.list();
    for (Ord<RelNode> input : Ord.zip(rel.getInputs())) {
      final JdbcImplementor.Result result =
          implementor.visitChild(input.i, input.e);
      list.add(result.asSelect());
    }
    final SqlCall node = operator.createCall(new SqlNodeList(list, POS));
    final List<JdbcImplementor.Clause> clauses =
        Expressions.list(JdbcImplementor.Clause.SET_OP);
    return implementor.result(node, clauses, rel);
  }

  private static boolean isStar(List<RexNode> exps, RelDataType inputRowType) {
    int i = 0;
    for (RexNode ref : exps) {
      if (!(ref instanceof RexInputRef)) {
        return false;
      } else if (((RexInputRef) ref).getIndex() != i++) {
        return false;
      }
    }
    return i == inputRowType.getFieldCount();
  }

  private static boolean isStar(RexProgram program) {
    int i = 0;
    for (RexLocalRef ref : program.getProjectList()) {
      if (ref.getIndex() != i++) {
        return false;
      }
    }
    return i == program.getInputRowType().getFieldCount();
  }

  /** Abstract base class for rule that converts to JDBC. */
  abstract static class JdbcConverterRule extends ConverterRule {
    protected final JdbcConvention out;

    public JdbcConverterRule(Class<? extends RelNode> clazz, RelTrait in,
        JdbcConvention out, String description) {
      super(clazz, in, out, description);
      this.out = out;
    }
  }

  /** Rule that converts a join to JDBC. */
  private static class JdbcJoinRule extends JdbcConverterRule {
    private JdbcJoinRule(JdbcConvention out) {
      super(JoinRel.class, Convention.NONE, out, "JdbcJoinRule");
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
                  input.getTraitSet().replace(out));
        }
        newInputs.add(input);
      }
      final JoinInfo joinInfo =
          JoinInfo.of(newInputs.get(0), newInputs.get(1), join.getCondition());
      if (!joinInfo.isEqui()) {
        // JdbcJoinRel only supports equi-join
        return null;
      }
      try {
        return new JdbcJoinRel(
            join.getCluster(),
            join.getTraitSet().replace(out),
            newInputs.get(0),
            newInputs.get(1),
            join.getCondition(),
            joinInfo.leftKeys,
            joinInfo.rightKeys,
            join.getJoinType(),
            join.getVariablesStopped());
      } catch (InvalidRelException e) {
        LOGGER.fine(e.toString());
        return null;
      }
    }
  }

  /** Join operator implemented in JDBC convention. */
  public static class JdbcJoinRel
      extends EquiJoinRel
      implements JdbcRel {
    protected JdbcJoinRel(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode left,
        RelNode right,
        RexNode condition,
        ImmutableIntList leftKeys,
        ImmutableIntList rightKeys,
        JoinRelType joinType,
        Set<String> variablesStopped)
        throws InvalidRelException {
      super(cluster, traits, left, right, condition, leftKeys, rightKeys,
          joinType, variablesStopped);
    }

    @Override
    public JdbcJoinRel copy(RelTraitSet traitSet, RexNode condition,
        RelNode left, RelNode right, JoinRelType joinType,
        boolean semiJoinDone) {
      final JoinInfo joinInfo = JoinInfo.of(left, right, condition);
      assert joinInfo.isEqui();
      try {
        return new JdbcJoinRel(getCluster(), traitSet, left, right,
            condition, joinInfo.leftKeys, joinInfo.rightKeys, joinType,
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

      return planner.getCostFactory().makeCost(rowCount, 0, 0);
    }

    @Override
    public double getRows() {
      final boolean leftKey = left.isKey(BitSets.of(leftKeys));
      final boolean rightKey = right.isKey(BitSets.of(rightKeys));
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

    public JdbcImplementor.Result implement(JdbcImplementor implementor) {
      final JdbcImplementor.Result leftResult =
          implementor.visitChild(0, left);
      final JdbcImplementor.Result rightResult =
          implementor.visitChild(1, right);
      SqlNode sqlCondition = null;
      final JdbcImplementor.Context leftContext = leftResult.qualifiedContext();
      final JdbcImplementor.Context rightContext =
          rightResult.qualifiedContext();
      for (Pair<Integer, Integer> pair : Pair.zip(leftKeys, rightKeys)) {
        SqlNode x =
            SqlStdOperatorTable.EQUALS.createCall(POS,
                leftContext.field(pair.left),
                rightContext.field(pair.right));
        if (sqlCondition == null) {
          sqlCondition = x;
        } else {
          sqlCondition =
              SqlStdOperatorTable.AND.createCall(POS, sqlCondition, x);
        }
      }
      SqlNode join =
          new SqlJoin(POS,
              leftResult.asFrom(),
              SqlLiteral.createBoolean(false, POS),
              joinType(joinType).symbol(POS),
              rightResult.asFrom(),
              JoinConditionType.ON.symbol(POS),
              sqlCondition);
      return implementor.result(join, leftResult, rightResult);
    }

    private static JoinType joinType(JoinRelType joinType) {
      switch (joinType) {
      case LEFT:
        return JoinType.LEFT;
      case RIGHT:
        return JoinType.RIGHT;
      case INNER:
        return JoinType.INNER;
      case FULL:
        return JoinType.FULL;
      default:
        throw new AssertionError(joinType);
      }
    }
  }

  /**
   * Rule to convert a {@link CalcRel} to an
   * {@link JdbcCalcRel}.
   */
  private static class JdbcCalcRule
      extends JdbcConverterRule {
    private JdbcCalcRule(JdbcConvention out) {
      super(CalcRel.class, Convention.NONE, out, "JdbcCalcRule");
    }

    public RelNode convert(RelNode rel) {
      final CalcRel calc = (CalcRel) rel;

      // If there's a multiset, let FarragoMultisetSplitter work on it
      // first.
      if (RexMultisetUtil.containsMultiset(calc.getProgram())) {
        return null;
      }

      return new JdbcCalcRel(rel.getCluster(), rel.getTraitSet().replace(out),
          convert(calc.getChild(), calc.getTraitSet().replace(out)),
          calc.getProgram(), ProjectRelBase.Flags.BOXED);
    }
  }

  /** Calc operator implemented in JDBC convention. */
  public static class JdbcCalcRel extends SingleRel implements JdbcRel {
    private final RexProgram program;

    /**
     * Values defined in {@link org.eigenbase.rel.ProjectRelBase.Flags}.
     */
    protected final int flags;

    public JdbcCalcRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode child,
        RexProgram program,
        int flags) {
      super(cluster, traitSet, child);
      assert getConvention() instanceof JdbcConvention;
      this.flags = flags;
      this.program = program;
      this.rowType = program.getOutputRowType();
    }

    public RelWriter explainTerms(RelWriter pw) {
      return program.explainCalc(super.explainTerms(pw));
    }

    public double getRows() {
      return FilterRel.estimateFilteredRows(
          getChild(), program);
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner) {
      double dRows = RelMetadataQuery.getRowCount(this);
      double dCpu = RelMetadataQuery.getRowCount(getChild())
          * program.getExprCount();
      double dIo = 0;
      return planner.getCostFactory().makeCost(dRows, dCpu, dIo);
    }

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new JdbcCalcRel(getCluster(), traitSet, sole(inputs), program,
          flags);
    }

    public JdbcImplementor.Result implement(JdbcImplementor implementor) {
      JdbcImplementor.Result x = implementor.visitChild(0, getChild());
      final JdbcImplementor.Builder builder =
          program.getCondition() != null
              ? x.builder(this, JdbcImplementor.Clause.FROM,
                  JdbcImplementor.Clause.WHERE)
              : x.builder(this, JdbcImplementor.Clause.FROM);
      if (!isStar(program)) {
        final List<SqlNode> selectList = new ArrayList<SqlNode>();
        for (RexLocalRef ref : program.getProjectList()) {
          SqlNode sqlExpr = builder.context.toSql(program, ref);
          addSelect(selectList, sqlExpr, getRowType());
        }
        builder.setSelect(new SqlNodeList(selectList, POS));
      }
      if (program.getCondition() != null) {
        builder.setWhere(
            builder.context.toSql(program, program.getCondition()));
      }
      return builder.result();
    }
  }

  /**
   * Rule to convert a {@link ProjectRel} to an
   * {@link JdbcProjectRel}.
   */
  private static class JdbcProjectRule
      extends ConverterRule {
    private JdbcProjectRule(JdbcConvention out) {
      super(ProjectRel.class, Convention.NONE, out, "JdbcProjectRule");
    }

    public RelNode convert(RelNode rel) {
      final ProjectRel project = (ProjectRel) rel;

      return new JdbcProjectRel(
          rel.getCluster(),
          rel.getTraitSet().replace(getOutConvention()),
          convert(
              project.getChild(),
              project.getChild().getTraitSet().replace(getOutConvention())),
          project.getProjects(),
          project.getRowType(),
          ProjectRelBase.Flags.BOXED);
    }
  }

  /** Implementation of {@link org.eigenbase.rel.ProjectRel} in
   * {@link JdbcConvention jdbc calling convention}. */
  public static class JdbcProjectRel
      extends ProjectRelBase
      implements JdbcRel {
    public JdbcProjectRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode child,
        List<RexNode> exps,
        RelDataType rowType,
        int flags) {
      super(cluster, traitSet, child, exps, rowType, flags);
      assert getConvention() instanceof JdbcConvention;
    }

    @Override public JdbcProjectRel copy(RelTraitSet traitSet, RelNode input,
        List<RexNode> exps, RelDataType rowType) {
      return new JdbcProjectRel(getCluster(), traitSet, input, exps, rowType,
          flags);
    }

    public JdbcImplementor.Result implement(JdbcImplementor implementor) {
      JdbcImplementor.Result x = implementor.visitChild(0, getChild());
      if (isStar(exps, getChild().getRowType())) {
        return x;
      }
      final JdbcImplementor.Builder builder =
          x.builder(this, JdbcImplementor.Clause.SELECT);
      final List<SqlNode> selectList = new ArrayList<SqlNode>();
      for (RexNode ref : exps) {
        SqlNode sqlExpr = builder.context.toSql(null, ref);
        addSelect(selectList, sqlExpr, getRowType());
      }
      builder.setSelect(new SqlNodeList(selectList, POS));
      return builder.result();
    }
  }

  /**
   * Rule to convert a {@link FilterRel} to an
   * {@link JdbcFilterRel}.
   */
  private static class JdbcFilterRule
      extends ConverterRule {
    private JdbcFilterRule(JdbcConvention out) {
      super(FilterRel.class, Convention.NONE, out, "JdbcFilterRule");
    }

    public RelNode convert(RelNode rel) {
      final FilterRel filter = (FilterRel) rel;

      return new JdbcFilterRel(
          rel.getCluster(),
          rel.getTraitSet().replace(getOutConvention()),
          convert(
              filter.getChild(),
              filter.getChild().getTraitSet().replace(getOutConvention())),
          filter.getCondition());
    }
  }

  /** Implementation of {@link org.eigenbase.rel.FilterRel} in
   * {@link JdbcConvention jdbc calling convention}. */
  public static class JdbcFilterRel
      extends FilterRelBase
      implements JdbcRel {
    public JdbcFilterRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode child,
        RexNode condition) {
      super(cluster, traitSet, child, condition);
      assert getConvention() instanceof JdbcConvention;
    }

    public JdbcFilterRel copy(RelTraitSet traitSet, RelNode input,
        RexNode condition) {
      return new JdbcFilterRel(getCluster(), traitSet, input, condition);
    }

    public JdbcImplementor.Result implement(JdbcImplementor implementor) {
      JdbcImplementor.Result x = implementor.visitChild(0, getChild());
      final JdbcImplementor.Builder builder =
          x.builder(this, JdbcImplementor.Clause.WHERE);
      builder.setWhere(builder.context.toSql(null, condition));
      return builder.result();
    }
  }

  /**
   * Rule to convert an {@link org.eigenbase.rel.AggregateRel} to an
   * {@link JdbcAggregateRel}.
   */
  private static class JdbcAggregateRule extends JdbcConverterRule {
    private JdbcAggregateRule(JdbcConvention out) {
      super(AggregateRel.class, Convention.NONE, out, "JdbcAggregateRule");
    }

    public RelNode convert(RelNode rel) {
      final AggregateRel agg = (AggregateRel) rel;
      final RelTraitSet traitSet =
          agg.getTraitSet().replace(out);
      try {
        return new JdbcAggregateRel(rel.getCluster(), traitSet,
            convert(agg.getChild(), traitSet), agg.getGroupSet(),
            agg.getAggCallList());
      } catch (InvalidRelException e) {
        LOGGER.fine(e.toString());
        return null;
      }
    }
  }

  /** Aggregate operator implemented in JDBC convention. */
  public static class JdbcAggregateRel extends AggregateRelBase
      implements JdbcRel {
    public JdbcAggregateRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode child,
        BitSet groupSet,
        List<AggregateCall> aggCalls)
        throws InvalidRelException {
      super(cluster, traitSet, child, groupSet, aggCalls);
      assert getConvention() instanceof JdbcConvention;
    }

    @Override public JdbcAggregateRel copy(RelTraitSet traitSet, RelNode input,
        BitSet groupSet, List<AggregateCall> aggCalls) {
      try {
        return new JdbcAggregateRel(getCluster(), traitSet, input, groupSet,
            aggCalls);
      } catch (InvalidRelException e) {
        // Semantic error not possible. Must be a bug. Convert to
        // internal error.
        throw new AssertionError(e);
      }
    }

    public JdbcImplementor.Result implement(JdbcImplementor implementor) {
      // "select a, b, sum(x) from ( ... ) group by a, b"
      final JdbcImplementor.Result x = implementor.visitChild(0, getChild());
      final JdbcImplementor.Builder builder =
          x.builder(this, JdbcImplementor.Clause.GROUP_BY);
      List<SqlNode> groupByList = Expressions.list();
      final List<SqlNode> selectList = new ArrayList<SqlNode>();
      for (int group : BitSets.toIter(groupSet)) {
        final SqlNode field = builder.context.field(group);
        addSelect(selectList, field, getRowType());
        groupByList.add(field);
      }
      for (AggregateCall aggCall : aggCalls) {
        addSelect(selectList, builder.context.toSql(aggCall), rowType);
      }
      builder.setSelect(new SqlNodeList(selectList, POS));
      if (!groupByList.isEmpty() || aggCalls.isEmpty()) {
        // Some databases don't support "GROUP BY ()". We can omit it as long
        // as there is at least one aggregate function.
        builder.setGroupBy(new SqlNodeList(groupByList, POS));
      }
      return builder.result();
    }
  }

  /**
   * Rule to convert an {@link org.eigenbase.rel.SortRel} to an
   * {@link JdbcSortRel}.
   */
  private static class JdbcSortRule extends JdbcConverterRule {
    private JdbcSortRule(JdbcConvention out) {
      super(SortRel.class, Convention.NONE, out, "JdbcSortRule");
    }

    public RelNode convert(RelNode rel) {
      final SortRel sort = (SortRel) rel;
      if (sort.offset != null || sort.fetch != null) {
        // Cannot implement "OFFSET n FETCH n" currently.
        return null;
      }
      final RelTraitSet traitSet = sort.getTraitSet().replace(out);
      return new JdbcSortRel(rel.getCluster(), traitSet,
          convert(sort.getChild(), traitSet), sort.getCollation());
    }
  }

  /** Sort operator implemented in JDBC convention. */
  public static class JdbcSortRel
      extends SortRel
      implements JdbcRel {
    public JdbcSortRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode child,
        RelCollation collation) {
      super(cluster, traitSet, child, collation);
      assert getConvention() instanceof JdbcConvention;
      assert getConvention() == child.getConvention();
    }

    @Override
    public JdbcSortRel copy(RelTraitSet traitSet, RelNode newInput,
        RelCollation newCollation) {
      return new JdbcSortRel(getCluster(), traitSet, newInput, newCollation);
    }

    public JdbcImplementor.Result implement(JdbcImplementor implementor) {
      final JdbcImplementor.Result x = implementor.visitChild(0, getChild());
      final JdbcImplementor.Builder builder =
          x.builder(this, JdbcImplementor.Clause.ORDER_BY);
      List<SqlNode> orderByList = Expressions.list();
      for (RelFieldCollation fieldCollation : collation.getFieldCollations()) {
        if (fieldCollation.nullDirection
            != RelFieldCollation.NullDirection.UNSPECIFIED
            && implementor.dialect.getDatabaseProduct()
               == SqlDialect.DatabaseProduct.MYSQL) {
          orderByList.add(
              ISNULL_FUNCTION.createCall(POS,
                  builder.context.field(fieldCollation.getFieldIndex())));
          fieldCollation = new RelFieldCollation(fieldCollation.getFieldIndex(),
              fieldCollation.getDirection());
        }
        orderByList.add(builder.context.toSql(fieldCollation));
      }
      builder.setOrderBy(new SqlNodeList(orderByList, POS));
      return builder.result();
    }
  }

  /** MySQL specific function. */
  private static final SqlFunction ISNULL_FUNCTION =
      new SqlFunction("ISNULL", SqlKind.OTHER_FUNCTION,
          ReturnTypes.BOOLEAN, InferTypes.FIRST_KNOWN,
          OperandTypes.ANY, SqlFunctionCategory.SYSTEM);

  /**
   * Rule to convert an {@link org.eigenbase.rel.UnionRel} to a
   * {@link JdbcUnionRel}.
   */
  private static class JdbcUnionRule
      extends JdbcConverterRule {
    private JdbcUnionRule(JdbcConvention out) {
      super(UnionRel.class, Convention.NONE, out, "JdbcUnionRule");
    }

    public RelNode convert(RelNode rel) {
      final UnionRel union = (UnionRel) rel;
      final RelTraitSet traitSet =
          union.getTraitSet().replace(out);
      return new JdbcUnionRel(rel.getCluster(), traitSet,
          convertList(union.getInputs(), out), union.all);
    }
  }

  /** Union operator implemented in JDBC convention. */
  public static class JdbcUnionRel
      extends UnionRelBase
      implements JdbcRel {
    public JdbcUnionRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelNode> inputs,
        boolean all) {
      super(cluster, traitSet, inputs, all);
    }

    public JdbcUnionRel copy(
        RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
      return new JdbcUnionRel(getCluster(), traitSet, inputs, all);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner) {
      return super.computeSelfCost(planner).multiplyBy(.1);
    }

    public JdbcImplementor.Result implement(JdbcImplementor implementor) {
      final SqlSetOperator operator = all
          ? SqlStdOperatorTable.UNION_ALL
          : SqlStdOperatorTable.UNION;
      return setOpToSql(implementor, operator, this);
    }
  }

  /**
   * Rule to convert an {@link org.eigenbase.rel.IntersectRel} to an
   * {@link JdbcIntersectRel}.
   */
  private static class JdbcIntersectRule extends JdbcConverterRule {
    private JdbcIntersectRule(JdbcConvention out) {
      super(IntersectRel.class, Convention.NONE, out, "JdbcIntersectRule");
    }

    public RelNode convert(RelNode rel) {
      final IntersectRel intersect = (IntersectRel) rel;
      if (intersect.all) {
        return null; // INTERSECT ALL not implemented
      }
      final RelTraitSet traitSet =
          intersect.getTraitSet().replace(out);
      return new JdbcIntersectRel(rel.getCluster(), traitSet,
          convertList(intersect.getInputs(), out), intersect.all);
    }
  }

  /** Intersect operator implemented in JDBC convention. */
  public static class JdbcIntersectRel
      extends IntersectRelBase
      implements JdbcRel {
    public JdbcIntersectRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelNode> inputs,
        boolean all) {
      super(cluster, traitSet, inputs, all);
      assert !all;
    }

    public JdbcIntersectRel copy(
        RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
      return new JdbcIntersectRel(getCluster(), traitSet, inputs, all);
    }

    public JdbcImplementor.Result implement(JdbcImplementor implementor) {
      return setOpToSql(implementor,
          all
              ? SqlStdOperatorTable.INTERSECT_ALL
              : SqlStdOperatorTable.INTERSECT,
          this);
    }
  }

  /**
   * Rule to convert an {@link org.eigenbase.rel.MinusRel} to an
   * {@link JdbcMinusRel}.
   */
  private static class JdbcMinusRule
      extends JdbcConverterRule {
    private JdbcMinusRule(JdbcConvention out) {
      super(MinusRel.class, Convention.NONE, out, "JdbcMinusRule");
    }

    public RelNode convert(RelNode rel) {
      final MinusRel minus = (MinusRel) rel;
      if (minus.all) {
        return null; // EXCEPT ALL not implemented
      }
      final RelTraitSet traitSet =
          rel.getTraitSet().replace(out);
      return new JdbcMinusRel(rel.getCluster(), traitSet,
          convertList(minus.getInputs(), out), minus.all);
    }
  }

  /** Minus operator implemented in JDBC convention. */
  public static class JdbcMinusRel
      extends MinusRelBase
      implements JdbcRel {
    public JdbcMinusRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelNode> inputs,
        boolean all) {
      super(cluster, traitSet, inputs, all);
      assert !all;
    }

    public JdbcMinusRel copy(
        RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
      return new JdbcMinusRel(getCluster(), traitSet, inputs, all);
    }

    public JdbcImplementor.Result implement(JdbcImplementor implementor) {
      return setOpToSql(implementor,
          all
              ? SqlStdOperatorTable.EXCEPT_ALL
              : SqlStdOperatorTable.EXCEPT,
          this);
    }
  }

  /** Rule that converts a table-modification to JDBC. */
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
          /* || modifiableTable.getExpression(tableInSchema) == null */) {
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

  /** Table-modification operator implemented in JDBC convention. */
  public static class JdbcTableModificationRel
      extends TableModificationRelBase
      implements JdbcRel {
    private final Expression expression;

    public JdbcTableModificationRel(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelOptTable table,
        Prepare.CatalogReader catalogReader,
        RelNode child,
        Operation operation,
        List<String> updateColumnList,
        boolean flattened) {
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
      this.expression = table.getExpression(Queryable.class);
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

    public JdbcImplementor.Result implement(JdbcImplementor implementor) {
      throw new AssertionError(); // TODO:
    }
  }

  /** Rule that converts a values operator to JDBC. */
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
          valuesRel.getTraitSet().replace(out));
    }
  }

  /** Values operator implemented in JDBC convention. */
  public static class JdbcValuesRel
      extends ValuesRelBase
      implements JdbcRel {
    JdbcValuesRel(
        RelOptCluster cluster,
        RelDataType rowType,
        List<List<RexLiteral>> tuples,
        RelTraitSet traitSet) {
      super(cluster, rowType, tuples, traitSet);
    }

    @Override
    public RelNode copy(
        RelTraitSet traitSet, List<RelNode> inputs) {
      assert inputs.isEmpty();
      return new JdbcValuesRel(
          getCluster(), rowType, tuples, traitSet);
    }

    public JdbcImplementor.Result implement(JdbcImplementor implementor) {
      final List<String> fields = getRowType().getFieldNames();
      final List<JdbcImplementor.Clause> clauses = Collections.singletonList(
          JdbcImplementor.Clause.SELECT);
      final JdbcImplementor.Context context =
          implementor.new AliasContext(
              Collections.<Pair<String, RelDataType>>emptyList(), false);
      final List<SqlSelect> selects = new ArrayList<SqlSelect>();
      for (List<RexLiteral> tuple : tuples) {
        final List<SqlNode> selectList = new ArrayList<SqlNode>();
        for (Pair<RexLiteral, String> literal : Pair.zip(tuple, fields)) {
          selectList.add(
              SqlStdOperatorTable.AS.createCall(
                  POS,
                  context.toSql(null, literal.left),
                  new SqlIdentifier(literal.right, POS)));
        }
        selects.add(
            new SqlSelect(POS, SqlNodeList.EMPTY,
                new SqlNodeList(selectList, POS), null, null, null,
                null, null, null, null, null));
      }
      SqlNode query = null;
      for (SqlSelect select : selects) {
        if (query == null) {
          query = select;
        } else {
          query = SqlStdOperatorTable.UNION_ALL.createCall(POS, query,
              select);
        }
      }
      return implementor.result(query, clauses, this);
    }
  }
}

// End JdbcRules.java
