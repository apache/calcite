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
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.type.SqlTypeStrategies;
import org.eigenbase.sql.validate.SqlValidatorUtil;
import org.eigenbase.trace.EigenbaseTrace;
import org.eigenbase.util.*;

import java.util.*;
import java.util.logging.Logger;

/**
 * Rules and relational operators for
 * {@link JdbcConvention}
 * calling convention.
 */
public class JdbcRules {
  protected static final Logger tracer = EigenbaseTrace.getPlannerTracer();

  private static final SqlParserPos POS = SqlParserPos.ZERO;

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

  private static void addSelect(
      List<SqlNode> selectList, SqlNode node, RelDataType rowType) {
    String name = rowType.getFieldNames().get(selectList.size());
    String alias = SqlValidatorUtil.getAlias(node, -1);
    if (alias == null || !alias.equals(name)) {
      node = SqlStdOperatorTable.asOperator.createCall(
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

  static abstract class JdbcConverterRule extends ConverterRule {
    protected final JdbcConvention out;

    public JdbcConverterRule(Class<? extends RelNode> clazz, RelTrait in,
        JdbcConvention out, String description) {
      super(clazz, in, out, description);
      this.out = out;
    }
  }

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
      implements JdbcRel {
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
        throws InvalidRelException {
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
        RelNode right) {
      try {
        return new JdbcJoinRel(getCluster(), traitSet, left, right,
            conditionExpr, joinType, variablesStopped);
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
            SqlStdOperatorTable.equalsOperator.createCall(POS,
                leftContext.field(pair.left),
                rightContext.field(pair.right));
        if (sqlCondition == null) {
          sqlCondition = x;
        } else {
          sqlCondition =
              SqlStdOperatorTable.andOperator.createCall(POS, sqlCondition, x);
        }
      }
      SqlNode join =
          SqlStdOperatorTable.joinOperator.createCall(
              leftResult.asFrom(),
              SqlLiteral.createBoolean(false, POS),
              joinType(joinType).symbol(POS),
              rightResult.asFrom(),
              SqlJoinOperator.ConditionType.On.symbol(POS),
              sqlCondition,
              POS);
      return implementor.result(join, leftResult, rightResult);
    }

    private static SqlJoinOperator.JoinType joinType(JoinRelType joinType) {
      switch (joinType) {
      case LEFT:
        return SqlJoinOperator.JoinType.Left;
      case RIGHT:
        return SqlJoinOperator.JoinType.Right;
      case INNER:
        return SqlJoinOperator.JoinType.Inner;
      case FULL:
        return SqlJoinOperator.JoinType.Full;
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
          calc.getProgram(), ProjectRelBase.Flags.Boxed);
    }
  }

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

    public RelOptPlanWriter explainTerms(RelOptPlanWriter pw) {
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
      return planner.makeCost(dRows, dCpu, dIo);
    }

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new JdbcCalcRel(getCluster(), traitSet, sole(inputs),
          program.copy(), flags);
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

    private static boolean isStar(RexProgram program) {
      int i = 0;
      for (RexLocalRef ref : program.getProjectList()) {
        if (ref.getIndex() != i++) {
          return false;
        }
      }
      return i == program.getInputRowType().getFieldCount();
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
        tracer.warning(e.toString());
        return null;
      }
    }
  }

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

    @Override
    public JdbcAggregateRel copy(RelTraitSet traitSet, List<RelNode> inputs) {
      try {
        return new JdbcAggregateRel(getCluster(), traitSet, sole(inputs),
            groupSet, aggCalls);
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
      for (int group : Util.toIter(groupSet)) {
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
      final RelTraitSet traitSet = sort.getTraitSet().replace(out);
      return new JdbcSortRel(rel.getCluster(), traitSet,
          convert(sort.getChild(), traitSet), sort.getCollation());
    }
  }

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
  private static SqlFunction ISNULL_FUNCTION =
      new SqlFunction("ISNULL", SqlKind.OTHER_FUNCTION,
          SqlTypeStrategies.rtiBoolean, SqlTypeStrategies.otiFirstKnown,
          SqlTypeStrategies.otcAny, SqlFunctionCategory.System);

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
          convertList(union.getInputs(), traitSet), union.all);
    }
  }

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
          ? SqlStdOperatorTable.unionAllOperator
          : SqlStdOperatorTable.unionOperator;
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
      return new JdbcIntersectRel(
          rel.getCluster(), traitSet,
          convertList(intersect.getInputs(), traitSet), intersect.all);
    }
  }

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
              ? SqlStdOperatorTable.intersectAllOperator
              : SqlStdOperatorTable.intersectOperator,
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
      return new JdbcMinusRel(
          rel.getCluster(),
          traitSet,
          convertList(minus.getInputs(), traitSet),
          minus.all);
    }
  }

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
              ? SqlStdOperatorTable.exceptAllOperator
              : SqlStdOperatorTable.exceptOperator,
          this);
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
          || modifiableTable.getExpression() == null) {
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

    public JdbcImplementor.Result implement(JdbcImplementor implementor) {
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
              SqlStdOperatorTable.asOperator.createCall(
                  POS,
                  context.toSql(null, literal.left),
                  new SqlIdentifier(literal.right, POS)));
        }
        selects.add(
            SqlStdOperatorTable.selectOperator.createCall(SqlNodeList.Empty,
                new SqlNodeList(selectList, POS), null, null, null,
                null, null, null, POS));
      }
      SqlNode query = null;
      for (SqlSelect select : selects) {
        if (query == null) {
          query = select;
        } else {
          query = SqlStdOperatorTable.unionAllOperator.createCall(POS, query,
              select);
        }
      }
      return implementor.result(query, clauses, this);
    }
  }
}

// End JdbcRules.java
