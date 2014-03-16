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
package net.hydromatic.optiq.impl.mongodb;

import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.rules.java.RexImpTable;
import net.hydromatic.optiq.rules.java.RexToLixTranslator;

import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.ConverterRule;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.*;
import org.eigenbase.sql.SqlKind;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.util.Bug;

import java.util.*;

/**
 * Rules and relational operators for
 * {@link MongoRel#CONVENTION MONGO}
 * calling convention.
 */
public class MongoRules {
  private MongoRules() {}

  public static final RelOptRule[] RULES = {
    MongoSortRule.INSTANCE,
    MongoFilterRule.INSTANCE,
    MongoProjectRule.INSTANCE,
  };

  private static String parseFieldAccess(RexNode rex) {
    if (rex instanceof RexCall) {
      final RexCall call = (RexCall) rex;
      if (call.getOperator() == SqlStdOperatorTable.ITEM
          && call.getOperands().size() == 2
          && call.getOperands().get(0) instanceof RexInputRef
          && ((RexInputRef) call.getOperands().get(0)).getIndex() == 0
          && call.getOperands().get(1) instanceof RexLiteral) {
        RexLiteral arg = (RexLiteral) call.getOperands().get(1);
        if (arg.getTypeName() == SqlTypeName.CHAR) {
          return (String) arg.getValue2();
        }
      }
    }
    return null;
  }

  private static RelDataType parseCast(RexNode rex) {
    if (rex instanceof RexCall) {
      final RexCall call = (RexCall) rex;
      if (call.getOperator() == SqlStdOperatorTable.CAST) {
        assert call.getOperands().size() == 1;
        return call.getType();
      }
    }
    return null;
  }

  /** Returns 'string' if it is a call to item['string'], null otherwise. */
  static String isItem(RexCall call) {
    if (call.getOperator() != SqlStdOperatorTable.ITEM) {
      return null;
    }
    final RexNode op0 = call.operands.get(0);
    final RexNode op1 = call.operands.get(1);
    if (op0 instanceof RexInputRef
        && ((RexInputRef) op0).getIndex() == 0
        && op1 instanceof RexLiteral
        && ((RexLiteral) op1).getValue2() instanceof String) {
      return (String) ((RexLiteral) op1).getValue2();
    }
    return null;
  }

  /** Translator from {@link RexNode} to strings in MongoDB's expression
   * language. */
  static class RexToMongoTranslator extends RexVisitorImpl<String> {
    private final JavaTypeFactory typeFactory;

    protected RexToMongoTranslator(JavaTypeFactory typeFactory) {
      super(true);
      this.typeFactory = typeFactory;
    }

    @Override public String visitLiteral(RexLiteral literal) {
      return "{$ifNull: [null, "
          + RexToLixTranslator.translateLiteral(literal, literal.getType(),
              typeFactory, RexImpTable.NullAs.NOT_POSSIBLE)
          + "]}";
    }

    @Override public String visitCall(RexCall call) {
      String name = isItem(call);
      if (name != null) {
        return "'$" + name + "'";
      }
      final List<String> strings = visitList(call.operands);
      if (call.getKind() == SqlKind.CAST) {
        return strings.get(0);
      }
      if (call.getOperator() == SqlStdOperatorTable.ITEM) {
        final RexNode op1 = call.operands.get(1);
        if (op1 instanceof RexLiteral
            && op1.getType().getSqlTypeName() == SqlTypeName.INTEGER) {
          if (!Bug.OPTIQ194_FIXED) {
            return "'" + stripQuotes(strings.get(0)) + "["
                + ((RexLiteral) op1).getValue2() + "]'";
          }
          return strings.get(0) + "[" + strings.get(1) + "]";
        }
      }
      return super.visitCall(call);
    }

    private String stripQuotes(String s) {
      return s.startsWith("'") && s.endsWith("'")
          ? s.substring(1, s.length() - 1)
          : s;
    }

    public List<String> visitList(List<RexNode> list) {
      final List<String> strings = new ArrayList<String>();
      for (RexNode node : list) {
        strings.add(node.accept(this));
      }
      return strings;
    }
  }

  /** Base class for planner rules that convert a relational expression to
   * MongoDB calling convention. */
  abstract static class MongoConverterRule extends ConverterRule {
    protected final Convention out;
    public MongoConverterRule(
        Class<? extends RelNode> clazz,
        RelTrait in,
        Convention out,
        String description) {
      super(clazz, in, out, description);
      this.out = out;
    }
  }

  /**
   * Rule to convert a {@link org.eigenbase.rel.SortRel} to a
   * {@link MongoSortRel}.
   */
  private static class MongoSortRule extends MongoConverterRule {
    public static final MongoSortRule INSTANCE = new MongoSortRule();

    private MongoSortRule() {
      super(SortRel.class, Convention.NONE, MongoRel.CONVENTION,
          "MongoSortRule");
    }

    public RelNode convert(RelNode rel) {
      final SortRel sort = (SortRel) rel;
      final RelTraitSet traitSet =
          sort.getTraitSet().replace(out)
              .replace(sort.getCollation());
      return new MongoSortRel(
          rel.getCluster(),
          traitSet,
          convert(sort.getChild(), traitSet.replace(RelCollationImpl.EMPTY)),
          sort.getCollation());
    }
  }

  /**
   * Rule to convert a {@link org.eigenbase.rel.FilterRel} to a
   * {@link MongoFilterRel}.
   */
  private static class MongoFilterRule extends MongoConverterRule {
    private static final MongoFilterRule INSTANCE = new MongoFilterRule();

    private MongoFilterRule() {
      super(FilterRel.class, Convention.NONE, MongoRel.CONVENTION,
          "MongoFilterRule");
    }

    public RelNode convert(RelNode rel) {
      final FilterRel filter = (FilterRel) rel;
      final RelTraitSet traitSet = filter.getTraitSet().replace(out);
      return new MongoFilterRel(
          rel.getCluster(),
          traitSet,
          convert(filter.getChild(), traitSet),
          filter.getCondition());
    }
  }

  /**
   * Rule to convert a {@link org.eigenbase.rel.ProjectRel} to a
   * {@link MongoProjectRel}.
   */
  private static class MongoProjectRule extends MongoConverterRule {
    private static final MongoProjectRule INSTANCE = new MongoProjectRule();

    private MongoProjectRule() {
      super(ProjectRel.class, Convention.NONE, MongoRel.CONVENTION,
          "MongoProjectRule");
    }

    public RelNode convert(RelNode rel) {
      final ProjectRel project = (ProjectRel) rel;
      final RelTraitSet traitSet = project.getTraitSet().replace(out);
      return new MongoProjectRel(project.getCluster(), traitSet,
          convert(project.getChild(), traitSet), project.getProjects(),
          project.getRowType(), ProjectRel.Flags.BOXED);
    }
  }

/*

  /**
   * Rule to convert a {@link CalcRel} to an
   * {@link MongoCalcRel}.
   o/
  private static class MongoCalcRule
      extends MongoConverterRule {
    private MongoCalcRule(MongoConvention out) {
      super(
          CalcRel.class,
          Convention.NONE,
          out,
          "MongoCalcRule");
    }

    public RelNode convert(RelNode rel) {
      final CalcRel calc = (CalcRel) rel;

      // If there's a multiset, let FarragoMultisetSplitter work on it
      // first.
      if (RexMultisetUtil.containsMultiset(calc.getProgram())) {
        return null;
      }

      return new MongoCalcRel(
          rel.getCluster(),
          rel.getTraitSet().replace(out),
          convert(
              calc.getChild(),
              calc.getTraitSet().replace(out)),
          calc.getProgram(),
          ProjectRelBase.Flags.Boxed);
    }
  }

  public static class MongoCalcRel extends SingleRel implements MongoRel {
    private final RexProgram program;

    /**
     * Values defined in {@link org.eigenbase.rel.ProjectRelBase.Flags}.
     o/
    protected int flags;

    public MongoCalcRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode child,
        RexProgram program,
        int flags) {
      super(cluster, traitSet, child);
      assert getConvention() instanceof MongoConvention;
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
      return new MongoCalcRel(
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

    public SqlString implement(MongoImplementor implementor) {
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
      return i == program.getInputRowType().getFieldCount();
    }

    private static void expr(
        SqlBuilder buf, RexProgram program, RexNode rex) {
      if (rex instanceof RexLocalRef) {
        final int index = ((RexLocalRef) rex).getIndex();
        expr(buf, program, program.getExprList().get(index));
      } else if (rex instanceof RexInputRef) {
        buf.identifier(
            program.getInputRowType().getFieldNames().get(
                ((RexInputRef) rex).getIndex()));
      } else if (rex instanceof RexLiteral) {
        toSql(buf, (RexLiteral) rex);
      } else if (rex instanceof RexCall) {
        final RexCall call = (RexCall) rex;
        switch (call.getOperator().getSyntax()) {
        case Binary:
          expr(buf, program, call.getOperands().get(0));
          buf.append(' ')
              .append(call.getOperator().toString())
              .append(' ');
          expr(buf, program, call.getOperands().get(1));
          break;
        default:
          throw new AssertionError(call.getOperator());
        }
      } else {
        throw new AssertionError(rex);
      }
    }
  }

  private static SqlBuilder toSql(SqlBuilder buf, RexLiteral rex) {
    switch (rex.getTypeName()) {
    case CHAR:
    case VARCHAR:
      return buf.append(
          new NlsString(rex.getValue2().toString(), null, null)
              .asSql(false, false));
    default:
      return buf.append(rex.getValue2().toString());
    }
  }

  /**
   * Rule to convert an {@link org.eigenbase.rel.AggregateRel} to an
   * {@link MongoAggregateRel}.
   o/
  private static class MongoAggregateRule extends MongoConverterRule {
    private MongoAggregateRule(MongoConvention out) {
      super(
          AggregateRel.class,
          Convention.NONE,
          out,
          "MongoAggregateRule");
    }

    public RelNode convert(RelNode rel) {
      final AggregateRel agg = (AggregateRel) rel;
      final RelTraitSet traitSet =
          agg.getTraitSet().replace(out);
      try {
        return new MongoAggregateRel(
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

  public static class MongoAggregateRel
      extends AggregateRelBase
      implements MongoRel {
    public MongoAggregateRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode child,
        BitSet groupSet,
        List<AggregateCall> aggCalls)
        throws InvalidRelException {
      super(cluster, traitSet, child, groupSet, aggCalls);
      assert getConvention() instanceof MongoConvention;

      for (AggregateCall aggCall : aggCalls) {
        if (aggCall.isDistinct()) {
          throw new InvalidRelException(
              "distinct aggregation not supported");
        }
      }
    }

    @Override
    public MongoAggregateRel copy(
        RelTraitSet traitSet, List<RelNode> inputs) {
      try {
        return new MongoAggregateRel(
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

    public SqlString implement(MongoImplementor implementor) {
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
   * Rule to convert an {@link org.eigenbase.rel.UnionRel} to a
   * {@link MongoUnionRel}.
   o/
  private static class MongoUnionRule
      extends MongoConverterRule {
    private MongoUnionRule(MongoConvention out) {
      super(
          UnionRel.class,
          Convention.NONE,
          out,
          "MongoUnionRule");
    }

    public RelNode convert(RelNode rel) {
      final UnionRel union = (UnionRel) rel;
      final RelTraitSet traitSet =
          union.getTraitSet().replace(out);
      return new MongoUnionRel(
          rel.getCluster(),
          traitSet,
          convertList(union.getInputs(), traitSet),
          union.all);
    }
  }

  public static class MongoUnionRel
      extends UnionRelBase
      implements MongoRel {
    public MongoUnionRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelNode> inputs,
        boolean all) {
      super(cluster, traitSet, inputs, all);
    }

    public MongoUnionRel copy(
        RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
      return new MongoUnionRel(getCluster(), traitSet, inputs, all);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner) {
      return super.computeSelfCost(planner).multiplyBy(.1);
    }

    public SqlString implement(MongoImplementor implementor) {
      return setOpSql(this, implementor, "UNION");
    }
  }

  private static SqlString setOpSql(
      SetOpRel setOpRel, MongoImplementor implementor, String op) {
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
   * {@link MongoIntersectRel}.
   o/
  private static class MongoIntersectRule
      extends MongoConverterRule {
    private MongoIntersectRule(MongoConvention out) {
      super(
          IntersectRel.class,
          Convention.NONE,
          out,
          "MongoIntersectRule");
    }

    public RelNode convert(RelNode rel) {
      final IntersectRel intersect = (IntersectRel) rel;
      if (intersect.all) {
        return null; // INTERSECT ALL not implemented
      }
      final RelTraitSet traitSet =
          intersect.getTraitSet().replace(out);
      return new MongoIntersectRel(
          rel.getCluster(),
          traitSet,
          convertList(intersect.getInputs(), traitSet),
          intersect.all);
    }
  }

  public static class MongoIntersectRel
      extends IntersectRelBase
      implements MongoRel {
    public MongoIntersectRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelNode> inputs,
        boolean all) {
      super(cluster, traitSet, inputs, all);
      assert !all;
    }

    public MongoIntersectRel copy(
        RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
      return new MongoIntersectRel(getCluster(), traitSet, inputs, all);
    }

    public SqlString implement(MongoImplementor implementor) {
      return setOpSql(this, implementor, " intersect ");
    }
  }

  /**
   * Rule to convert an {@link org.eigenbase.rel.MinusRel} to an
   * {@link MongoMinusRel}.
   o/
  private static class MongoMinusRule
      extends MongoConverterRule {
    private MongoMinusRule(MongoConvention out) {
      super(
          MinusRel.class,
          Convention.NONE,
          out,
          "MongoMinusRule");
    }

    public RelNode convert(RelNode rel) {
      final MinusRel minus = (MinusRel) rel;
      if (minus.all) {
        return null; // EXCEPT ALL not implemented
      }
      final RelTraitSet traitSet =
          rel.getTraitSet().replace(out);
      return new MongoMinusRel(
          rel.getCluster(),
          traitSet,
          convertList(minus.getInputs(), traitSet),
          minus.all);
    }
  }

  public static class MongoMinusRel
      extends MinusRelBase
      implements MongoRel {
    public MongoMinusRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelNode> inputs,
        boolean all) {
      super(cluster, traitSet, inputs, all);
      assert !all;
    }

    public MongoMinusRel copy(
        RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
      return new MongoMinusRel(getCluster(), traitSet, inputs, all);
    }

    public SqlString implement(MongoImplementor implementor) {
      return setOpSql(this, implementor, " minus ");
    }
  }

  public static class MongoValuesRule extends MongoConverterRule {
    private MongoValuesRule(MongoConvention out) {
      super(
          ValuesRel.class,
          Convention.NONE,
          out,
          "MongoValuesRule");
    }

    @Override
    public RelNode convert(RelNode rel) {
      ValuesRel valuesRel = (ValuesRel) rel;
      return new MongoValuesRel(
          valuesRel.getCluster(),
          valuesRel.getRowType(),
          valuesRel.getTuples(),
          valuesRel.getTraitSet().plus(out));
    }
  }

  public static class MongoValuesRel
      extends ValuesRelBase
      implements MongoRel {
    MongoValuesRel(
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
      return new MongoValuesRel(
          getCluster(), rowType, tuples, traitSet);
    }

    public SqlString implement(MongoImplementor implementor) {
      throw new AssertionError(); // TODO:
    }
  }
*/
}

// End MongoRules.java
