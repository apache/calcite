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
package net.hydromatic.optiq.rules.java;

import net.hydromatic.linq4j.*;
import net.hydromatic.linq4j.expressions.*;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.linq4j.function.*;

import net.hydromatic.optiq.*;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.prepare.Prepare;
import net.hydromatic.optiq.runtime.SortedMultiMap;
import net.hydromatic.optiq.util.BitSets;

import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.ConverterRule;
import org.eigenbase.rel.metadata.RelMetadataQuery;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.SqlWindow;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.trace.EigenbaseTrace;
import org.eigenbase.util.*;

import com.google.common.collect.ImmutableList;

import java.lang.reflect.*;
import java.math.BigDecimal;
import java.util.*;
import java.util.logging.Logger;

/**
 * Rules and relational operators for the
 * {@link EnumerableConvention enumerable calling convention}.
 */
public class JavaRules {
  protected static final Logger LOGGER = EigenbaseTrace.getPlannerTracer();

  public static final boolean BRIDGE_METHODS = true;

  private static final List<ParameterExpression> NO_PARAMS =
      Collections.emptyList();

  private static final List<Expression> NO_EXPRS =
      Collections.emptyList();

  public static final RelOptRule ENUMERABLE_JOIN_RULE =
      new EnumerableJoinRule();

  public static final String[] LEFT_RIGHT = new String[]{"left", "right"};

  private JavaRules() {
  }

  private static class EnumerableJoinRule extends ConverterRule {
    private EnumerableJoinRule() {
      super(
          JoinRel.class,
          Convention.NONE,
          EnumerableConvention.INSTANCE,
          "EnumerableJoinRule");
    }

    @Override
    public RelNode convert(RelNode rel) {
      JoinRel join = (JoinRel) rel;
      List<RelNode> newInputs = new ArrayList<RelNode>();
      for (RelNode input : join.getInputs()) {
        if (!(input.getConvention() instanceof EnumerableConvention)) {
          input =
              convert(
                  input,
                  input.getTraitSet()
                      .replace(EnumerableConvention.INSTANCE));
        }
        newInputs.add(input);
      }
      try {
        return new EnumerableJoinRel(
            join.getCluster(),
            join.getTraitSet().replace(EnumerableConvention.INSTANCE),
            newInputs.get(0),
            newInputs.get(1),
            join.getCondition(),
            join.getJoinType(),
            join.getVariablesStopped());
      } catch (InvalidRelException e) {
        LOGGER.fine(e.toString());
        return null;
      }
    }
  }

  /** Implementation of {@link org.eigenbase.rel.JoinRel} in
   * {@link EnumerableConvention enumerable calling convention}. */
  public static class EnumerableJoinRel
      extends JoinRelBase
      implements EnumerableRel {
    final ImmutableIntList leftKeys;
    final ImmutableIntList rightKeys;

    protected EnumerableJoinRel(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode left,
        RelNode right,
        RexNode condition,
        JoinRelType joinType,
        Set<String> variablesStopped)
      throws InvalidRelException {
      super(
          cluster,
          traits,
          left,
          right,
          condition,
          joinType,
          variablesStopped);
      final List<Integer> leftKeys = new ArrayList<Integer>();
      final List<Integer> rightKeys = new ArrayList<Integer>();
      RexNode remaining =
          RelOptUtil.splitJoinCondition(
              left,
              right,
              condition,
              leftKeys,
              rightKeys);
      if (!remaining.isAlwaysTrue()) {
        throw new InvalidRelException(
            "EnumerableJoinRel only supports equi-join");
      }
      this.leftKeys = ImmutableIntList.copyOf(leftKeys);
      this.rightKeys = ImmutableIntList.copyOf(rightKeys);
    }

    @Override
    public EnumerableJoinRel copy(RelTraitSet traitSet, RexNode conditionExpr,
        RelNode left, RelNode right, JoinRelType joinType) {
      try {
        return new EnumerableJoinRel(getCluster(), traitSet, left, right,
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

      // Joins can be flipped, and for many algorithms, both versions are viable
      // and have the same cost. To make the results stable between versions of
      // the planner, make one of the versions slightly more expensive.
      switch (joinType) {
      case RIGHT:
        rowCount = addEpsilon(rowCount);
        break;
      default:
        if (left.getId() > right.getId()) {
          rowCount = addEpsilon(rowCount);
        }
      }

      // Cheaper if the smaller number of rows is coming from the RHS.
      final double rightRowCount = right.getRows();
      final double leftRowCount = left.getRows();
      if (rightRowCount > leftRowCount && !Double.isInfinite(rightRowCount)) {
        rowCount *= rightRowCount / (leftRowCount + 1d);
      }
      if (condition.isAlwaysTrue()) {
        rowCount *= 10d;
      }
      return planner.getCostFactory().makeCost(rowCount, 0, 0);
    }

    private double addEpsilon(double d) {
      assert d >= 0d;
      final double d0 = d;
      if (d < 10) {
        // For small d, adding 1 would change the value significantly.
        d *= 1.001d;
        if (d != d0) {
          return d;
        }
      }
      // For medium d, add 1. Keeps integral values integral.
      ++d;
      if (d != d0) {
        return d;
      }
      // For large d, adding 1 might not change the value. Add .1%.
      // If d is NaN, this still will probably not change the value. That's OK.
      d *= 1.001d;
      return d;
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

    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
      BlockBuilder builder = new BlockBuilder();
      final Result leftResult =
          implementor.visitChild(this, 0, (EnumerableRel) left, pref);
      Expression leftExpression =
          builder.append(
              "left", leftResult.block);
      final Result rightResult =
          implementor.visitChild(this, 1, (EnumerableRel) right, pref);
      Expression rightExpression =
          builder.append(
              "right", rightResult.block);
      final PhysType physType =
          PhysTypeImpl.of(
              implementor.getTypeFactory(), getRowType(), pref.preferArray());
      final PhysType keyPhysType =
          leftResult.physType.project(
              leftKeys, JavaRowFormat.LIST);
      return implementor.result(
          physType,
          builder.append(
              Expressions.call(
                  leftExpression,
                  BuiltinMethod.JOIN.method,
                  Expressions.list(
                      rightExpression,
                      leftResult.physType.generateAccessor(leftKeys),
                      rightResult.physType.generateAccessor(rightKeys),
                      generateSelector(
                          physType,
                          ImmutableList.of(
                              leftResult.physType, rightResult.physType)))
                      .append(
                          Util.first(keyPhysType.comparer(),
                              Expressions.constant(null)))
                      .append(Expressions.constant(
                          joinType.generatesNullsOnLeft()))
                      .append(Expressions.constant(
                          joinType.generatesNullsOnRight())))).toBlock());
    }

    Expression generateSelector(PhysType physType,
        List<PhysType> inputPhysTypes) {
      // A parameter for each input.
      final List<ParameterExpression> parameters =
          new ArrayList<ParameterExpression>();

      // Generate all fields.
      final List<Expression> expressions =
          new ArrayList<Expression>();
      for (Ord<PhysType> ord : Ord.zip(inputPhysTypes)) {
        final PhysType inputPhysType =
            ord.e.makeNullable(joinType.generatesNullsOn(ord.i));
        final ParameterExpression parameter =
            Expressions.parameter(inputPhysType.getJavaRowType(),
                LEFT_RIGHT[ord.i]);
        parameters.add(parameter);
        final int fieldCount = inputPhysType.getRowType().getFieldCount();
        for (int i = 0; i < fieldCount; i++) {
          Expression expression =
              inputPhysType.fieldReference(parameter, i);
          if (joinType.generatesNullsOn(ord.i)) {
            expression =
                Expressions.condition(
                    Expressions.equal(parameter, Expressions.constant(null)),
                    Expressions.constant(null),
                    expression);
          }
          expressions.add(Types.castIfNecessary(inputPhysType.fieldClass(i),
              expression));
        }
      }
      return Expressions.lambda(
          Function2.class,
          physType.record(expressions),
          parameters);
    }
  }

  /**
   * Utilities for generating programs in the Enumerable (functional)
   * style.
   */
  public static class EnumUtil {
    /** Declares a method that overrides another method. */
    public static MethodDeclaration overridingMethodDecl(Method method,
        Iterable<ParameterExpression> parameters,
        BlockStatement body) {
      return Expressions.methodDecl(
          method.getModifiers() & ~Modifier.ABSTRACT,
          method.getReturnType(),
          method.getName(),
          parameters,
          body);
    }

    static Type javaClass(
        JavaTypeFactory typeFactory, RelDataType type) {
      final Type clazz = typeFactory.getJavaClass(type);
      return clazz instanceof Class ? clazz : Object[].class;
    }

    static Class javaRowClass(
        JavaTypeFactory typeFactory, RelDataType type) {
      if (type.isStruct() && type.getFieldCount() == 1) {
        type = type.getFieldList().get(0).getType();
      }
      final Type clazz = typeFactory.getJavaClass(type);
      return clazz instanceof Class ? (Class) clazz : Object[].class;
    }

    static List<Type> fieldTypes(
        final JavaTypeFactory typeFactory,
        final RelDataType inputRowType,
        final List<Integer> argList) {
      return new AbstractList<Type>() {
        public Type get(int index) {
          return EnumUtil.javaClass(
              typeFactory,
              inputRowType.getFieldList()
                  .get(argList.get(index))
                  .getType());
        }
        public int size() {
          return argList.size();
        }
      };
    }

    static List<AggImplementor> getImplementors(List<AggregateCall> aggCalls) {
      final List<AggImplementor> implementors = new ArrayList<AggImplementor>();
      for (AggregateCall aggCall : aggCalls) {
        AggImplementor implementor2 =
            RexImpTable.INSTANCE.get(aggCall.getAggregation());
        if (implementor2 == null) {
          throw new RuntimeException(
              "cannot implement aggregate " + aggCall);
        }
        implementors.add(implementor2);
      }
      return implementors;
    }
  }

  /** Implementation of {@link org.eigenbase.rel.TableAccessRel} in
   * {@link EnumerableConvention enumerable calling convention}. */
  public static class EnumerableTableAccessRel
      extends TableAccessRelBase
      implements EnumerableRel {
    private final Class elementType;

    public EnumerableTableAccessRel(RelOptCluster cluster, RelTraitSet traitSet,
        RelOptTable table, Class elementType) {
      super(cluster, traitSet, table);
      assert getConvention() instanceof EnumerableConvention;
      this.elementType = elementType;
    }

    private Expression getExpression() {
      Expression expression = table.getExpression(Queryable.class);
      final Type type = expression.getType();
      if (Types.isArray(type)) {
        if (Types.toClass(type).getComponentType().isPrimitive()) {
          expression =
              Expressions.call(
                  BuiltinMethod.AS_LIST.method,
                  expression);
        }
        expression =
            Expressions.call(
                BuiltinMethod.AS_ENUMERABLE.method,
                expression);
      } else if (Types.isAssignableFrom(Iterable.class, type)
          && !Types.isAssignableFrom(Enumerable.class, type)) {
        expression =
            Expressions.call(
                BuiltinMethod.AS_ENUMERABLE2.method,
                expression);
      } else if (Types.isAssignableFrom(Queryable.class, type)) {
        // Queryable extends Enumerable, but it's too "clever", so we call
        // Queryable.asEnumerable so that operations such as take(int) will be
        // evaluated directly.
        expression =
            Expressions.call(
                expression,
                BuiltinMethod.QUERYABLE_AS_ENUMERABLE.method);
      }
      return expression;
    }

    private JavaRowFormat format() {
      if (Object[].class.isAssignableFrom(elementType)) {
        return JavaRowFormat.ARRAY;
      } else {
        return JavaRowFormat.CUSTOM;
      }
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new EnumerableTableAccessRel(getCluster(), traitSet, table,
          elementType);
    }

    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
      // Note that representation is ARRAY. This assumes that the table
      // returns a Object[] for each record. Actually a Table<T> can
      // return any type T. And, if it is a JdbcTable, we'd like to be
      // able to generate alternate accessors that return e.g. synthetic
      // records {T0 f0; T1 f1; ...} and don't box every primitive value.
      final PhysType physType =
          PhysTypeImpl.of(
              implementor.getTypeFactory(),
              getRowType(),
              format());
      final Expression expression = getExpression();
      return implementor.result(physType, Blocks.toBlock(expression));
    }
  }

  public static final EnumerableProjectRule ENUMERABLE_PROJECT_RULE =
      new EnumerableProjectRule();

  /**
   * Rule to convert a {@link ProjectRel} to an
   * {@link EnumerableProjectRel}.
   */
  private static class EnumerableProjectRule
      extends ConverterRule {
    private EnumerableProjectRule() {
      super(
          ProjectRel.class,
          Convention.NONE,
          EnumerableConvention.INSTANCE,
          "EnumerableProjectRule");
    }

    public RelNode convert(RelNode rel) {
      final ProjectRel project = (ProjectRel) rel;

      if (RexMultisetUtil.containsMultiset(project.getProjects(), true)
          || RexOver.containsOver(project.getProjects(), null)) {
        return null;
      }

      return new EnumerableProjectRel(
          rel.getCluster(),
          rel.getTraitSet().replace(EnumerableConvention.INSTANCE),
          convert(
              project.getChild(),
              project.getChild().getTraitSet()
                  .replace(EnumerableConvention.INSTANCE)),
          project.getProjects(),
          project.getRowType(),
          ProjectRelBase.Flags.BOXED);
    }
  }

  /** Implementation of {@link org.eigenbase.rel.ProjectRel} in
   * {@link EnumerableConvention enumerable calling convention}. */
  public static class EnumerableProjectRel
      extends ProjectRelBase
      implements EnumerableRel {
    public EnumerableProjectRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode child,
        List<RexNode> exps,
        RelDataType rowType,
        int flags) {
      super(cluster, traitSet, child, exps, rowType, flags);
      assert getConvention() instanceof EnumerableConvention;
    }

    public EnumerableProjectRel copy(RelTraitSet traitSet, RelNode input,
        List<RexNode> exps, RelDataType rowType) {
      return new EnumerableProjectRel(getCluster(), traitSet, input,
          exps, rowType, flags);
    }

    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
      // EnumerableCalcRel is always better
      throw new UnsupportedOperationException();
    }
  }

  public static final EnumerableFilterRule ENUMERABLE_FILTER_RULE =
      new EnumerableFilterRule();

  /**
   * Rule to convert a {@link FilterRel} to an
   * {@link EnumerableFilterRel}.
   */
  private static class EnumerableFilterRule
      extends ConverterRule {
    private EnumerableFilterRule() {
      super(
          FilterRel.class,
          Convention.NONE,
          EnumerableConvention.INSTANCE,
          "EnumerableFilterRule");
    }

    public RelNode convert(RelNode rel) {
      final FilterRel filter = (FilterRel) rel;

      if (RexMultisetUtil.containsMultiset(filter.getCondition(), true)
          || RexOver.containsOver(filter.getCondition())) {
        return null;
      }

      return new EnumerableFilterRel(
          rel.getCluster(),
          rel.getTraitSet().replace(EnumerableConvention.INSTANCE),
          convert(
              filter.getChild(),
              filter.getChild().getTraitSet()
                  .replace(EnumerableConvention.INSTANCE)),
          filter.getCondition());
    }
  }

  /** Implementation of {@link org.eigenbase.rel.FilterRel} in
   * {@link EnumerableConvention enumerable calling convention}. */
  public static class EnumerableFilterRel
      extends FilterRelBase
      implements EnumerableRel {
    public EnumerableFilterRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode child,
        RexNode condition) {
      super(cluster, traitSet, child, condition);
      assert getConvention() instanceof EnumerableConvention;
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new EnumerableFilterRel(getCluster(), traitSet, sole(inputs),
          condition);
    }

    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
      // EnumerableCalcRel is always better
      throw new UnsupportedOperationException();
    }
  }

  public static final EnumerableCalcRule ENUMERABLE_CALC_RULE =
      new EnumerableCalcRule();

  /**
   * Rule to convert a {@link CalcRel} to an
   * {@link EnumerableCalcRel}.
   */
  private static class EnumerableCalcRule
      extends ConverterRule {
    private EnumerableCalcRule() {
      super(
          CalcRel.class,
          Convention.NONE,
          EnumerableConvention.INSTANCE,
          "EnumerableCalcRule");
    }

    public RelNode convert(RelNode rel) {
      final CalcRel calc = (CalcRel) rel;

      // If there's a multiset, let FarragoMultisetSplitter work on it
      // first.
      final RexProgram program = calc.getProgram();
      if (RexMultisetUtil.containsMultiset(program)
          || program.containsAggs()) {
        return null;
      }

      return new EnumerableCalcRel(
          rel.getCluster(),
          rel.getTraitSet().replace(EnumerableConvention.INSTANCE),
          convert(
              calc.getChild(),
              calc.getChild().getTraitSet()
                  .replace(EnumerableConvention.INSTANCE)),
          calc.getRowType(),
          program,
          calc.getCollationList());
    }
  }

  /** Implementation of {@link org.eigenbase.rel.CalcRel} in
   * {@link EnumerableConvention enumerable calling convention}. */
  public static class EnumerableCalcRel
      extends CalcRelBase
      implements EnumerableRel {
    private final RexProgram program;

    public EnumerableCalcRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode child,
        RelDataType rowType,
        RexProgram program,
        List<RelCollation> collationList) {
      super(cluster, traitSet, child, rowType, program, collationList);
      assert getConvention() instanceof EnumerableConvention;
      assert !program.containsAggs();
      this.program = program;
      this.rowType = program.getOutputRowType();
    }

    @Override public EnumerableCalcRel copy(RelTraitSet traitSet, RelNode child,
        RexProgram program, List<RelCollation> collationList) {
      // we do not need to copy program; it is immutable
      return new EnumerableCalcRel(getCluster(), traitSet, child,
          program.getOutputRowType(), program, collationList);
    }

    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
      final JavaTypeFactory typeFactory = implementor.getTypeFactory();
      final BlockBuilder builder = new BlockBuilder();
      final EnumerableRel child = (EnumerableRel) getChild();

      final Result result =
          implementor.visitChild(this, 0, child, pref);

      final PhysType physType =
          PhysTypeImpl.of(
              typeFactory, getRowType(), pref.prefer(result.format));

      // final Enumerable<Employee> inputEnumerable = <<child impl>>;
      // return new Enumerable<IntString>() {
      //     Enumerator<IntString> enumerator() {
      //         return new Enumerator<IntString>() {
      //             public void reset() {
      // ...
      Type outputJavaType = physType.getJavaRowType();
      final Type enumeratorType =
          Types.of(
              Enumerator.class, outputJavaType);
      Type inputJavaType = result.physType.getJavaRowType();
      ParameterExpression inputEnumerator =
          Expressions.parameter(
              Types.of(
                  Enumerator.class, inputJavaType),
              "inputEnumerator");
      Expression input =
          RexToLixTranslator.convert(
              Expressions.call(
                  inputEnumerator,
                  BuiltinMethod.ENUMERATOR_CURRENT.method),
              inputJavaType);

      BlockStatement moveNextBody;
      if (program.getCondition() == null) {
        moveNextBody =
            Blocks.toFunctionBlock(
                Expressions.call(
                    inputEnumerator,
                    BuiltinMethod.ENUMERATOR_MOVE_NEXT.method));
      } else {
        final BlockBuilder builder2 = new BlockBuilder();
        Expression condition =
            RexToLixTranslator.translateCondition(
                program,
                typeFactory,
                builder2,
                new RexToLixTranslator.InputGetterImpl(
                    Collections.singletonList(
                        Pair.of(input, result.physType))));
        builder2.add(
            Expressions.ifThen(
                condition,
                Expressions.return_(
                    null, Expressions.constant(true))));
        moveNextBody =
            Expressions.block(
                Expressions.while_(
                    Expressions.call(
                        inputEnumerator,
                        BuiltinMethod.ENUMERATOR_MOVE_NEXT.method),
                    builder2.toBlock()),
                Expressions.return_(
                    null,
                    Expressions.constant(false)));
      }

      final BlockBuilder builder3 = new BlockBuilder();
      List<Expression> expressions =
          RexToLixTranslator.translateProjects(
              program,
              typeFactory,
              builder3,
              new RexToLixTranslator.InputGetterImpl(
                  Collections.singletonList(
                      Pair.of(input, result.physType))));
      builder3.add(
          Expressions.return_(
              null, physType.record(expressions)));
      BlockStatement currentBody =
          builder3.toBlock();

      final Expression inputEnumerable =
          builder.append(
              "inputEnumerable", result.block, false);
      final Expression body =
          Expressions.new_(
              enumeratorType,
              NO_EXPRS,
              Expressions.<MemberDeclaration>list(
                  Expressions.fieldDecl(
                      Modifier.PUBLIC
                      | Modifier.FINAL,
                      inputEnumerator,
                      Expressions.call(
                          inputEnumerable,
                          BuiltinMethod.ENUMERABLE_ENUMERATOR.method)),
                  EnumUtil.overridingMethodDecl(
                      BuiltinMethod.ENUMERATOR_RESET.method,
                      NO_PARAMS,
                      Blocks.toFunctionBlock(
                          Expressions.call(
                              inputEnumerator,
                              BuiltinMethod.ENUMERATOR_RESET.method))),
                  EnumUtil.overridingMethodDecl(
                      BuiltinMethod.ENUMERATOR_MOVE_NEXT.method,
                      NO_PARAMS,
                      moveNextBody),
                  EnumUtil.overridingMethodDecl(
                      BuiltinMethod.ENUMERATOR_CLOSE.method,
                      NO_PARAMS,
                      Blocks.toFunctionBlock(
                          Expressions.call(
                              inputEnumerator,
                              BuiltinMethod.ENUMERATOR_CLOSE.method))),
                  Expressions.methodDecl(
                      Modifier.PUBLIC,
                      BRIDGE_METHODS
                          ? Object.class
                          : outputJavaType,
                      "current",
                      NO_PARAMS,
                      currentBody)));
      builder.add(
          Expressions.return_(
              null,
              Expressions.new_(
                  BuiltinMethod.ABSTRACT_ENUMERABLE_CTOR.constructor,
                  // TODO: generics
                  //   Collections.singletonList(inputRowType),
                  NO_EXPRS,
                  Arrays.<MemberDeclaration>asList(
                      Expressions.methodDecl(
                          Modifier.PUBLIC,
                          enumeratorType,
                          BuiltinMethod.ENUMERABLE_ENUMERATOR.method.getName(),
                          NO_PARAMS,
                          Blocks.toFunctionBlock(body))))));
      return implementor.result(physType, builder.toBlock());
    }

    public RexProgram getProgram() {
      return program;
    }
  }

  public static final EnumerableAggregateRule ENUMERABLE_AGGREGATE_RULE =
      new EnumerableAggregateRule();

  /**
   * Rule to convert an {@link org.eigenbase.rel.AggregateRel} to an
   * {@link net.hydromatic.optiq.rules.java.JavaRules.EnumerableAggregateRel}.
   */
  private static class EnumerableAggregateRule
      extends ConverterRule {
    private EnumerableAggregateRule() {
      super(
          AggregateRel.class,
          Convention.NONE,
          EnumerableConvention.INSTANCE,
          "EnumerableAggregateRule");
    }

    public RelNode convert(RelNode rel) {
      final AggregateRel agg = (AggregateRel) rel;
      final RelTraitSet traitSet =
          agg.getTraitSet().replace(EnumerableConvention.INSTANCE);
      try {
        return new EnumerableAggregateRel(
            rel.getCluster(),
            traitSet,
            convert(agg.getChild(), traitSet),
            agg.getGroupSet(),
            agg.getAggCallList());
      } catch (InvalidRelException e) {
        LOGGER.fine(e.toString());
        return null;
      }
    }
  }

  /** Implementation of {@link org.eigenbase.rel.AggregateRel} in
   * {@link EnumerableConvention enumerable calling convention}. */
  public static class EnumerableAggregateRel
      extends AggregateRelBase
      implements EnumerableRel {
    private static final List<Aggregation> SUPPORTED_AGGREGATIONS =
        Arrays.<Aggregation>asList(
            SqlStdOperatorTable.COUNT,
            SqlStdOperatorTable.MIN,
            SqlStdOperatorTable.MAX,
            SqlStdOperatorTable.SUM);

    public EnumerableAggregateRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode child,
        BitSet groupSet,
        List<AggregateCall> aggCalls)
      throws InvalidRelException {
      super(cluster, traitSet, child, groupSet, aggCalls);
      assert getConvention() instanceof EnumerableConvention;

      for (AggregateCall aggCall : aggCalls) {
        if (aggCall.isDistinct()) {
          throw new InvalidRelException(
              "distinct aggregation not supported");
        }
        AggImplementor implementor2 =
            RexImpTable.INSTANCE.get(aggCall.getAggregation());
        if (implementor2 == null) {
          throw new InvalidRelException(
              "aggregation " + aggCall.getAggregation() + " not supported");
        }
      }
    }

    @Override public EnumerableAggregateRel copy(RelTraitSet traitSet,
        RelNode input, BitSet groupSet, List<AggregateCall> aggCalls) {
      try {
        return new EnumerableAggregateRel(getCluster(), traitSet, input,
            groupSet, aggCalls);
      } catch (InvalidRelException e) {
        // Semantic error not possible. Must be a bug. Convert to
        // internal error.
        throw new AssertionError(e);
      }
    }

    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
      final JavaTypeFactory typeFactory = implementor.getTypeFactory();
      final BlockBuilder builder = new BlockBuilder();
      final RexToLixTranslator translator =
          RexToLixTranslator.forAggregation(typeFactory);
      final EnumerableRel child = (EnumerableRel) getChild();
      final Result result = implementor.visitChild(this, 0, child, pref);
      Expression childExp =
          builder.append(
              "child",
              result.block);
      RelDataType inputRowType = getChild().getRowType();

      final PhysType physType =
          PhysTypeImpl.of(
              typeFactory, getRowType(), pref.preferCustom());

      // final Enumerable<Employee> child = <<child impl>>;
      // Function1<Employee, Integer> keySelector =
      //     new Function1<Employee, Integer>() {
      //         public Integer apply(Employee a0) {
      //             return a0.deptno;
      //         }
      //     };
      // Function1<Employee, Object[]> accumulatorInitializer =
      //     new Function1<Employee, Object[]>() {
      //         public Object[] apply(Employee a0) {
      //             return new Object[] {0, 0};
      //         }
      //     };
      // Function2<Object[], Employee, Object[]> accumulatorAdder =
      //     new Function2<Object[], Employee, Object[]>() {
      //         public Object[] apply(Object[] a1, Employee a0) {
      //              a1[0] = ((Integer) a1[0]) + 1;
      //              a1[1] = ((Integer) a1[1]) + a0.salary;
      //             return a1;
      //         }
      //     };
      // Function2<Integer, Object[], Object[]> resultSelector =
      //     new Function2<Integer, Object[], Object[]>() {
      //         public Object[] apply(Integer a0, Object[] a1) {
      //             return new Object[] { a0, a1[0], a1[1] };
      //         }
      //     };
      // return childEnumerable
      //     .groupBy(
      //        keySelector, accumulatorInitializer, accumulatorAdder,
      //        resultSelector);
      //
      // or, if key has 0 columns,
      //
      // return childEnumerable
      //     .aggregate(
      //       accumulatorInitializer.apply(),
      //       accumulatorAdder,
      //       resultSelector);
      //
      // with a slightly different resultSelector; or if there are no aggregate
      // functions
      //
      // final Enumerable<Employee> child = <<child impl>>;
      // Function1<Employee, Integer> keySelector =
      //     new Function1<Employee, Integer>() {
      //         public Integer apply(Employee a0) {
      //             return a0.deptno;
      //         }
      //     };
      // EqualityComparer<Employee> equalityComparer =
      //     new EqualityComparer<Employee>() {
      //         boolean equal(Employee a0, Employee a1) {
      //             return a0.deptno;
      //         }
      //     };
      // return child
      //     .distinct(equalityComparer);

      PhysType inputPhysType = result.physType;

      ParameterExpression parameter =
          Expressions.parameter(inputPhysType.getJavaRowType(), "a0");

      final List<Expression> keyExpressions = Expressions.list();
      PhysType keyPhysType =
          inputPhysType.project(
              BitSets.toList(groupSet), JavaRowFormat.LIST);
      final int keyArity = groupSet.cardinality();
      for (int groupKey : BitSets.toIter(groupSet)) {
        keyExpressions.add(
            inputPhysType.fieldReference(parameter, groupKey));
      }
      final Expression keySelector =
          builder.append(
              "keySelector",
              inputPhysType.generateSelector(
                  parameter,
                  BitSets.toList(groupSet),
                  keyPhysType.getFormat()));

      final List<AggImplementor> implementors =
          EnumUtil.getImplementors(aggCalls);

      // Function0<Object[]> accumulatorInitializer =
      //     new Function0<Object[]>() {
      //         public Object[] apply() {
      //             return new Object[] {0, 0};
      //         }
      //     };
      final List<Expression> initExpressions =
          new ArrayList<Expression>();
      for (Ord<Pair<AggregateCall, AggImplementor>> ord
          : Ord.zip(Pair.zip(aggCalls, implementors))) {
        initExpressions.add(
            ord.e.right.implementInit(translator, ord.e.left.getAggregation(),
                physType.fieldClass(keyArity + ord.i),
                EnumUtil.fieldTypes(typeFactory,
                    inputRowType,
                    ord.e.left.getArgList())));
      }

      final PhysType accPhysType =
          PhysTypeImpl.of(
              typeFactory,
              typeFactory.createSyntheticType(
                  new AbstractList<Type>() {
                    public Type get(int index) {
                      return initExpressions.get(index).getType();
                    }

                    public int size() {
                      return initExpressions.size();
                    }
                  }));

      final Expression accumulatorInitializer =
          builder.append(
              "accumulatorInitializer",
              Expressions.lambda(
                  Function0.class,
                  accPhysType.record(initExpressions)));

      // Function2<Object[], Employee, Object[]> accumulatorAdder =
      //     new Function2<Object[], Employee, Object[]>() {
      //         public Object[] apply(Object[] acc, Employee in) {
      //              acc[0] = ((Integer) acc[0]) + 1;
      //              acc[1] = ((Integer) acc[1]) + in.salary;
      //             return acc;
      //         }
      //     };
      BlockBuilder builder2 = new BlockBuilder();
      final ParameterExpression inParameter =
          Expressions.parameter(inputPhysType.getJavaRowType(), "in");
      final ParameterExpression acc_ =
          Expressions.parameter(accPhysType.getJavaRowType(), "acc");
      for (Ord<Pair<AggregateCall, AggImplementor>> ord
          : Ord.zip(Pair.zip(aggCalls, implementors))) {
        final Type type = initExpressions.get(ord.i).type;
        final Expression accumulator =
            accPhysType.fieldReference(acc_, ord.i);
        final List<Expression> conditions = new ArrayList<Expression>();
        for (int arg : ord.e.left.getArgList()) {
          if (inputPhysType.fieldNullable(arg)) {
            conditions.add(
                Expressions.notEqual(
                    inputPhysType.fieldReference(inParameter, arg),
                    Expressions.constant(null)));
          }
        }
        final Statement assign =
            Expressions.statement(
                Expressions.assign(accumulator,
                    ord.e.right.implementAdd(translator,
                        ord.e.left.getAggregation(),
                        Types.castIfNecessary(type, accumulator),
                        inputPhysType.accessors(
                            inParameter, ord.e.left.getArgList()))));
        if (conditions.isEmpty()) {
          builder2.add(assign);
        } else {
          builder2.add(
              Expressions.ifThen(
                  Expressions.foldAnd(conditions), assign));
        }
      }
      builder2.add(acc_);
      final Expression accumulatorAdder =
          builder.append(
              "accumulatorAdder",
              Expressions.lambda(
                  Function2.class,
                  builder2.toBlock(),
                  acc_,
                  inParameter));

      // Function2<Integer, Object[], Object[]> resultSelector =
      //     new Function2<Integer, Object[], Object[]>() {
      //         public Object[] apply(Integer key, Object[] acc) {
      //             return new Object[] { key, acc[0], acc[1] };
      //         }
      //     };
      final List<Expression> results = Expressions.list();
      final ParameterExpression key_;
      if (keyArity == 0) {
        key_ = null;
      } else {
        final Type keyType = keyPhysType.getJavaRowType();
        key_ = Expressions.parameter(keyType, "key");
        for (int j = 0; j < keyArity; j++) {
          results.add(
              keyPhysType.fieldReference(key_, j));
        }
      }
      for (Ord<Pair<AggregateCall, AggImplementor>> ord
          : Ord.zip(Pair.zip(aggCalls, implementors))) {
        results.add(
            ord.e.right.implementResult(translator, ord.e.left.getAggregation(),
                accPhysType.fieldReference(
                    acc_, ord.i)));
      }
      final PhysType resultPhysType = physType;
      if (keyArity == 0) {
        final Expression resultSelector =
            builder.append(
                "resultSelector",
                Expressions.lambda(
                    Function1.class,
                    resultPhysType.record(results),
                    acc_));
        builder.add(
            Expressions.return_(
                null,
                Expressions.call(
                    BuiltinMethod.SINGLETON_ENUMERABLE.method,
                    Expressions.call(
                        childExp,
                        BuiltinMethod.AGGREGATE.method,
                        Expressions.call(accumulatorInitializer, "apply"),
                        accumulatorAdder,
                        resultSelector))));
      } else if (aggCalls.isEmpty()) {
        builder.add(
            Expressions.return_(
                null,
                Expressions.call(
                    childExp,
                    BuiltinMethod.DISTINCT.method,
                    Expressions.<Expression>list()
                        .appendIfNotNull(
                            keyPhysType.comparer()))));
      } else {
        final Expression resultSelector =
            builder.append(
                "resultSelector",
                Expressions.lambda(
                    Function2.class,
                    resultPhysType.record(results),
                    key_,
                    acc_));
        builder.add(
            Expressions.return_(
                null,
                Expressions.call(
                    childExp,
                    BuiltinMethod.GROUP_BY2.method,
                    Expressions
                        .list(
                            keySelector,
                            accumulatorInitializer,
                            accumulatorAdder,
                            resultSelector)
                        .appendIfNotNull(
                            keyPhysType.comparer()))));
      }
      return implementor.result(physType, builder.toBlock());
    }
  }

  public static final EnumerableSortRule ENUMERABLE_SORT_RULE =
      new EnumerableSortRule();

  /**
   * Rule to convert an {@link org.eigenbase.rel.SortRel} to an
   * {@link net.hydromatic.optiq.rules.java.JavaRules.EnumerableSortRel}.
   */
  private static class EnumerableSortRule
      extends ConverterRule {
    private EnumerableSortRule() {
      super(
          SortRel.class,
          Convention.NONE,
          EnumerableConvention.INSTANCE,
          "EnumerableSortRule");
    }

    public RelNode convert(RelNode rel) {
      final SortRel sort = (SortRel) rel;
      if (sort.offset != null || sort.fetch != null) {
        return null;
      }
      final RelTraitSet traitSet =
          sort.getTraitSet().replace(EnumerableConvention.INSTANCE);
      final RelNode input = sort.getChild();
      return new EnumerableSortRel(
          rel.getCluster(),
          traitSet,
          convert(
              input,
              input.getTraitSet().replace(EnumerableConvention.INSTANCE)),
          sort.getCollation(),
          null,
          null);
    }
  }

  /** Implementation of {@link org.eigenbase.rel.SortRel} in
   * {@link EnumerableConvention enumerable calling convention}. */
  public static class EnumerableSortRel
      extends SortRel
      implements EnumerableRel {
    public EnumerableSortRel(RelOptCluster cluster, RelTraitSet traitSet,
        RelNode child, RelCollation collation, RexNode offset, RexNode fetch) {
      super(cluster, traitSet, child, collation, offset, fetch);
      assert getConvention() instanceof EnumerableConvention;
      assert getConvention() == child.getConvention();
    }

    @Override
    public EnumerableSortRel copy(
        RelTraitSet traitSet,
        RelNode newInput,
        RelCollation newCollation,
        RexNode offset,
        RexNode fetch) {
      return new EnumerableSortRel(
          getCluster(),
          traitSet,
          newInput,
          newCollation,
          offset,
          fetch);
    }

    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
      final BlockBuilder builder = new BlockBuilder();
      final EnumerableRel child = (EnumerableRel) getChild();
      final Result result = implementor.visitChild(this, 0, child, pref);
      final PhysType physType =
          PhysTypeImpl.of(
              implementor.getTypeFactory(),
              getRowType(),
              result.format);
      Expression childExp =
          builder.append(
              "child", result.block);

      PhysType inputPhysType = result.physType;
      final Pair<Expression, Expression> pair =
          inputPhysType.generateCollationKey(
              collation.getFieldCollations());

      builder.add(
          Expressions.return_(
              null,
              Expressions.call(
                  childExp,
                  BuiltinMethod.ORDER_BY.method,
                  Expressions.list(
                      builder.append("keySelector", pair.left))
                  .appendIfNotNull(builder.appendIfNotNull("comparator",
                      pair.right)))));
      return implementor.result(physType, builder.toBlock());
    }
  }

  public static final EnumerableLimitRule ENUMERABLE_LIMIT_RULE =
      new EnumerableLimitRule();

  /**
   * Rule to convert an {@link org.eigenbase.rel.SortRel} that has
   * {@code offset} or {@code fetch} set to an
   * {@link net.hydromatic.optiq.rules.java.JavaRules.EnumerableLimitRel}
   * on top of a "pure" {@code SortRel} that has no offset or fetch.
   */
  private static class EnumerableLimitRule
      extends RelOptRule {
    private EnumerableLimitRule() {
      super(
          operand(SortRel.class, any()),
          "EnumerableLimitRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final SortRel sort = call.rel(0);
      if (sort.offset == null && sort.fetch == null) {
        return;
      }
      final RelTraitSet traitSet =
          sort.getTraitSet().replace(EnumerableConvention.INSTANCE);
      RelNode input = sort.getChild();
      if (!sort.getCollation().getFieldCollations().isEmpty()) {
        // Create a sort with the same sort key, but no offset or fetch.
        input = sort.copy(
            sort.getTraitSet(),
            input,
            sort.getCollation(),
            null,
            null);
      }
      RelNode x = convert(
          input,
          input.getTraitSet().replace(EnumerableConvention.INSTANCE));
      call.transformTo(
          new EnumerableLimitRel(
              sort.getCluster(),
              traitSet,
              x,
              sort.offset,
              sort.fetch));
    }
  }

  /** Relational expression that applies a limit and/or offset to its input. */
  public static class EnumerableLimitRel
      extends SingleRel
      implements EnumerableRel {
    private final RexNode offset;
    private final RexNode fetch;

    public EnumerableLimitRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode child,
        RexNode offset,
        RexNode fetch) {
      super(cluster, traitSet, child);
      this.offset = offset;
      this.fetch = fetch;
      assert getConvention() instanceof EnumerableConvention;
      assert getConvention() == child.getConvention();
    }

    @Override
    public EnumerableLimitRel copy(
        RelTraitSet traitSet,
        List<RelNode> newInputs) {
      return new EnumerableLimitRel(
          getCluster(),
          traitSet,
          sole(newInputs),
          offset,
          fetch);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
      return super.explainTerms(pw)
          .itemIf("offset", offset, offset != null)
          .itemIf("fetch", fetch, fetch != null);
    }

    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
      final BlockBuilder builder = new BlockBuilder();
      final EnumerableRel child = (EnumerableRel) getChild();
      final Result result = implementor.visitChild(this, 0, child, pref);
      final PhysType physType =
          PhysTypeImpl.of(
              implementor.getTypeFactory(),
              getRowType(),
              result.format);
      Expression childExp =
          builder.append(
              "child", result.block);

      Expression v = childExp;
      if (offset != null) {
        v = builder.append(
            "offset",
            Expressions.call(
                v,
                BuiltinMethod.SKIP.method,
                Expressions.constant(RexLiteral.intValue(offset))));
      }
      if (fetch != null) {
        v = builder.append(
            "fetch",
            Expressions.call(
                v,
                BuiltinMethod.TAKE.method,
                Expressions.constant(RexLiteral.intValue(fetch))));
      }

      builder.add(
          Expressions.return_(
              null,
              v));
      return implementor.result(physType, builder.toBlock());
    }
  }

  public static final EnumerableUnionRule ENUMERABLE_UNION_RULE =
      new EnumerableUnionRule();

  /**
   * Rule to convert an {@link org.eigenbase.rel.UnionRel} to an
   * {@link net.hydromatic.optiq.rules.java.JavaRules.EnumerableUnionRel}.
   */
  private static class EnumerableUnionRule
      extends ConverterRule {
    private EnumerableUnionRule() {
      super(
          UnionRel.class,
          Convention.NONE,
          EnumerableConvention.INSTANCE,
          "EnumerableUnionRule");
    }

    public RelNode convert(RelNode rel) {
      final UnionRel union = (UnionRel) rel;
      final EnumerableConvention out = EnumerableConvention.INSTANCE;
      final RelTraitSet traitSet = union.getTraitSet().replace(out);
      return new EnumerableUnionRel(rel.getCluster(), traitSet,
          convertList(union.getInputs(), out), union.all);
    }
  }

  /** Implementation of {@link org.eigenbase.rel.UnionRel} in
   * {@link EnumerableConvention enumerable calling convention}. */
  public static class EnumerableUnionRel
      extends UnionRelBase
      implements EnumerableRel {
    public EnumerableUnionRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelNode> inputs,
        boolean all) {
      super(cluster, traitSet, inputs, all);
    }

    public EnumerableUnionRel copy(
        RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
      return new EnumerableUnionRel(getCluster(), traitSet, inputs, all);
    }

    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
      final BlockBuilder builder = new BlockBuilder();
      Expression unionExp = null;
      for (Ord<RelNode> ord : Ord.zip(inputs)) {
        EnumerableRel input = (EnumerableRel) ord.e;
        final Result result = implementor.visitChild(this, ord.i, input, pref);
        Expression childExp =
            builder.append(
                "child" + ord.i,
                result.block);

        if (unionExp == null) {
          unionExp = childExp;
        } else {
          unionExp =
              Expressions.call(
                  unionExp,
                  all
                      ? BuiltinMethod.CONCAT.method
                      : BuiltinMethod.UNION.method,
                  childExp);
        }

        // Once the first input has chosen its format, ask for the same for
        // other inputs.
        pref = pref.of(result.format);
      }

      builder.add(unionExp);
      final PhysType physType =
          PhysTypeImpl.of(
              implementor.getTypeFactory(),
              getRowType(),
              pref.prefer(JavaRowFormat.CUSTOM));
      return implementor.result(physType, builder.toBlock());
    }
  }

  public static final EnumerableIntersectRule ENUMERABLE_INTERSECT_RULE =
      new EnumerableIntersectRule();

  /**
   * Rule to convert an {@link org.eigenbase.rel.IntersectRel} to an
   * {@link net.hydromatic.optiq.rules.java.JavaRules.EnumerableIntersectRel}.
   */
  private static class EnumerableIntersectRule
      extends ConverterRule {
    private EnumerableIntersectRule() {
      super(
          IntersectRel.class,
          Convention.NONE,
          EnumerableConvention.INSTANCE,
          "EnumerableIntersectRule");
    }

    public RelNode convert(RelNode rel) {
      final IntersectRel intersect = (IntersectRel) rel;
      if (intersect.all) {
        return null; // INTERSECT ALL not implemented
      }
      final EnumerableConvention out = EnumerableConvention.INSTANCE;
      final RelTraitSet traitSet = intersect.getTraitSet().replace(out);
      return new EnumerableIntersectRel(rel.getCluster(), traitSet,
          convertList(intersect.getInputs(), out), intersect.all);
    }
  }

  /** Implementation of {@link org.eigenbase.rel.IntersectRel} in
   * {@link EnumerableConvention enumerable calling convention}. */
  public static class EnumerableIntersectRel
      extends IntersectRelBase
      implements EnumerableRel {
    public EnumerableIntersectRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelNode> inputs,
        boolean all) {
      super(cluster, traitSet, inputs, all);
      assert !all;
    }

    public EnumerableIntersectRel copy(
        RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
      return new EnumerableIntersectRel(
          getCluster(),
          traitSet,
          inputs,
          all);
    }

    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
      final BlockBuilder builder = new BlockBuilder();
      Expression intersectExp = null;
      for (Ord<RelNode> ord : Ord.zip(inputs)) {
        EnumerableRel input = (EnumerableRel) ord.e;
        final Result result = implementor.visitChild(this, ord.i, input, pref);
        Expression childExp =
            builder.append(
                "child" + ord.i,
                result.block);

        if (intersectExp == null) {
          intersectExp = childExp;
        } else {
          intersectExp =
              Expressions.call(
                  intersectExp,
                  all
                      ? BuiltinMethod.CONCAT.method
                      : BuiltinMethod.INTERSECT.method,
                  childExp);
        }

        // Once the first input has chosen its format, ask for the same for
        // other inputs.
        pref = pref.of(result.format);
      }

      builder.add(intersectExp);
      final PhysType physType =
          PhysTypeImpl.of(
              implementor.getTypeFactory(),
              getRowType(),
              pref.prefer(JavaRowFormat.CUSTOM));
      return implementor.result(physType, builder.toBlock());
    }
  }

  public static final EnumerableMinusRule ENUMERABLE_MINUS_RULE =
      new EnumerableMinusRule();

  /**
   * Rule to convert an {@link org.eigenbase.rel.MinusRel} to an
   * {@link net.hydromatic.optiq.rules.java.JavaRules.EnumerableMinusRel}.
   */
  private static class EnumerableMinusRule
      extends ConverterRule {
    private EnumerableMinusRule() {
      super(
          MinusRel.class,
          Convention.NONE,
          EnumerableConvention.INSTANCE,
          "EnumerableMinusRule");
    }

    public RelNode convert(RelNode rel) {
      final MinusRel minus = (MinusRel) rel;
      if (minus.all) {
        return null; // EXCEPT ALL not implemented
      }
      final EnumerableConvention out = EnumerableConvention.INSTANCE;
      final RelTraitSet traitSet =
          rel.getTraitSet().replace(
              EnumerableConvention.INSTANCE);
      return new EnumerableMinusRel(rel.getCluster(), traitSet,
          convertList(minus.getInputs(), out), minus.all);
    }
  }

  /** Implementation of {@link org.eigenbase.rel.MinusRel} in
   * {@link EnumerableConvention enumerable calling convention}. */
  public static class EnumerableMinusRel
      extends MinusRelBase
      implements EnumerableRel {
    public EnumerableMinusRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelNode> inputs,
        boolean all) {
      super(cluster, traitSet, inputs, all);
      assert !all;
    }

    public EnumerableMinusRel copy(
        RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
      return new EnumerableMinusRel(
          getCluster(),
          traitSet,
          inputs,
          all);
    }

    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
      final BlockBuilder builder = new BlockBuilder();
      Expression minusExp = null;
      for (Ord<RelNode> ord : Ord.zip(inputs)) {
        EnumerableRel input = (EnumerableRel) ord.e;
        final Result result = implementor.visitChild(this, ord.i, input, pref);
        Expression childExp =
            builder.append(
                "child" + ord.i,
                result.block);

        if (minusExp == null) {
          minusExp = childExp;
        } else {
          minusExp =
              Expressions.call(
                  minusExp,
                  BuiltinMethod.EXCEPT.method,
                  childExp);
        }

        // Once the first input has chosen its format, ask for the same for
        // other inputs.
        pref = pref.of(result.format);
      }

      builder.add(minusExp);
      final PhysType physType =
          PhysTypeImpl.of(
              implementor.getTypeFactory(),
              getRowType(),
              pref.prefer(JavaRowFormat.CUSTOM));
      return implementor.result(physType, builder.toBlock());
    }
  }

  public static final EnumerableTableModificationRule
  ENUMERABLE_TABLE_MODIFICATION_RULE =
      new EnumerableTableModificationRule();

  public static class EnumerableTableModificationRule extends ConverterRule {
    private EnumerableTableModificationRule() {
      super(
          TableModificationRel.class,
          Convention.NONE,
          EnumerableConvention.INSTANCE,
          "EnumerableTableModificationRule");
    }

    @Override
    public RelNode convert(RelNode rel) {
      final TableModificationRel modify =
          (TableModificationRel) rel;
      final ModifiableTable modifiableTable =
          modify.getTable().unwrap(ModifiableTable.class);
      if (modifiableTable == null) {
        return null;
      }
      final RelTraitSet traitSet =
          modify.getTraitSet().replace(EnumerableConvention.INSTANCE);
      return new EnumerableTableModificationRel(
          modify.getCluster(), traitSet,
          modify.getTable(),
          modify.getCatalogReader(),
          convert(modify.getChild(), traitSet),
          modify.getOperation(),
          modify.getUpdateColumnList(),
          modify.isFlattened());
    }
  }

  /** Implementation of {@link org.eigenbase.rel.TableModificationRel} in
   * {@link EnumerableConvention enumerable calling convention}. */
  public static class EnumerableTableModificationRel
      extends TableModificationRelBase
      implements EnumerableRel {
    public EnumerableTableModificationRel(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelOptTable table,
        Prepare.CatalogReader catalogReader,
        RelNode child,
        Operation operation,
        List<String> updateColumnList,
        boolean flattened) {
      super(
          cluster,
          traits,
          table,
          catalogReader,
          child,
          operation,
          updateColumnList,
          flattened);
      assert child.getConvention() instanceof EnumerableConvention;
      assert getConvention() instanceof EnumerableConvention;
      final ModifiableTable modifiableTable =
          table.unwrap(ModifiableTable.class);
      if (modifiableTable == null) {
        throw new AssertionError(); // TODO: user error in validator
      }
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new EnumerableTableModificationRel(
          getCluster(),
          traitSet,
          getTable(),
          getCatalogReader(),
          sole(inputs),
          getOperation(),
          getUpdateColumnList(),
          isFlattened());
    }

    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
      final BlockBuilder builder = new BlockBuilder();
      final Result result = implementor.visitChild(
          this, 0, (EnumerableRel) getChild(), pref);
      Expression childExp =
          builder.append(
              "child", result.block);
      final ParameterExpression collectionParameter =
          Expressions.parameter(Collection.class,
              builder.newName("collection"));
      final Expression expression = table.getExpression(ModifiableTable.class);
      assert expression != null; // TODO: user error in validator
      assert ModifiableTable.class.isAssignableFrom(
          Types.toClass(expression.getType())) : expression.getType();
      builder.add(
          Expressions.declare(
              0,
              collectionParameter,
              Expressions.call(
                  expression,
                  BuiltinMethod.MODIFIABLE_TABLE_GET_MODIFIABLE_COLLECTION
                      .method)));
      final Expression countParameter =
          builder.append(
              "count",
              Expressions.call(collectionParameter, "size"),
              false);
      Expression convertedChildExp;
      if (!getChild().getRowType().equals(getRowType())) {
        final JavaTypeFactory typeFactory =
            (JavaTypeFactory) getCluster().getTypeFactory();
        PhysType physType =
            PhysTypeImpl.of(
                typeFactory,
                table.getRowType(),
                JavaRowFormat.CUSTOM);
        List<Expression> expressionList = new ArrayList<Expression>();
        final PhysType childPhysType = result.physType;
        final ParameterExpression o_ =
            Expressions.parameter(childPhysType.getJavaRowType(), "o");
        final int fieldCount =
            childPhysType.getRowType().getFieldCount();
        for (int i = 0; i < fieldCount; i++) {
          expressionList.add(childPhysType.fieldReference(o_, i));
        }
        convertedChildExp =
            builder.append(
                "convertedChild",
                Expressions.call(
                    childExp,
                    BuiltinMethod.SELECT.method,
                    Expressions.lambda(
                        physType.record(expressionList), o_)));
      } else {
        convertedChildExp = childExp;
      }
      builder.add(
          Expressions.statement(
              Expressions.call(
                  convertedChildExp, "into", collectionParameter)));
      builder.add(
          Expressions.return_(
              null,
              Expressions.call(
                  BuiltinMethod.SINGLETON_ENUMERABLE.method,
                  Expressions.convert_(
                      Expressions.subtract(
                          Expressions.call(
                              collectionParameter, "size"),
                          countParameter),
                      long.class))));
      final PhysType physType =
          PhysTypeImpl.of(
              implementor.getTypeFactory(),
              getRowType(),
              pref == Prefer.ARRAY
                  ? JavaRowFormat.ARRAY : JavaRowFormat.SCALAR);
      return implementor.result(physType, builder.toBlock());
    }
  }

  public static final EnumerableValuesRule ENUMERABLE_VALUES_RULE =
      new EnumerableValuesRule();

  public static class EnumerableValuesRule extends ConverterRule {
    private EnumerableValuesRule() {
      super(
          ValuesRel.class,
          Convention.NONE,
          EnumerableConvention.INSTANCE,
          "EnumerableValuesRule");
    }

    @Override
    public RelNode convert(RelNode rel) {
      ValuesRel valuesRel = (ValuesRel) rel;
      return new EnumerableValuesRel(
          valuesRel.getCluster(),
          valuesRel.getRowType(),
          valuesRel.getTuples(),
          valuesRel.getTraitSet().replace(EnumerableConvention.INSTANCE));
    }
  }

  public static final EnumerableOneRowRule ENUMERABLE_ONE_ROW_RULE =
      new EnumerableOneRowRule();

  public static class EnumerableOneRowRule extends ConverterRule {
    private EnumerableOneRowRule() {
      super(OneRowRel.class,
          Convention.NONE,
          EnumerableConvention.INSTANCE,
          "EnumerableOneRowRule");
    }

    public RelNode convert(RelNode rel) {
      OneRowRel oneRow = (OneRowRel) rel;
      RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
      return new EnumerableValuesRel(
          oneRow.getCluster(),
          oneRow.getRowType(),
          Collections.singletonList(
              Collections.singletonList(
                  rexBuilder.makeExactLiteral(BigDecimal.ZERO))),
          oneRow.getTraitSet().replace(EnumerableConvention.INSTANCE));
    }
  }

  public static final EnumerableEmptyRule ENUMERABLE_EMPTY_RULE =
      new EnumerableEmptyRule();

  public static class EnumerableEmptyRule extends ConverterRule {
    private EnumerableEmptyRule() {
      super(EmptyRel.class,
          Convention.NONE,
          EnumerableConvention.INSTANCE,
          "EnumerableEmptyRule");
    }

    public RelNode convert(RelNode rel) {
      EmptyRel empty = (EmptyRel) rel;
      return new EnumerableValuesRel(
          empty.getCluster(),
          empty.getRowType(),
          ImmutableList.<List<RexLiteral>>of(),
          empty.getTraitSet().replace(EnumerableConvention.INSTANCE));
    }
  }

  /** Implementation of {@link org.eigenbase.rel.ValuesRel} in
   * {@link EnumerableConvention enumerable calling convention}. */
  public static class EnumerableValuesRel
      extends ValuesRelBase
      implements EnumerableRel {
    EnumerableValuesRel(
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
      return new EnumerableValuesRel(
          getCluster(), rowType, tuples, traitSet);
    }

    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
/*
            return Linq4j.asEnumerable(
                new Object[][] {
                    new Object[] {1, 2},
                    new Object[] {3, 4}
                });
*/
      final JavaTypeFactory typeFactory =
          (JavaTypeFactory) getCluster().getTypeFactory();
      final BlockBuilder builder = new BlockBuilder();
      final PhysType physType =
          PhysTypeImpl.of(
              implementor.getTypeFactory(),
              getRowType(),
              pref.preferCustom());
      final Type rowClass = physType.getJavaRowType();

      final List<Expression> expressions = new ArrayList<Expression>();
      final List<RelDataTypeField> fields = rowType.getFieldList();
      for (List<RexLiteral> tuple : tuples) {
        final List<Expression> literals = new ArrayList<Expression>();
        for (Pair<RelDataTypeField, RexLiteral> pair
            : Pair.zip(fields, tuple)) {
          literals.add(
              RexToLixTranslator.translateLiteral(
                  pair.right,
                  pair.left.getType(),
                  typeFactory,
                  RexImpTable.NullAs.NULL));
        }
        expressions.add(physType.record(literals));
      }
      builder.add(
          Expressions.return_(
              null,
              Expressions.call(
                  BuiltinMethod.AS_ENUMERABLE.method,
                  Expressions.newArrayInit(
                      Primitive.box(rowClass), expressions))));
      return implementor.result(physType, builder.toBlock());
    }
  }

  public static final EnumerableWindowRule ENUMERABLE_WINDOW_RULE =
      new EnumerableWindowRule();

  /**
   * Rule to convert an {@link org.eigenbase.rel.AggregateRel} to an
   * {@link net.hydromatic.optiq.rules.java.JavaRules.EnumerableAggregateRel}.
   */
  private static class EnumerableWindowRule
      extends ConverterRule {
    private EnumerableWindowRule() {
      super(WindowRel.class, Convention.NONE, EnumerableConvention.INSTANCE,
          "EnumerableWindowRule");
    }

    public RelNode convert(RelNode rel) {
      final WindowRel winAgg = (WindowRel) rel;
      final RelTraitSet traitSet =
          winAgg.getTraitSet().replace(EnumerableConvention.INSTANCE);
      final RelNode child = winAgg.getChild();
      final RelNode convertedChild =
          convert(child,
              child.getTraitSet().replace(EnumerableConvention.INSTANCE));
      return new EnumerableWindowRel(rel.getCluster(), traitSet, convertedChild,
          winAgg.getRowType(), winAgg.windows);
    }
  }

  /** Implementation of {@link org.eigenbase.rel.WindowRel} in
   * {@link EnumerableConvention enumerable calling convention}. */
  public static class EnumerableWindowRel extends WindowRelBase
      implements EnumerableRel {
    /** Creates an EnumerableWindowRel. */
    EnumerableWindowRel(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode child,
        RelDataType rowType,
        List<WindowRel.Window> windows) {
      super(cluster, traits, child, rowType, windows);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new EnumerableWindowRel(getCluster(), traitSet, sole(inputs),
          rowType, windows);
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner) {
      // Cost is proportional to the number of rows and the number of
      // components (windows and aggregate functions). There is
      // no I/O cost.
      //
      // TODO #1. Add memory cost.
      // TODO #2. MIN and MAX have higher CPU cost than SUM and COUNT.
      final double rowsIn = RelMetadataQuery.getRowCount(getChild());
      int count = windows.size();
      for (WindowRel.Window window : windows) {
        count += window.aggCalls.size();
      }
      return planner.getCostFactory().makeCost(rowsIn, rowsIn * count, 0);
    }

    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
      final JavaTypeFactory typeFactory = implementor.getTypeFactory();
      final EnumerableRel child = (EnumerableRel) getChild();
      final RexToLixTranslator translator =
          RexToLixTranslator.forAggregation(typeFactory);
      final BlockBuilder builder = new BlockBuilder();
      final Result result = implementor.visitChild(this, 0, child, pref);
      Expression source_ = builder.append("source", result.block);

      PhysType inputPhysType = result.physType;

      for (Window window : windows) {
        // Comparator:
        // final Comparator<JdbcTest.Employee> comparator =
        //    new Comparator<JdbcTest.Employee>() {
        //      public int compare(JdbcTest.Employee o1,
        //          JdbcTest.Employee o2) {
        //        return Integer.compare(o1.empid, o2.empid);
        //      }
        //    };
        final Expression comparator_ =
            builder.append(
                "comparator",
                inputPhysType.generateComparator(
                    window.collation()));

        // Populate map of lists, one per partition
        //   final Map<Integer, List<Employee>> multiMap =
        //     new SortedMultiMap<Integer, List<Employee>>();
        //    source.foreach(
        //      new Function1<Employee, Void>() {
        //        public Void apply(Employee v) {
        //          final Integer k = v.deptno;
        //          multiMap.putMulti(k, v);
        //          return null;
        //        }
        //      });
        //   final List<Xxx> list = new ArrayList<Xxx>(multiMap.size());
        //   Iterator<Employee[]> iterator = multiMap.arrays(comparator);
        //
        final Expression collectionExpr;
        final Expression iterator_;
        if (window.groupSet.isEmpty()) {
          // If partition key is empty, no need to partition.
          //
          //   final List<Employee> tempList =
          //       source.into(new ArrayList<Employee>());
          //   Iterator<Employee[]> iterator =
          //       SortedMultiMap.singletonArrayIterator(comparator, tempList);
          //   final List<Xxx> list = new ArrayList<Xxx>(tempList.size());

          final Expression tempList_ = builder.append(
              "tempList",
              Expressions.convert_(
                  Expressions.call(
                      source_,
                      BuiltinMethod.INTO.method,
                      Expressions.new_(ArrayList.class)),
                  List.class));
          iterator_ = builder.append(
              "iterator",
              Expressions.call(
                  null,
                  BuiltinMethod.SORTED_MULTI_MAP_SINGLETON.method,
                  comparator_,
                  tempList_));

          collectionExpr = tempList_;
        } else {
          Expression multiMap_ =
              builder.append(
                  "multiMap", Expressions.new_(SortedMultiMap.class));
          final BlockBuilder builder2 = new BlockBuilder();
          final ParameterExpression v_ =
              Expressions.parameter(inputPhysType.getJavaRowType(),
                  builder2.newName("v"));
          final DeclarationStatement declare =
              Expressions.declare(
                  0, "key",
                  inputPhysType.selector(
                      v_,
                      BitSets.toList(window.groupSet),
                      JavaRowFormat.CUSTOM));
          builder2.add(declare);
          final ParameterExpression key_ = declare.parameter;
          builder2.add(
              Expressions.statement(
                  Expressions.call(
                      multiMap_,
                      BuiltinMethod.SORTED_MULTI_MAP_PUT_MULTI.method,
                      key_,
                      v_)));
          builder2.add(
              Expressions.return_(
                  null, Expressions.constant(null)));

          builder.add(
              Expressions.statement(
                  Expressions.call(
                      source_,
                      BuiltinMethod.ENUMERABLE_FOREACH.method,
                      Expressions.lambda(
                          builder2.toBlock(), v_))));

          iterator_ = builder.append(
              "iterator",
              Expressions.call(
                  multiMap_,
                  BuiltinMethod.SORTED_MULTI_MAP_ARRAYS.method,
                  comparator_));

          collectionExpr = multiMap_;
        }

        // The output from this stage is the input plus the aggregate functions.
        final RelDataTypeFactory.FieldInfoBuilder typeBuilder =
            typeFactory.builder();
        typeBuilder.addAll(inputPhysType.getRowType().getFieldList());
        final int offset = typeBuilder.getFieldCount();
        final List<AggregateCall> aggregateCalls =
            window.getAggregateCalls(this);
        for (AggregateCall aggregateCall : aggregateCalls) {
          typeBuilder.add(aggregateCall.name, aggregateCall.type);
        }
        RelDataType outputRowType = typeBuilder.build();
        PhysType outputPhysType =
            PhysTypeImpl.of(
                typeFactory, outputRowType, pref.prefer(result.format));

        // For each list of rows that have the same partitioning key, evaluate
        // all of the windowed aggregate functions.
        //
        //   while (iterator.hasNext()) {
        //     // <builder3>
        //     Employee[] rows = iterator.next();
        //     int i = 0;
        //     while (i < rows.length) {
        //       // <builder4>
        //       JdbcTest.Employee row = rows[i];
        //       int sum = 0;
        //       int count = 0;
        //       int j = Math.max(0, i - 1);
        //       while (j <= i) {
        //         // <builder5>
        //         sum += rows[j].salary;
        //         ++count;
        //         ++j;
        //       }
        //       list.add(new Xxx(row.deptno, row.empid, sum, count));
        //       i++;
        //     }
        //     multiMap.clear(); // allows gc
        //   }
        //   source = Linq4j.asEnumerable(list);

        final Expression list_ =
            builder.append(
                "list",
                Expressions.new_(
                    ArrayList.class,
                    Expressions.call(
                        collectionExpr, BuiltinMethod.COLLECTION_SIZE.method)),
                false);

        final BlockBuilder builder3 = new BlockBuilder();
        Expression rows_ =
            builder3.append(
                "rows",
                Expressions.convert_(
                    Expressions.call(
                        iterator_, BuiltinMethod.ITERATOR_NEXT.method),
                    Object[].class),
                false);

        final BlockBuilder builder4 = new BlockBuilder();

        final ParameterExpression i_ =
            Expressions.parameter(int.class, builder4.newName("i"));

        final Expression row_ =
            builder4.append(
                "row",
                Expressions.convert_(
                    Expressions.arrayIndex(rows_, i_),
                    inputPhysType.getJavaRowType()));

        final List<AggImplementor> implementors =
            EnumUtil.getImplementors(aggregateCalls);
        final List<ParameterExpression> variables =
            new ArrayList<ParameterExpression>();
        for (Ord<Pair<AggregateCall, AggImplementor>> ord
            : Ord.zip(Pair.zip(aggregateCalls, implementors))) {
          final ParameterExpression parameter =
              Expressions.parameter(outputPhysType.fieldClass(offset + ord.i),
                  builder4.newName(ord.e.left.name));
          final Expression initExpression =
              ord.e.right.implementInit(translator, ord.e.left.getAggregation(),
                  parameter.type,
                  EnumUtil.fieldTypes(typeFactory,
                      inputPhysType.getRowType(),
                      ord.e.left.getArgList()));
          variables.add(parameter);
          builder4.add(Expressions.declare(0, parameter, initExpression));
        }

        final List<Expression> expressions = new ArrayList<Expression>();
        for (int i = 0; i < offset; i++) {
          expressions.add(
              Types.castIfNecessary(
                  inputPhysType.fieldClass(i),
                  inputPhysType.fieldReference(row_, i)));
        }

        final PhysType finalInputPhysType = inputPhysType;
        generateWindowLoop(translator, builder4, window, aggregateCalls,
            implementors, variables, rows_, i_, row_, expressions,
            new Function1<AggCallContext, Void>() {
              public Void apply(AggCallContext a0) {
                for (Ord<Pair<AggregateCall, AggImplementor>> ord
                    : Ord.zip(Pair.zip(aggregateCalls, implementors))) {
                  final Expression accumulator = variables.get(ord.i);
                  final List<Expression> conditions =
                      new ArrayList<Expression>();
                  for (int arg : ord.e.left.getArgList()) {
                    if (finalInputPhysType.fieldNullable(arg)) {
                      conditions.add(
                          Expressions.notEqual(
                              finalInputPhysType.fieldReference(row_, arg),
                              Expressions.constant(null)));
                    }
                  }
                  final Statement assign =
                      Expressions.statement(
                          Expressions.assign(accumulator,
                              ord.e.right.implementAdd(translator,
                                  ord.e.left.getAggregation(),
                                  accumulator,
                                  finalInputPhysType.accessors(
                                      row_, ord.e.left.getArgList()))));
                  if (conditions.isEmpty()) {
                    a0.builder().add(assign);
                  } else {
                    a0.builder().add(
                        Expressions.ifThen(
                            Expressions.foldAnd(conditions), assign));
                  }
                }
                return null;
              }
            });

        builder4.add(
            Expressions.statement(
                Expressions.call(
                    list_,
                    BuiltinMethod.COLLECTION_ADD.method,
                    outputPhysType.record(expressions))));

        builder3.add(
            Expressions.for_(
                Expressions.declare(0, i_, Expressions.constant(0)),
                Expressions.lessThan(
                    i_,
                    Expressions.field(rows_, "length")),
                Expressions.preIncrementAssign(i_),
                builder4.toBlock()));

        builder.add(
            Expressions.while_(
                Expressions.call(
                    iterator_,
                    BuiltinMethod.ITERATOR_HAS_NEXT.method),
                builder3.toBlock()));
        builder.add(
            Expressions.statement(
                Expressions.call(
                    collectionExpr,
                    BuiltinMethod.MAP_CLEAR.method)));

        // We're not assigning to "source". For each window, create a new
        // final variable called "source" or "sourceN".
        source_ =
            builder.append(
                "source",
                Expressions.call(
                    BuiltinMethod.AS_ENUMERABLE.method, list_));

        inputPhysType = outputPhysType;
      }

      //   return Linq4j.asEnumerable(list);
      builder.add(
          Expressions.return_(null, source_));
      return implementor.result(inputPhysType, builder.toBlock());
    }

    /** Generates the loop that computes aggregate functions over the window.
     * Calls a callback to increment each aggregate function's accumulator. */
    private void generateWindowLoop(RexToLixTranslator translator,
        BlockBuilder builder, Window window, List<AggregateCall> aggregateCalls,
        List<AggImplementor> implementors, List<ParameterExpression> variables,
        Expression rows_, ParameterExpression i_, Expression row_,
        List<Expression> expressions, Function1<AggCallContext, Void> f3) {
      //       int j = Math.max(0, i - 1);
      //       while (j <= i) {
      //         // <builder5>
      //         Employee row2 = rows[j];
      //         sum += rows[j].salary;
      //         ++count;
      //         ++j;
      //       }

      final Expression min_ = Expressions.constant(0);
      final Expression max_ =
          Expressions.subtract(Expressions.field(rows_, "length"),
              Expressions.constant(1));
      final SqlWindow.OffsetRange offsetAndRange =
          SqlWindow.getOffsetAndRange(
              window.lowerBound, window.upperBound, window.isRows);
      final Expression start_ =
          builder.append("start",
              optimizeAdd(i_,
                  (int) offsetAndRange.offset - (int) offsetAndRange.range,
                  min_, max_),
              false);
      final Expression end_ =
          builder.append("end",
              optimizeAdd(i_, (int) offsetAndRange.offset, min_, max_),
              false);
      final DeclarationStatement jDecl = Expressions.declare(0, "j", start_);
      final ParameterExpression j_ = jDecl.parameter;
      final Expression row2_ = builder.append("row2",
          Expressions.convert_(
              Expressions.arrayIndex(rows_, j_),
              row_.getType()));

      final BlockBuilder builder5 = new BlockBuilder();
      f3.apply(
          new AggCallContext() {
            public BlockBuilder builder() {
              return builder5;
            }

            public Expression index() {
              return j_;
            }

            public Expression current() {
              return row2_;
            }

            public Expression isFirst() {
              return Expressions.equal(j_, start_);
            }

            public Expression isLast() {
              return Expressions.equal(j_, end_);
            }
          });

      builder.add(
          Expressions.for_(
              jDecl,
              Expressions.lessThanOrEqual(j_, end_),
              Expressions.preIncrementAssign(j_),
              builder5.toBlock()));

      for (Ord<Pair<AggregateCall, AggImplementor>> ord
          : Ord.zip(Pair.zip(aggregateCalls, implementors))) {
        final AggImplementor implementor2 = ord.e.right;
        expressions.add(
            implementor2 instanceof WinAggImplementor
                ? ((WinAggImplementor) implementor2)
                .implementResultPlus(translator,
                    ord.e.left.getAggregation(), variables.get(ord.i),
                    start_, end_, rows_, i_)
                : implementor2.implementResult(translator,
                    ord.e.left.getAggregation(), variables.get(ord.i)));
      }
    }

    private Expression optimizeAdd(Expression i_,
        int offset,
        Expression min_,
        Expression max_) {
      if (offset == 0) {
        return i_;
      } else if (offset < 0) {
        return Expressions.call(null,
            BuiltinMethod.MATH_MAX.method,
            min_,
            Expressions.subtract(i_, Expressions.constant(-offset)));
      } else {
        return Expressions.call(null,
            BuiltinMethod.MATH_MIN.method,
            max_,
            Expressions.add(i_, Expressions.constant(offset)));
      }
    }
  }

  public static final EnumerableFilterToCalcRule
  ENUMERABLE_FILTER_TO_CALC_RULE =
      new EnumerableFilterToCalcRule();

  /** Variant of {@link org.eigenbase.rel.rules.FilterToCalcRule} for
   * {@link EnumerableConvention enumerable calling convention}. */
  public static class EnumerableFilterToCalcRule extends RelOptRule {
    private EnumerableFilterToCalcRule() {
      super(operand(EnumerableFilterRel.class, any()));
    }

    public void onMatch(RelOptRuleCall call) {
      final EnumerableFilterRel filter = call.rel(0);
      final RelNode rel = filter.getChild();

      // Create a program containing a filter.
      final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
      final RelDataType inputRowType = rel.getRowType();
      final RexProgramBuilder programBuilder =
          new RexProgramBuilder(inputRowType, rexBuilder);
      programBuilder.addIdentity();
      programBuilder.addCondition(filter.getCondition());
      final RexProgram program = programBuilder.getProgram();

      final EnumerableCalcRel calc =
          new EnumerableCalcRel(
              filter.getCluster(),
              filter.getTraitSet(),
              rel,
              inputRowType,
              program,
              ImmutableList.<RelCollation>of());
      call.transformTo(calc);
    }
  }

  public static final EnumerableProjectToCalcRule
  ENUMERABLE_PROJECT_TO_CALC_RULE =
      new EnumerableProjectToCalcRule();

  /** Variant of {@link org.eigenbase.rel.rules.ProjectToCalcRule} for
   * {@link EnumerableConvention enumerable calling convention}. */
  public static class EnumerableProjectToCalcRule extends RelOptRule {
    private EnumerableProjectToCalcRule() {
      super(operand(EnumerableProjectRel.class, any()));
    }

    public void onMatch(RelOptRuleCall call) {
      final EnumerableProjectRel project = call.rel(0);
      final RelNode child = project.getChild();
      final RelDataType rowType = project.getRowType();
      final RexProgram program =
          RexProgram.create(child.getRowType(),
              project.getProjects(),
              null,
              project.getRowType(),
              project.getCluster().getRexBuilder());
      final EnumerableCalcRel calc =
          new EnumerableCalcRel(
              project.getCluster(),
              project.getTraitSet(),
              child,
              rowType,
              program,
              ImmutableList.<RelCollation>of());
      call.transformTo(calc);
    }
  }

  public interface AggCallContext {
    BlockBuilder builder();
    Expression index();
    Expression current();
    Expression isFirst();
    Expression isLast();
  }
}

// End JavaRules.java
