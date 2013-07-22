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

import net.hydromatic.optiq.BuiltinMethod;
import net.hydromatic.optiq.ModifiableTable;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.jdbc.JavaTypeFactoryImpl;
import net.hydromatic.optiq.prepare.Prepare;
import net.hydromatic.optiq.runtime.SortedMultiMap;

import net.hydromatic.linq4j.*;
import net.hydromatic.linq4j.expressions.*;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.linq4j.function.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.ConverterRule;
import org.eigenbase.rel.metadata.RelMetadataQuery;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.*;
import org.eigenbase.sql.SqlWindowOperator;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.trace.EigenbaseTrace;
import org.eigenbase.util.*;

import com.google.common.collect.ImmutableList;

import java.lang.reflect.*;
import java.math.BigDecimal;
import java.util.*;
import java.util.logging.Logger;

/**
 * Rules and relational operators for the {@link Enumerable} calling convention.
 */
public class JavaRules {

  private static final Constructor ABSTRACT_ENUMERABLE_CTOR =
      Types.lookupConstructor(AbstractEnumerable.class);

  protected static final Logger tracer = EigenbaseTrace.getPlannerTracer();

  public static final boolean BRIDGE_METHODS = true;

  private static final List<ParameterExpression> NO_PARAMS =
      Collections.emptyList();

  private static final List<Expression> NO_EXPRS =
      Collections.emptyList();

  public static final RelOptRule ENUMERABLE_JOIN_RULE =
      new EnumerableJoinRule();
  public static final String[] LEFT_RIGHT = new String[]{"left", "right"};

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
        tracer.warning(e.toString());
        return null;
      }
    }
  }

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
    public EnumerableJoinRel copy(
        RelTraitSet traitSet,
        RexNode conditionExpr,
        RelNode left,
        RelNode right) {
      try {
        return new EnumerableJoinRel(
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

      return planner.makeCost(rowCount, 0, 0);
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
                      .appendIfNotNull(keyPhysType.comparer()))).toBlock());
    }

    Expression generateSelector(PhysType physType,
        List<PhysType> inputPhysTypes) {
      // A parameter for each input.
      final List<ParameterExpression> parameters =
          new ArrayList<ParameterExpression>();

      // Generate all fields.
      final List<Expression> expressions =
          new ArrayList<Expression>();
      for (Ord<PhysType> inputPhysType : Ord.zip(inputPhysTypes)) {
        final ParameterExpression parameter =
            Expressions.parameter(
                inputPhysType.e.getJavaRowType(),
                LEFT_RIGHT[inputPhysType.i]);
        parameters.add(parameter);
        int fieldCount = inputPhysType.e.getRowType().getFieldCount();
        for (int i = 0; i < fieldCount; i++) {
          expressions.add(
              Types.castIfNecessary(
                  inputPhysType.e.fieldClass(i),
                  inputPhysType.e.fieldReference(parameter, i)));
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
    static MethodDeclaration overridingMethodDecl(
        Method method,
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

    static List<RexImpTable.AggImplementor2> getImplementors(
        List<AggregateCall> aggCalls) {
      final List<RexImpTable.AggImplementor2> implementors =
          new ArrayList<RexImpTable.AggImplementor2>();
      for (AggregateCall aggCall : aggCalls) {
        RexImpTable.AggImplementor2 implementor2 =
            RexImpTable.INSTANCE.get2(
                aggCall.getAggregation());
        if (implementor2 == null) {
          throw new RuntimeException(
              "cannot implement aggregate " + aggCall);
        }
        implementors.add(implementor2);
      }
      return implementors;
    }
  }

  public static class EnumerableTableAccessRel
      extends TableAccessRelBase
      implements EnumerableRel {
    private final Expression expression;
    private final Class elementType;

    public EnumerableTableAccessRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelOptTable table,
        Expression expression,
        Class elementType) {
      super(cluster, traitSet, table);
      assert getConvention() instanceof EnumerableConvention;
      this.elementType = elementType;
      if (Types.isArray(expression.getType())) {
        if (Types.toClass(expression.getType()).getComponentType()
            .isPrimitive()) {
          expression = Expressions.call(
              BuiltinMethod.AS_LIST.method,
              expression);
        }
        expression =
            Expressions.call(
                BuiltinMethod.AS_ENUMERABLE.method,
                expression);
      } else if (Types.isAssignableFrom(
          Iterable.class, expression.getType())
          && !Types.isAssignableFrom(
              Enumerable.class, expression.getType())) {
        expression =
            Expressions.call(
                BuiltinMethod.AS_ENUMERABLE2.method,
                expression);
      }
      this.expression = expression;
    }

    private JavaRowFormat format() {
      if (Object[].class.isAssignableFrom(elementType)) {
        return JavaRowFormat.ARRAY;
      } else {
        return JavaRowFormat.CUSTOM;
      }
    }

    @Override
    public RelNode copy(
        RelTraitSet traitSet, List<RelNode> inputs) {
      return new EnumerableTableAccessRel(
          getCluster(), traitSet, table, expression, elementType);
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
      return implementor.result(physType, Blocks.toBlock(expression));
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
          program,
          ProjectRelBase.Flags.Boxed);
    }
  }

  public static class EnumerableCalcRel
      extends SingleRel
      implements EnumerableRel {
    private final RexProgram program;

    /**
     * Values defined in {@link org.eigenbase.rel.ProjectRelBase.Flags}.
     */
    protected int flags;

    public EnumerableCalcRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode child,
        RexProgram program,
        int flags) {
      super(cluster, traitSet, child);
      assert getConvention() instanceof EnumerableConvention;
      assert !program.containsAggs();
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
      return new EnumerableCalcRel(
          getCluster(),
          traitSet,
          sole(inputs),
          program.copy(),
          getFlags());
    }

    public int getFlags() {
      return flags;
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
                  ABSTRACT_ENUMERABLE_CTOR,
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
        tracer.warning(e.toString());
        return null;
      }
    }
  }

  public static class EnumerableAggregateRel
      extends AggregateRelBase
      implements EnumerableRel {
    private static final List<Aggregation> SUPPORTED_AGGREGATIONS =
        Arrays.<Aggregation>asList(
            SqlStdOperatorTable.countOperator,
            SqlStdOperatorTable.minOperator,
            SqlStdOperatorTable.maxOperator,
            SqlStdOperatorTable.sumOperator);

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
        final Aggregation aggregation = aggCall.getAggregation();
        if (!SUPPORTED_AGGREGATIONS.contains(aggregation)) {
          throw new InvalidRelException(
              "aggregation " + aggregation + " not supported");
        }
      }
    }

    @Override
    public EnumerableAggregateRel copy(
        RelTraitSet traitSet, List<RelNode> inputs) {
      try {
        return new EnumerableAggregateRel(
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

    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
      final JavaTypeFactory typeFactory = implementor.getTypeFactory();
      final BlockBuilder builder = new BlockBuilder();
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
      // with a slightly different resultSelector.
      PhysType inputPhysType = result.physType;

      ParameterExpression parameter =
          Expressions.parameter(inputPhysType.getJavaRowType(), "a0");

      final List<Expression> keyExpressions = Expressions.list();
      PhysType keyPhysType =
          inputPhysType.project(
              Util.toList(groupSet), JavaRowFormat.LIST);
      final int keyArity = groupSet.cardinality();
      for (int groupKey : Util.toIter(groupSet)) {
        keyExpressions.add(
            inputPhysType.fieldReference(parameter, groupKey));
      }
      final Expression keySelector =
          builder.append(
              "keySelector",
              inputPhysType.generateSelector(
                  parameter, Util.toList(groupSet), keyPhysType.getFormat()));

      final List<RexImpTable.AggImplementor2> implementors =
          EnumUtil.getImplementors(aggCalls);

      // Function0<Object[]> accumulatorInitializer =
      //     new Function0<Object[]>() {
      //         public Object[] apply() {
      //             return new Object[] {0, 0};
      //         }
      //     };
      final List<Expression> initExpressions =
          new ArrayList<Expression>();
      for (Ord<Pair<AggregateCall, RexImpTable.AggImplementor2>> ord
          : Ord.zip(Pair.zip(aggCalls, implementors))) {
        initExpressions.add(
            ord.e.right.implementInit(
                ord.e.left.getAggregation(),
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
      for (Ord<Pair<AggregateCall, RexImpTable.AggImplementor2>> ord
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
                Expressions.assign(
                    accumulator,
                    ord.e.right.implementAdd(
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
      for (Ord<Pair<AggregateCall, RexImpTable.AggImplementor2>> ord
          : Ord.zip(Pair.zip(aggCalls, implementors))) {
        results.add(
            ord.e.right.implementResult(
                ord.e.left.getAggregation(),
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
      final RelTraitSet traitSet =
          sort.getTraitSet().replace(EnumerableConvention.INSTANCE);
      final RelNode input = sort.getChild();
      return new EnumerableSortRel(
          rel.getCluster(),
          traitSet,
          convert(
              input,
              input.getTraitSet().replace(EnumerableConvention.INSTANCE)),
          sort.getCollation());
    }
  }

  public static class EnumerableSortRel
      extends SortRel
      implements EnumerableRel {
    public EnumerableSortRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode child,
        RelCollation collation) {
      super(cluster, traitSet, child, collation);
      assert getConvention() instanceof EnumerableConvention;
      assert getConvention() == child.getConvention();
    }

    @Override
    public EnumerableSortRel copy(
        RelTraitSet traitSet,
        RelNode newInput,
        RelCollation newCollation) {
      return new EnumerableSortRel(
          getCluster(),
          traitSet,
          newInput,
          newCollation);
    }

    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
      final BlockBuilder builder = new BlockBuilder();
      final EnumerableRel child = (EnumerableRel) getChild();
      final Result result = implementor.visitChild(this, 0, child, pref);
      final PhysType physType =
          PhysTypeImpl.of(
              implementor.getTypeFactory(),
              getRowType(),
              pref.prefer(result.format));
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
                  .appendIfNotNull(
                      builder.appendIfNotNull("comparator", pair.right)))));
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
      final RelTraitSet traitSet =
          union.getTraitSet().replace(
              EnumerableConvention.INSTANCE);
      return new EnumerableUnionRel(
          rel.getCluster(),
          traitSet,
          convertList(union.getInputs(), traitSet),
          union.all);
    }
  }

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
      final RelTraitSet traitSet =
          intersect.getTraitSet().replace(
              EnumerableConvention.INSTANCE);
      return new EnumerableIntersectRel(
          rel.getCluster(),
          traitSet,
          convertList(intersect.getInputs(), traitSet),
          intersect.all);
    }
  }

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
      final RelTraitSet traitSet =
          rel.getTraitSet().replace(
              EnumerableConvention.INSTANCE);
      return new EnumerableMinusRel(
          rel.getCluster(),
          traitSet,
          convertList(minus.getInputs(), traitSet),
          minus.all);
    }
  }

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
      if (modifiableTable == null
          || modifiableTable.getExpression() == null) {
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

  public static class EnumerableTableModificationRel
      extends TableModificationRelBase
      implements EnumerableRel {
    private final Expression expression;

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
      this.expression = modifiableTable.getExpression();
      if (expression == null) {
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
      builder.add(
          Expressions.declare(
              0,
              collectionParameter,
              Expressions.call(
                  Expressions.convert_(
                      expression,
                      ModifiableTable.class),
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

  public static class EnumerableOneRowRule extends RelOptRule {
    private EnumerableOneRowRule() {
      super(
          leaf(OneRowRel.class, Convention.NONE),
          "EnumerableOneRowRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final OneRowRel rel = call.rel(0);
      call.transformTo(
          new ValuesRel(
              rel.getCluster(),
              rel.getRowType(),
              Collections.singletonList(
                  Collections.singletonList(
                      rel.getCluster().getRexBuilder().makeExactLiteral(
                          BigDecimal.ZERO)))));
    }

    public RelNode convert(RelNode rel) {
      ValuesRel valuesRel = (ValuesRel) rel;
      return new EnumerableValuesRel(
          valuesRel.getCluster(),
          valuesRel.getRowType(),
          valuesRel.getTuples(),
          valuesRel.getTraitSet().replace(EnumerableConvention.INSTANCE));
    }
  }

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
      return planner.makeCost(rowsIn, rowsIn * count, 0);
    }

    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
      final JavaTypeFactoryImpl typeFactory = implementor.getTypeFactory();
      final EnumerableRel child = (EnumerableRel) getChild();

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
                      v_, Util.toList(window.groupSet), JavaRowFormat.CUSTOM));
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
        final List<Map.Entry<String, RelDataType>> fieldList =
            new ArrayList<Map.Entry<String, RelDataType>>(
                inputPhysType.getRowType().getFieldList());
        final int offset = fieldList.size();
        final List<AggregateCall> aggregateCalls =
            window.getAggregateCalls(this);
        for (AggregateCall aggregateCall : aggregateCalls) {
          fieldList.add(Pair.of(aggregateCall.name, aggregateCall.type));
        }
        RelDataType outputRowType = typeFactory.createStructType(fieldList);
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

        final List<RexImpTable.AggImplementor2> implementors =
            EnumUtil.getImplementors(aggregateCalls);
        final List<ParameterExpression> variables =
            new ArrayList<ParameterExpression>();
        for (Ord<Pair<AggregateCall, RexImpTable.AggImplementor2>> ord
            : Ord.zip(Pair.zip(aggregateCalls, implementors))) {
          final ParameterExpression parameter =
              Expressions.parameter(outputPhysType.fieldClass(offset + ord.i),
                  builder4.newName(ord.e.left.name));
          final Expression initExpression =
              ord.e.right.implementInit(ord.e.left.getAggregation(),
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
        generateWindowLoop(builder4,
            window,
            aggregateCalls,
            implementors,
            variables,
            rows_,
            i_,
            row_,
            expressions,
            new Function1<AggCallContext, Void>() {
              public Void apply(AggCallContext a0) {
                for (Ord<Pair<AggregateCall, RexImpTable.AggImplementor2>> ord
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
                          Expressions.assign(
                              accumulator,
                              ord.e.right.implementAdd(
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
    private void generateWindowLoop(BlockBuilder builder, Window window,
        List<AggregateCall> aggregateCalls,
        List<RexImpTable.AggImplementor2> implementors,
        List<ParameterExpression> variables,
        Expression rows_,
        ParameterExpression i_,
        Expression row_,
        List<Expression> expressions,
        Function1<AggCallContext, Void> f3) {
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
      final SqlWindowOperator.OffsetRange offsetAndRange =
          SqlWindowOperator.getOffsetAndRange(
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

      for (Ord<Pair<AggregateCall, RexImpTable.AggImplementor2>> ord
          : Ord.zip(Pair.zip(aggregateCalls, implementors))) {
        final RexImpTable.AggImplementor2 implementor2 = ord.e.right;
        expressions.add(
            implementor2 instanceof RexImpTable.WinAggImplementor
                ? ((RexImpTable.WinAggImplementor) implementor2)
                .implementResultPlus(
                    ord.e.left.getAggregation(), variables.get(ord.i),
                    start_, end_, rows_, i_)
                : implementor2.implementResult(
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

  public static interface AggCallContext {
    BlockBuilder builder();
    Expression index();
    Expression current();
    Expression isFirst();
    Expression isLast();
  }
}

// End JavaRules.java
