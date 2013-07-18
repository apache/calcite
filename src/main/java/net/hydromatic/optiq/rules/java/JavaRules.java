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
import net.hydromatic.optiq.prepare.Prepare;

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
import org.eigenbase.sql.*;
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
      BlockBuilder list = new BlockBuilder();
      final Result leftResult =
          implementor.visitChild(this, 0, (EnumerableRel) left, pref);
      Expression leftExpression =
          list.append(
              "left", leftResult.expression);
      final Result rightResult =
          implementor.visitChild(this, 1, (EnumerableRel) right, pref);
      Expression rightExpression =
          list.append(
              "right", rightResult.expression);
      final PhysType physType =
          PhysTypeImpl.of(
              implementor.getTypeFactory(), getRowType(), pref.preferArray());
      final PhysType keyPhysType =
          leftResult.physType.project(
              leftKeys, JavaRowFormat.LIST);
      return implementor.result(
          physType,
          list.append(
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
        BlockExpression body) {
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
      final BlockBuilder statements = new BlockBuilder();
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

      BlockExpression moveNextBody;
      if (program.getCondition() == null) {
        moveNextBody =
            Blocks.toFunctionBlock(
                Expressions.call(
                    inputEnumerator,
                    BuiltinMethod.ENUMERATOR_MOVE_NEXT.method));
      } else {
        final BlockBuilder list = new BlockBuilder();
        Expression condition =
            RexToLixTranslator.translateCondition(
                program,
                typeFactory,
                list,
                new RexToLixTranslator.InputGetterImpl(
                    Collections.singletonList(
                        Pair.of(input, result.physType))));
        list.add(
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
                    list.toBlock()),
                Expressions.return_(
                    null,
                    Expressions.constant(false)));
      }

      final BlockBuilder list = new BlockBuilder();
      List<Expression> expressions =
          RexToLixTranslator.translateProjects(
              program,
              typeFactory,
              list,
              new RexToLixTranslator.InputGetterImpl(
                  Collections.singletonList(
                      Pair.of(input, result.physType))));
      list.add(
          Expressions.return_(
              null,
              physType.record(expressions)));
      BlockExpression currentBody =
          list.toBlock();

      final Expression inputEnumerable =
          statements.append(
              "inputEnumerable", result.expression, false);
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
                      Expressions.block()),
                  Expressions.methodDecl(
                      Modifier.PUBLIC,
                      BRIDGE_METHODS
                          ? Object.class
                          : outputJavaType,
                      "current",
                      NO_PARAMS,
                      currentBody)));
      statements.add(
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
                          BuiltinMethod.ENUMERABLE_ENUMERATOR
                              .method.getName(),
                          NO_PARAMS,
                          Blocks.toFunctionBlock(body))))));
      return implementor.result(physType, statements.toBlock());
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
      final BlockBuilder statements = new BlockBuilder();
      final EnumerableRel child = (EnumerableRel) getChild();
      final Result result = implementor.visitChild(this, 0, child, pref);
      Expression childExp =
          statements.append(
              "child",
              result.expression);
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
              Util.toList(groupSet),
              JavaRowFormat.LIST);
      final int keyArity = groupSet.cardinality();
      for (int groupKey : Util.toIter(groupSet)) {
        keyExpressions.add(
            inputPhysType.fieldReference(parameter, groupKey));
      }
      final Expression keySelector =
          statements.append(
              "keySelector",
              inputPhysType.generateSelector(
                  parameter, Util.toList(groupSet), keyPhysType.getFormat()));

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
                fieldTypes(
                    typeFactory,
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
          statements.append(
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
      BlockBuilder bb2 = new BlockBuilder();
      final ParameterExpression inParameter =
          Expressions.parameter(inputPhysType.getJavaRowType(), "in");
      final ParameterExpression accParameter =
          Expressions.parameter(accPhysType.getJavaRowType(), "acc");
      for (Ord<Pair<AggregateCall, RexImpTable.AggImplementor2>> ord
          : Ord.zip(Pair.zip(aggCalls, implementors))) {
        final Type type = initExpressions.get(ord.i).type;
        final Expression accumulator =
            accPhysType.fieldReference(accParameter, ord.i);
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
                        accessors(
                            inputPhysType, inParameter, ord.e.left))));
        if (conditions.isEmpty()) {
          bb2.add(assign);
        } else {
          bb2.add(
              Expressions.ifThen(
                  Expressions.foldAnd(conditions),
                  assign));
        }
      }
      bb2.add(accParameter);
      final Expression accumulatorAdder =
          statements.append(
              "accumulatorAdder",
              Expressions.lambda(
                  Function2.class,
                  bb2.toBlock(),
                  Arrays.asList(accParameter, inParameter)));

      // Function2<Integer, Object[], Object[]> resultSelector =
      //     new Function2<Integer, Object[], Object[]>() {
      //         public Object[] apply(Integer key, Object[] acc) {
      //             return new Object[] { key, acc[0], acc[1] };
      //         }
      //     };
      final List<Expression> results = Expressions.list();
      final ParameterExpression keyParameter;
      if (keyArity == 0) {
        keyParameter = null;
      } else {
        final Type keyType = keyPhysType.getJavaRowType();
        keyParameter = Expressions.parameter(keyType, "key");
        for (int j = 0; j < keyArity; j++) {
          results.add(
              keyPhysType.fieldReference(keyParameter, j));
        }
      }
      for (Ord<Pair<AggregateCall, RexImpTable.AggImplementor2>> ord
          : Ord.zip(Pair.zip(aggCalls, implementors))) {
        results.add(
            ord.e.right.implementResult(
                ord.e.left.getAggregation(),
                accPhysType.fieldReference(
                    accParameter, ord.i)));
      }
      final PhysType resultPhysType = physType;
      if (keyArity == 0) {
        final Expression resultSelector =
            statements.append(
                "resultSelector",
                Expressions.lambda(
                    Function1.class,
                    resultPhysType.record(results),
                    accParameter));
        statements.add(
            Expressions.return_(
                null,
                Expressions.call(
                    BuiltinMethod.SINGLETON_ENUMERABLE.method,
                    Expressions.call(
                        childExp,
                        BuiltinMethod.AGGREGATE.method,
                        Expressions.<Expression>list()
                            .append(
                                Expressions.call(
                                    accumulatorInitializer,
                                    "apply"))
                            .append(accumulatorAdder)
                            .append(resultSelector)))));
      } else {
        final Expression resultSelector =
            statements.append(
                "resultSelector",
                Expressions.lambda(
                    Function2.class,
                    resultPhysType.record(results),
                    Expressions.list(keyParameter, accParameter)));
        statements.add(
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
      return implementor.result(physType, statements.toBlock());
    }

    private List<Type> fieldTypes(
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

    private List<Expression> accessors(
        PhysType physType,
        ParameterExpression v1,
        AggregateCall aggCall) {
      final List<Expression> expressions = new ArrayList<Expression>();
      for (int field : aggCall.getArgList()) {
        expressions.add(
            Types.castIfNecessary(
                physType.fieldClass(field),
                physType.fieldReference(v1, field)));
      }
      return expressions;
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
      final BlockBuilder statements = new BlockBuilder();
      final EnumerableRel child = (EnumerableRel) getChild();
      final Result result = implementor.visitChild(this, 0, child, pref);
      final PhysType physType =
          PhysTypeImpl.of(
              implementor.getTypeFactory(),
              getRowType(),
              pref.prefer(result.format));
      Expression childExp =
          statements.append(
              "child", result.expression);

      PhysType inputPhysType = result.physType;
      final Pair<Expression, Expression> pair =
          inputPhysType.generateCollationKey(
              collation.getFieldCollations());

      final Expression keySelector =
          statements.append(
              "keySelector", pair.left);

      final Expression comparatorExp = pair.right;

      final List<Expression> arguments =
          Expressions.list(keySelector);
      if (comparatorExp != null) {
        final Expression comparator =
            statements.append(
                "comparator",
                comparatorExp);
        arguments.add(comparator);
      }
      statements.add(
          Expressions.return_(
              null,
              Expressions.call(
                  childExp,
                  BuiltinMethod.ORDER_BY.method,
                  arguments)));
      return implementor.result(physType, statements.toBlock());
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
      final BlockBuilder statements = new BlockBuilder();
      Expression unionExp = null;
      for (Ord<RelNode> ord : Ord.zip(inputs)) {
        EnumerableRel input = (EnumerableRel) ord.e;
        final Result result = implementor.visitChild(this, ord.i, input, pref);
        Expression childExp =
            statements.append(
                "child" + ord.i,
                result.expression);

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

      statements.add(unionExp);
      final PhysType physType =
          PhysTypeImpl.of(
              implementor.getTypeFactory(),
              getRowType(),
              pref.prefer(JavaRowFormat.CUSTOM));
      return implementor.result(physType, statements.toBlock());
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
      final BlockBuilder statements = new BlockBuilder();
      Expression intersectExp = null;
      for (Ord<RelNode> ord : Ord.zip(inputs)) {
        EnumerableRel input = (EnumerableRel) ord.e;
        final Result result = implementor.visitChild(this, ord.i, input, pref);
        Expression childExp =
            statements.append(
                "child" + ord.i,
                result.expression);

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

      statements.add(intersectExp);
      final PhysType physType =
          PhysTypeImpl.of(
              implementor.getTypeFactory(),
              getRowType(),
              pref.prefer(JavaRowFormat.CUSTOM));
      return implementor.result(physType, statements.toBlock());
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
      final BlockBuilder statements = new BlockBuilder();
      Expression minusExp = null;
      for (Ord<RelNode> ord : Ord.zip(inputs)) {
        EnumerableRel input = (EnumerableRel) ord.e;
        final Result result = implementor.visitChild(this, ord.i, input, pref);
        Expression childExp =
            statements.append(
                "child" + ord.i,
                result.expression);

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

      statements.add(minusExp);
      final PhysType physType =
          PhysTypeImpl.of(
              implementor.getTypeFactory(),
              getRowType(),
              pref.prefer(JavaRowFormat.CUSTOM));
      return implementor.result(physType, statements.toBlock());
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
              "child", result.expression);
      final ParameterExpression collectionParameter =
          Expressions.parameter(Collection.class, "collection");
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
        final ParameterExpression o =
            Expressions.parameter(childPhysType.getJavaRowType(), "o");
        final int fieldCount =
            childPhysType.getRowType().getFieldCount();
        for (int i = 0; i < fieldCount; i++) {
          expressionList.add(childPhysType.fieldReference(o, i));
        }
        convertedChildExp =
            builder.append(
                "convertedChild",
                Expressions.call(
                    childExp,
                    BuiltinMethod.SELECT.method,
                    Expressions.lambda(
                        physType.record(expressionList), o)));
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
      final BlockBuilder statements = new BlockBuilder();
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
      statements.add(
          Expressions.return_(
              null,
              Expressions.call(
                  BuiltinMethod.AS_ENUMERABLE.method,
                  Expressions.newArrayInit(
                      Primitive.box(rowClass), expressions))));
      return implementor.result(physType, statements.toBlock());
    }
  }

  public static final EnumerableWindowRule ENUMERABLE_WINAGG_RULE =
      new EnumerableWindowRule();

  /**
   * Rule to convert an {@link org.eigenbase.rel.AggregateRel} to an
   * {@link net.hydromatic.optiq.rules.java.JavaRules.EnumerableAggregateRel}.
   */
  private static class EnumerableWindowRule
      extends ConverterRule {
    private EnumerableWindowRule() {
      super(
          WindowRel.class,
          Convention.NONE,
          EnumerableConvention.INSTANCE,
          "EnumerableWindowRule");
    }

    public RelNode convert(RelNode rel) {
      final WindowRel winAgg = (WindowRel) rel;
      final RelTraitSet traitSet =
          winAgg.getTraitSet().replace(EnumerableConvention.INSTANCE);
      final RelOptCluster cluster = winAgg.getCluster();
      final RelNode child = winAgg.getChild();
      RelNode fennelInput = convert(child, traitSet);

      // Build the input program.
      final RexProgram inProgram = RexProgram.createIdentity(child.getRowType());

      // Build a list of distinct windows, partitions and aggregate
      // functions.
      List<WindowRel.Window> windowList =
          new ArrayList<WindowRel.Window>();
      final Map<RexOver, WindowRelBase.RexWinAggCall> aggMap =
          new HashMap<RexOver, WindowRelBase.RexWinAggCall>();
      final RexProgram aggProgram =
          RexProgramBuilder.mergePrograms(
              winAgg.getProgram(),
              inProgram,
              cluster.getRexBuilder(),
              false);

      // Make sure the orderkey is among the input expressions from the child.
      // Merged program creates intermediate rowtype below. OutputProgram is
      // then built based on the intermediate row which also holds winAgg
      // results. orderKey is resolved before intermediate rowtype is
      // constructed and "orderKey ordinal" may point to wrong offset within
      // intermediate row. dtbug #2209.
      for (RexNode expr : aggProgram.getExprList()) {
          if (expr instanceof RexOver) {
              RexOver over = (RexOver) expr;
              List<RexNode> orderKeys = over.getWindow().orderKeys;
              for (RexNode orderKey : orderKeys) {
                  if (orderKey instanceof RexLocalRef
                      && ((RexLocalRef)orderKey).getIndex()
                          >= child.getRowType().getFieldCount()) {
                      // do not merge inCalc and winAggRel.
                      return;
                  }
              }
          }
      }

      // The purpose of the input program is to provide the expressions
      // needed by all of the aggregate functions. Its outputs are (a) all
      // of the input fields, followed by (b) the input expressions for the
      // aggregate functions.
      final RexProgramBuilder inputProgramBuilder =
          new RexProgramBuilder(
              aggProgram.getInputRowType(),
              cluster.getRexBuilder());
      int i = -1;
      for (RexNode expr : aggProgram.getExprList()) {
          ++i;
          if (expr instanceof RexInputRef) {
              inputProgramBuilder.addProject(
                  i,
                  aggProgram.getInputRowType()
                      .getFieldList()
                      .get(i)
                      .getName());
          } else {
              inputProgramBuilder.addExpr(expr);
          }
      }

      // Build a list of windows, partitions, and aggregate functions. Each
      // aggregate function will add its arguments as outputs of the input
      // program.
      for (RexNode agg : aggProgram.getExprList()) {
          if (agg instanceof RexOver) {
              final RexOver over = (RexOver) agg;
              WindowRelBase.RexWinAggCall aggCall =
                  addWindows(windowList, over, inputProgramBuilder);
              aggMap.put(over, aggCall);
          }
      }
      final RexProgram inputProgram = inputProgramBuilder.getProgram();

      // Now the windows are complete, compute their digests.
      for (WindowRel.Window window : windowList) {
          window.computeDigest();
      }

      // Figure out the type of the inputs to the output program.
      // They are: the inputs to this rel, followed by the outputs of
      // each window.
      final List<WindowRelBase.RexWinAggCall> flattenedAggCallList =
          new ArrayList<WindowRelBase.RexWinAggCall>();
      List<String> intermediateNameList =
          new ArrayList<String>(child.getRowType().getFieldNames());
      final List<RelDataType> intermediateTypeList =
          new ArrayList<RelDataType>(
              RelOptUtil.getFieldTypeList(child.getRowType()));

      i = -1;
      for (WindowRel.Window window : windowList) {
          ++i;
          int j = -1;
          for (WindowRel.Partition p : window.getPartitionList()) {
              ++j;
              int k = -1;
              for (WindowRelBase.RexWinAggCall over : p.overList) {
                  ++k;

                  // Add the k'th over expression of the j'th partition of
                  // the i'th window to the output of the program.
                  intermediateNameList.add("w" + i + "$p" + j + "$o" + k);
                  intermediateTypeList.add(over.getType());
                  flattenedAggCallList.add(over);
              }
          }
      }
      RelDataType intermediateRowType =
          cluster.getTypeFactory().createStructType(
              intermediateTypeList,
              intermediateNameList);

      // The output program is the windowed agg's program, combined with
      // the output calc (if it exists).
      RexProgramBuilder outputProgramBuilder =
          new RexProgramBuilder(
              intermediateRowType,
              cluster.getRexBuilder());
      final int inputFieldCount = child.getRowType().getFieldCount();
      RexShuttle shuttle =
          new RexShuttle() {
              public RexNode visitOver(RexOver over) {
                  // Look up the aggCall which this expr was translated to.
                  final WindowRelBase.RexWinAggCall aggCall =
                      aggMap.get(over);
                  assert aggCall != null;
                  assert RelOptUtil.eq(
                      "over",
                      over.getType(),
                      "aggCall",
                      aggCall.getType(),
                      true);

                  // Find the index of the aggCall among all partitions of all
                  // windows.
                  final int aggCallIndex =
                      flattenedAggCallList.indexOf(aggCall);
                  assert aggCallIndex >= 0;

                  // Replace expression with a reference to the window slot.
                  final int index = inputFieldCount + aggCallIndex;
                  assert RelOptUtil.eq(
                      "over",
                      over.getType(),
                      "intermed",
                      intermediateTypeList.get(index),
                      true);
                  return new RexInputRef(
                      index,
                      over.getType());
              }

              public RexNode visitLocalRef(RexLocalRef localRef) {
                  final int index = localRef.getIndex();
                  if (index < inputFieldCount) {
                      // Reference to input field.
                      return localRef;
                  }
                  return new RexLocalRef(
                      flattenedAggCallList.size() + index,
                      localRef.getType());
              }
          };
      for (RexNode expr : aggProgram.getExprList()) {
          expr = expr.accept(shuttle);
          outputProgramBuilder.registerInput(expr);
      }

      final List<String> fieldNames =
          winAgg.getRowType().getFieldNames();
      i = -1;
      for (RexLocalRef ref : aggProgram.getProjectList()) {
          ++i;
          int index = ref.getIndex();
          final RexNode expr = aggProgram.getExprList().get(index);
          RexNode expr2 = expr.accept(shuttle);
          outputProgramBuilder.addProject(
              outputProgramBuilder.registerInput(expr2),
              fieldNames.get(i));
      }

      // Create the output program.
      final RexProgram outputProgram;
      if (null == null) {
          outputProgram = outputProgramBuilder.getProgram();
          assert RelOptUtil.eq(
              "type1",
              outputProgram.getOutputRowType(),
              "type2",
              winAgg.getRowType(),
              true);
      } else {
          // Merge intermediate program (from winAggRel) with output program
          // (from outCalc).
          RexProgram intermediateProgram = outputProgramBuilder.getProgram();
          outputProgram =
              RexProgramBuilder.mergePrograms(
                  ((CalcRel) null).getProgram(),
                  intermediateProgram,
                  cluster.getRexBuilder());
          assert RelOptUtil.eq(
              "type1",
              outputProgram.getInputRowType(),
              "type2",
              intermediateRowType,
              true);
          assert RelOptUtil.eq(
              "type1",
              outputProgram.getOutputRowType(),
              "type2",
              ((CalcRel) null).getRowType(),
              true);
      }

      // Put all these programs together in the final relational expression.
      WindowRel.Window[] windows = windowList.toArray(
          new WindowRel.Window[windowList.size()]);
      // default case; normal java to show what it means
      EnumerableWindowRel fennelCalcRel = new EnumerableWindowRel(
          cluster,
          fennelInput,
          outputProgram.getOutputRowType(),
          inputProgram,
          windows,
          outputProgram);
      RelTraitSet outTraits = fennelCalcRel.getTraitSet();
      // copy over other traits from the child
      for (i = 0; i < traits.size(); i++) {
          RelTrait trait = traits.getTrait(i);
          if (trait.getTraitDef() != ConventionTraitDef.instance) {
              outTraits = outTraits.plus(trait);
          }
      }
      // convert to the traits of the calling rel to which we are equivalent
      // and thus must have the same traits
      RelNode mergedFennelCalcRel = fennelCalcRel;
      if (fennelCalcRel.getTraitSet() != outTraits) {
          mergedFennelCalcRel = convert(fennelCalcRel, outTraits);
      }
      call.transformTo(mergedFennelCalcRel);
    }

      // implement RelOptRule
      public Convention getOutConvention() {
          return EnumerableConvention.INSTANCE;
      }

    private WindowRelBase.RexWinAggCall addWindows(
      List<WindowRel.Window> windowList,
      RexOver over,
      RexProgramBuilder programBuilder) {
        final RexWindow aggWindow = over.getWindow();

          // Look up or create a window.
          Integer [] orderKeys =
              getProjectOrdinals(programBuilder, aggWindow.orderKeys);
          WindowRel.Window fennelWindow =
              lookupWindow(
                  windowList,
                  aggWindow.isRows(),
                  aggWindow.getLowerBound(),
                  aggWindow.getUpperBound(),
                  orderKeys);

          // Lookup or create a partition within the window.
          Integer [] partitionKeys =
              getProjectOrdinals(programBuilder, aggWindow.partitionKeys);
          WindowRel.Partition fennelPartition =
              fennelWindow.lookupOrCreatePartition(partitionKeys);
          Util.discard(fennelPartition);

          // Create a clone the 'over' expression, omitting the window (which is
          // already part of the partition spec), and add the clone to the
          // partition.
          return fennelPartition.addOver(
              over.getType(),
              over.getAggOperator(),
              over.getOperands(),
              programBuilder);
      }

      /**
       * Converts a list of expressions into a list of ordinals that these
       * expressions are projected from a {@link org.eigenbase.rex.RexProgramBuilder}. If an
       * expression is not projected, adds it.
       *
       * @param programBuilder Program builder
       * @param exprs List of expressions
       *
       * @return List of ordinals where expressions are projected
       */
      private Integer [] getProjectOrdinals(
          RexProgramBuilder programBuilder,
          List<RexNode> exprs) {
          Integer [] newKeys = new Integer[exprs.size()];
          for (int i = 0; i < newKeys.length; i++) {
              RexLocalRef operand = (RexLocalRef) exprs.get(i);
              List<RexLocalRef> projectList = programBuilder.getProjectList();
              int index = projectList.indexOf(operand);
              if (index < 0) {
                  index = projectList.size();
                  programBuilder.addProject(operand, null);
              }
              newKeys[i] = index;
          }
          return newKeys;
      }

      private WindowRel.Window lookupWindow(
          List<WindowRel.Window> windowList,
          boolean physical,
          SqlNode lowerBound,
          SqlNode upperBound,
          Integer [] orderKeys) {
          for (WindowRel.Window window : windowList) {
              if ((physical == window.physical)
                  && Util.equal(lowerBound, window.lowerBound)
                  && Util.equal(upperBound, window.upperBound)
                  && Arrays.equals(orderKeys, window.orderKeys)) {
                  return window;
              }
          }
          final WindowRel.Window window =
              new WindowRel.Window(
                  physical,
                  lowerBound,
                  upperBound,
                  orderKeys);
          windowList.add(window);
          return window;
      }
  }

  /**
   * Relational expression which computes windowed aggregates.
   *
   * <p>A window rel can handle several window aggregate functions, over several
   * partitions, with pre- and post-expressions, and an optional post-filter.
   * Each of the partitions is defined by a partition key (zero or more columns)
   * and a range (logical or physical). The partitions expect the data to be
   * sorted correctly on input to the relational expression.
   *
   * <p>Rules:
   *
   * <ul>
   * <li>{@link net.hydromatic.optiq.rules.java.JavaRules.EnumerableWindowRule} creates this from a {@link org.eigenbase.rel.CalcRel}</li>
   * <li>{@link org.eigenbase.rel.rules.WindowedAggSplitterRule} decomposes a {@link org.eigenbase.rel.CalcRel} which
   * contains windowed aggregates into a {@link net.hydromatic.optiq.rules.java.JavaRules.EnumerableWindowRel} and zero or more
   * {@link org.eigenbase.rel.CalcRel}s which do not contain windowed aggregates</li>
   * </ul>
   * </p>
   */
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
      // components (windows, partitions, and aggregate functions). There is
      // no I/O cost.
      //
      // TODO #1. Add memory cost.
      // TODO #2. MIN and MAX have higher CPU cost than SUM and COUNT.
      final double rowsIn = RelMetadataQuery.getRowCount(getChild());
      int count = windows.size();
      for (WindowRel.Window window : windows) {
        count += window.partitionList.size();
        for (WindowRel.Partition partition : window.partitionList) {
          count += partition.overList.size();
        }
      }
      return planner.makeCost(rowsIn, rowsIn * count, 0);
    }
  }
}

// End JavaRules.java
