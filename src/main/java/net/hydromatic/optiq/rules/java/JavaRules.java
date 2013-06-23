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
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.trace.EigenbaseTrace;
import org.eigenbase.util.*;

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
          EnumerableConvention.CUSTOM,
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
                      .replace(EnumerableConvention.CUSTOM));
        }
        newInputs.add(input);
      }
      try {
        return new EnumerableJoinRel(
            join.getCluster(),
            join.getTraitSet().replace(EnumerableConvention.CUSTOM),
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
    private final PhysType physType;
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
      this.leftKeys = ImmutableIntList.of(leftKeys);
      this.rightKeys = ImmutableIntList.of(rightKeys);
      this.physType =
          PhysTypeImpl.of(
              (JavaTypeFactory) cluster.getTypeFactory(),
              getRowType(),
              (EnumerableConvention) getConvention());
    }

    public PhysType getPhysType() {
      return physType;
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

    public BlockExpression implement(EnumerableRelImplementor implementor) {
      BlockBuilder list = new BlockBuilder();
      Expression leftExpression =
          list.append(
              "left",
              implementor.visitChild(this, 0, (EnumerableRel) left));
      Expression rightExpression =
          list.append(
              "right",
              implementor.visitChild(this, 1, (EnumerableRel) right));
      final PhysType keyPhysType =
          ((EnumerableRel) left).getPhysType().project(
              leftKeys, JavaRowFormat.CUSTOM);
      return list.append(
          Expressions.call(
              leftExpression,
              BuiltinMethod.JOIN.method,
              Expressions.list(
                  rightExpression,
                  ((EnumerableRel) left).getPhysType()
                      .generateAccessor(leftKeys),
                  ((EnumerableRel) right).getPhysType()
                      .generateAccessor(rightKeys),
                  generateSelector())
                  .appendIfNotNull(keyPhysType.comparer())))
          .toBlock();
    }

    Expression generateSelector() {
      // A parameter for each input.
      final List<ParameterExpression> parameters =
          new ArrayList<ParameterExpression>();

      // Generate all fields.
      final List<Expression> expressions =
          new ArrayList<Expression>();
      for (Ord<RelNode> rel : Ord.zip(getInputs())) {
        PhysType inputPhysType = ((EnumerableRel) rel.e).getPhysType();
        final ParameterExpression parameter =
            Expressions.parameter(
                inputPhysType.getJavaRowType(),
                LEFT_RIGHT[rel.i]);
        parameters.add(parameter);
        int fieldCount = inputPhysType.getRowType().getFieldCount();
        for (int i = 0; i < fieldCount; i++) {
          expressions.add(
              Types.castIfNecessary(
                  inputPhysType.fieldClass(i),
                  inputPhysType.fieldReference(parameter, i)));
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
    private final PhysType physType;
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

      // Note that representation is ARRAY. This assumes that the table
      // returns a Object[] for each record. Actually a Table<T> can
      // return any type T. And, if it is a JdbcTable, we'd like to be
      // able to generate alternate accessors that return e.g. synthetic
      // records {T0 f0; T1 f1; ...} and don't box every primitive value.
      this.physType =
          PhysTypeImpl.of(
              (JavaTypeFactory) cluster.getTypeFactory(),
              getRowType(),
              ((EnumerableConvention) getConvention()).format);
    }


    @Override
    public RelNode copy(
        RelTraitSet traitSet, List<RelNode> inputs) {
      return new EnumerableTableAccessRel(
          getCluster(), traitSet, table, expression, elementType);
    }

    public PhysType getPhysType() {
      return physType;
    }

    public BlockExpression implement(EnumerableRelImplementor implementor) {
      return Blocks.toBlock(expression);
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
          EnumerableConvention.ARRAY,
          "EnumerableCalcRule");
    }

    public RelNode convert(RelNode rel) {
      final CalcRel calc = (CalcRel) rel;

      // If there's a multiset, let FarragoMultisetSplitter work on it
      // first.
      if (RexMultisetUtil.containsMultiset(calc.getProgram())) {
        return null;
      }

      return new EnumerableCalcRel(
          rel.getCluster(),
          rel.getTraitSet().replace(
              EnumerableConvention.ARRAY),
          convert(
              calc.getChild(),
              calc.getTraitSet().replace(EnumerableConvention.ARRAY)),
          calc.getProgram(),
          ProjectRelBase.Flags.Boxed);
    }
  }

  /**
   * Rule to convert an {@link EnumerableCalcRel} on an
   * {@link EnumerableConvention#ARRAY} input to one on a
   * {@link EnumerableConvention#CUSTOM} input.
   */
  public static class EnumerableCustomCalcRule extends RelOptRule {
    public static final RelOptRule INSTANCE =
        new EnumerableCustomCalcRule();

    private EnumerableCustomCalcRule() {
      super(
          some(EnumerableCalcRel.class,
              any(RelNode.class, EnumerableConvention.ARRAY)),
          "EnumerableCustomCalcRule");
    }

    public void onMatch(RelOptRuleCall call) {
      final EnumerableCalcRel calc = call.rel(0);
      final RelTraitSet traitSet =
          calc.getTraitSet().replace(EnumerableConvention.CUSTOM);
      call.transformTo(
          new EnumerableCalcRel(
              calc.getCluster(),
              calc.getTraitSet(),
              convert(call.rel(1), traitSet),
              calc.getProgram(),
              calc.flags));
    }
  }

  public static final ConverterRule ENUMERABLE_ARRAY_FROM_CUSTOM_RULE =
      new EnumerableConverterRule(
          EnumerableConvention.ARRAY,
          EnumerableConvention.CUSTOM);

  public static final ConverterRule ENUMERABLE_CUSTOM_FROM_ARRAY_RULE =
      new EnumerableConverterRule(
          EnumerableConvention.CUSTOM,
          EnumerableConvention.ARRAY);

  /**
   * Rule to convert a relational expression from
   * {@link EnumerableConvention#ARRAY} to another
   * {@link EnumerableConvention}.
   */
  private static class EnumerableConverterRule extends ConverterRule {
    private EnumerableConverterRule(
        EnumerableConvention out, EnumerableConvention in) {
      super(
          RelNode.class, in, out,
          "Enumerable-" + in.name() + "-to-" + out.name());
    }

    @Override
    public RelNode convert(RelNode rel) {
      if (rel instanceof EnumerableTableAccessRel) {
        // The physical row type of a table access is baked in.
        return null;
      }
      RelTraitSet newTraitSet =
          rel.getTraitSet().replace(
              ConventionTraitDef.instance, getOutTrait());
      if (rel instanceof EnumerableSortRel) {
        // The physical row type of a sort must be the same as its
        // input.
        EnumerableSortRel sortRel = (EnumerableSortRel) rel;
        return sortRel.copy(
            newTraitSet,
            convert(sortRel.getChild(), newTraitSet),
            sortRel.getCollation());
      }
      return rel.copy(newTraitSet, rel.getInputs());
    }
  }

  public static class EnumerableCalcRel
      extends SingleRel
      implements EnumerableRel {
    private final RexProgram program;

    private final PhysType physType;

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
      this.flags = flags;
      this.program = program;
      this.rowType = program.getOutputRowType();
      this.physType =
          PhysTypeImpl.of(
              (JavaTypeFactory) cluster.getTypeFactory(),
              getRowType(),
              (EnumerableConvention) getConvention());
    }

    public PhysType getPhysType() {
      return physType;
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

    public BlockExpression implement(EnumerableRelImplementor implementor) {
      final JavaTypeFactory typeFactory =
          (JavaTypeFactory) implementor.getTypeFactory();
      final BlockBuilder statements = new BlockBuilder();
      final EnumerableRel child = (EnumerableRel) getChild();

      // final Enumerable<Employee> inputEnumerable = <<child impl>>;
      // return new Enumerable<IntString>() {
      //     Enumerator<IntString> enumerator() {
      //         return new Enumerator<IntString>() {
      //             public void reset() {
      // ...
      Type outputJavaType = getPhysType().getJavaRowType();
      final Type enumeratorType =
          Types.of(
              Enumerator.class, outputJavaType);
      Type inputJavaType = child.getPhysType().getJavaRowType();
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
                        Pair.of(input, child.getPhysType()))));
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
                      Pair.of(input, child.getPhysType()))));
      list.add(
          Expressions.return_(
              null,
              physType.record(expressions)));
      BlockExpression currentBody =
          list.toBlock();

      final Expression inputEnumerable =
          statements.append(
              "inputEnumerable",
              implementor.visitChild(
                  this, 0, child),
              false);
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
      return statements.toBlock();
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
          EnumerableConvention.ARRAY,
          "EnumerableAggregateRule");
    }

    public RelNode convert(RelNode rel) {
      final AggregateRel agg = (AggregateRel) rel;
      final RelTraitSet traitSet =
          agg.getTraitSet().replace(EnumerableConvention.ARRAY);
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
    private final PhysType physType;

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
      this.physType =
          PhysTypeImpl.of(
              (JavaTypeFactory) cluster.getTypeFactory(),
              getRowType(),
              (EnumerableConvention) getConvention());
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

    public PhysType getPhysType() {
      return physType;
    }

    public BlockExpression implement(EnumerableRelImplementor implementor) {
      final JavaTypeFactory typeFactory =
          (JavaTypeFactory) implementor.getTypeFactory();
      final BlockBuilder statements = new BlockBuilder();
      final EnumerableRel child = (EnumerableRel) getChild();
      Expression childExp =
          statements.append(
              "child",
              implementor.visitChild(
                  this, 0, child));
      RelDataType inputRowType = getChild().getRowType();

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
      PhysType inputPhysType = child.getPhysType();

      ParameterExpression parameter =
          Expressions.parameter(inputPhysType.getJavaRowType(), "a0");

      final List<Expression> keyExpressions = Expressions.list();
      PhysType keyPhysType =
          inputPhysType.project(
              Util.toList(groupSet),
              JavaRowFormat.CUSTOM);
      final int keyArity = groupSet.cardinality();
      for (int groupKey : Util.toIter(groupSet)) {
        keyExpressions.add(
            inputPhysType.fieldReference(parameter, groupKey));
      }
      final Expression keySelector =
          statements.append(
              "keySelector",
              child.getPhysType().generateSelector(
                  parameter,
                  Util.toList(groupSet)));

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
      return statements.toBlock();
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
          EnumerableConvention.ARRAY,
          "EnumerableSortRule");
    }

    public RelNode convert(RelNode rel) {
      final SortRel sort = (SortRel) rel;
      final RelTraitSet traitSet =
          sort.getTraitSet().replace(EnumerableConvention.ARRAY);
      return new EnumerableSortRel(
          rel.getCluster(),
          traitSet,
          convert(sort.getChild(), traitSet),
          sort.getCollation());
    }
  }

  public static class EnumerableSortRel
      extends SortRel
      implements EnumerableRel {
    private final PhysType physType;

    public EnumerableSortRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode child,
        RelCollation collation) {
      super(cluster, traitSet, child, collation);
      assert getConvention() instanceof EnumerableConvention;
      assert getConvention() == child.getConvention();
      this.physType =
          PhysTypeImpl.of(
              (JavaTypeFactory) cluster.getTypeFactory(),
              getRowType(),
              (EnumerableConvention) getConvention());
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

    public PhysType getPhysType() {
      return physType;
    }

    public BlockExpression implement(EnumerableRelImplementor implementor) {
      final BlockBuilder statements = new BlockBuilder();
      final EnumerableRel child = (EnumerableRel) getChild();
      Expression childExp =
          statements.append(
              "child",
              implementor.visitChild(
                  this, 0, child));

      PhysType inputPhysType = child.getPhysType();
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
      return statements.toBlock();
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
          EnumerableConvention.ARRAY,
          "EnumerableUnionRule");
    }

    public RelNode convert(RelNode rel) {
      final UnionRel union = (UnionRel) rel;
      final RelTraitSet traitSet =
          union.getTraitSet().replace(
              EnumerableConvention.ARRAY);
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
    private final PhysType physType;

    public EnumerableUnionRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelNode> inputs,
        boolean all) {
      super(cluster, traitSet, inputs, all);
      this.physType =
          PhysTypeImpl.of(
              (JavaTypeFactory) cluster.getTypeFactory(),
              getRowType(),
              (EnumerableConvention) getConvention());
    }

    public EnumerableUnionRel copy(
        RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
      return new EnumerableUnionRel(getCluster(), traitSet, inputs, all);
    }

    public PhysType getPhysType() {
      return physType;
    }

    public BlockExpression implement(EnumerableRelImplementor implementor) {
      final BlockBuilder statements = new BlockBuilder();
      Expression unionExp = null;
      for (Ord<RelNode> ord : Ord.zip(inputs)) {
        EnumerableRel input = (EnumerableRel) ord.e;
        Expression childExp =
            statements.append(
                "child" + ord.i,
                implementor.visitChild(this, ord.i, input));

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
      }

      statements.add(unionExp);
      return statements.toBlock();
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
          EnumerableConvention.ARRAY,
          "EnumerableIntersectRule");
    }

    public RelNode convert(RelNode rel) {
      final IntersectRel intersect = (IntersectRel) rel;
      if (intersect.all) {
        return null; // INTERSECT ALL not implemented
      }
      final RelTraitSet traitSet =
          intersect.getTraitSet().replace(
              EnumerableConvention.ARRAY);
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
    private final PhysType physType;

    public EnumerableIntersectRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelNode> inputs,
        boolean all) {
      super(cluster, traitSet, inputs, all);
      assert !all;
      this.physType =
          PhysTypeImpl.of(
              (JavaTypeFactory) cluster.getTypeFactory(),
              getRowType(),
              (EnumerableConvention) getConvention());
    }

    public EnumerableIntersectRel copy(
        RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
      return new EnumerableIntersectRel(
          getCluster(),
          traitSet,
          inputs,
          all);
    }

    public PhysType getPhysType() {
      return physType;
    }

    public BlockExpression implement(EnumerableRelImplementor implementor) {
      final BlockBuilder statements = new BlockBuilder();
      Expression intersectExp = null;
      for (int i = 0; i < inputs.size(); i++) {
        RelNode input = inputs.get(i);
        Expression childExp =
            statements.append(
                "child" + i,
                implementor.visitChild(
                    this, i, (EnumerableRel) input));

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
      }

      statements.add(intersectExp);
      return statements.toBlock();
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
          EnumerableConvention.ARRAY,
          "EnumerableMinusRule");
    }

    public RelNode convert(RelNode rel) {
      final MinusRel minus = (MinusRel) rel;
      if (minus.all) {
        return null; // EXCEPT ALL not implemented
      }
      final RelTraitSet traitSet =
          rel.getTraitSet().replace(
              EnumerableConvention.ARRAY);
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
    private final PhysType physType;

    public EnumerableMinusRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelNode> inputs,
        boolean all) {
      super(cluster, traitSet, inputs, all);
      assert !all;
      this.physType =
          PhysTypeImpl.of(
              (JavaTypeFactory) cluster.getTypeFactory(),
              getRowType(),
              (EnumerableConvention) getConvention());
    }

    public EnumerableMinusRel copy(
        RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
      return new EnumerableMinusRel(
          getCluster(),
          traitSet,
          inputs,
          all);
    }

    public PhysType getPhysType() {
      return physType;
    }

    public BlockExpression implement(EnumerableRelImplementor implementor) {
      final BlockBuilder statements = new BlockBuilder();
      Expression minusExp = null;
      for (int i = 0; i < inputs.size(); i++) {
        RelNode input = inputs.get(i);
        Expression childExp =
            statements.append(
                "child" + i,
                implementor.visitChild(
                    this, i, (EnumerableRel) input));

        if (minusExp == null) {
          minusExp = childExp;
        } else {
          minusExp =
              Expressions.call(
                  minusExp,
                  BuiltinMethod.EXCEPT.method,
                  childExp);
        }
      }

      statements.add(minusExp);
      return statements.toBlock();
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
          EnumerableConvention.ARRAY,
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
          modify.getTraitSet().replace(EnumerableConvention.CUSTOM);
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
    private final PhysType physType;
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
      this.physType =
          PhysTypeImpl.of(
              (JavaTypeFactory) cluster.getTypeFactory(),
              getRowType(),
              (EnumerableConvention) getConvention());
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

    public PhysType getPhysType() {
      return physType;
    }

    public BlockExpression implement(EnumerableRelImplementor implementor) {
      final BlockBuilder builder = new BlockBuilder();
      Expression childExp =
          builder.append(
              "child",
              implementor.visitChild(
                  this, 0, (EnumerableRel) getChild()));
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
        final PhysType childPhysType =
            ((EnumerableRel) getChild()).getPhysType();
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
      return builder.toBlock();
    }
  }

  public static final EnumerableValuesRule ENUMERABLE_VALUES_RULE =
      new EnumerableValuesRule();

  public static class EnumerableValuesRule extends ConverterRule {
    private EnumerableValuesRule() {
      super(
          ValuesRel.class,
          Convention.NONE,
          EnumerableConvention.ARRAY,
          "EnumerableValuesRule");
    }

    @Override
    public RelNode convert(RelNode rel) {
      ValuesRel valuesRel = (ValuesRel) rel;
      return new EnumerableValuesRel(
          valuesRel.getCluster(),
          valuesRel.getRowType(),
          valuesRel.getTuples(),
          valuesRel.getTraitSet().plus(EnumerableConvention.ARRAY));
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
          valuesRel.getTraitSet().plus(EnumerableConvention.ARRAY));
    }
  }

  public static class EnumerableValuesRel
      extends ValuesRelBase
      implements EnumerableRel {
    private final PhysType physType;

    EnumerableValuesRel(
        RelOptCluster cluster,
        RelDataType rowType,
        List<List<RexLiteral>> tuples,
        RelTraitSet traitSet) {
      super(cluster, rowType, tuples, traitSet);
      this.physType =
          PhysTypeImpl.of(
              (JavaTypeFactory) cluster.getTypeFactory(),
              getRowType(),
              (EnumerableConvention) getConvention());
    }

    @Override
    public RelNode copy(
        RelTraitSet traitSet, List<RelNode> inputs) {
      assert inputs.isEmpty();
      return new EnumerableValuesRel(
          getCluster(), rowType, tuples, traitSet);
    }

    public PhysType getPhysType() {
      return physType;
    }

    public BlockExpression implement(EnumerableRelImplementor implementor) {
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
      return statements.toBlock();
    }
  }
}

// End JavaRules.java
