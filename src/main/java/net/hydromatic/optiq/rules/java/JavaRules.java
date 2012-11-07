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
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.runtime.ArrayComparator;

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
import org.eigenbase.rex.RexMultisetUtil;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexProgram;
import org.eigenbase.util.Pair;
import org.eigenbase.util.Util;

import java.lang.reflect.*;
import java.util.*;

/**
 * Rules and relational operators for the {@link Enumerable} calling convention.
 *
 * @author jhyde
 */
public class JavaRules {
    private static final Constructor ABSTRACT_ENUMERABLE_CTOR =
        Types.lookupConstructor(AbstractEnumerable.class);


    public static final boolean BRIDGE_METHODS = true;

    private static final List<ParameterExpression> NO_PARAMS =
        Collections.emptyList();

    private static final List<Expression> NO_EXPRS =
        Collections.emptyList();

    public static final RelOptRule ENUMERABLE_JOIN_RULE =
        new EnumerableJoinRule();

    private static class EnumerableJoinRule extends ConverterRule {
        private EnumerableJoinRule() {
            super(
                JoinRel.class,
                CallingConvention.NONE,
                CallingConvention.ENUMERABLE,
                "EnumerableJoinRule");
        }

        @Override
        public RelNode convert(RelNode rel) {
            JoinRel join = (JoinRel) rel;
            List<RelNode> newInputs = convert(
                CallingConvention.ENUMERABLE,
                join.getInputs());
            if (newInputs == null) {
                return null;
            }
            return new EnumerableJoinRel(
                join.getCluster(),
                join.getTraitSet().replace(CallingConvention.ENUMERABLE),
                newInputs.get(0),
                newInputs.get(1),
                join.getCondition(),
                join.getJoinType(),
                join.getVariablesStopped());
        }
    }

    public static class EnumerableJoinRel
        extends JoinRelBase
        implements EnumerableRel
    {
        protected EnumerableJoinRel(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode left,
            RelNode right,
            RexNode condition,
            JoinRelType joinType,
            Set<String> variablesStopped)
        {
            super(
                cluster,
                traits,
                left,
                right,
                condition,
                joinType,
                variablesStopped);
        }

        @Override
        public EnumerableJoinRel copy(
            RelTraitSet traitSet,
            RexNode conditionExpr,
            RelNode left,
            RelNode right)
        {
            return new EnumerableJoinRel(
                getCluster(),
                traitSet,
                left,
                right,
                conditionExpr,
                joinType,
                variablesStopped);
        }

        @Override
        public RelOptCost computeSelfCost(RelOptPlanner planner) {
            // Inflate Java cost to make Cascading implementation more
            // attractive.
            return super.computeSelfCost(planner).multiplyBy(2d);
        }

        public BlockExpression implement(EnumerableRelImplementor implementor) {
            final List<Integer> leftKeys = new ArrayList<Integer>();
            final List<Integer> rightKeys = new ArrayList<Integer>();
            RexNode remaining =
                RelOptUtil.splitJoinCondition(
                    left,
                    right,
                    condition,
                    leftKeys,
                    rightKeys);
            assert remaining.isAlwaysTrue()
                : "EnumerableJoin is equi only; "
                + remaining; // TODO: stricter pre-check
            final JavaTypeFactory typeFactory =
                (JavaTypeFactory) left.getCluster().getTypeFactory();
            BlockBuilder list = new BlockBuilder();
            Expression leftExpression =
                list.append(
                    "left",
                    implementor.visitChild(this, 0, (EnumerableRel) left));
            Expression rightExpression =
                list.append(
                    "right",
                    implementor.visitChild(this, 1, (EnumerableRel) right));
            return list.append(
                Expressions.call(
                    leftExpression,
                    BuiltinMethod.JOIN.method,
                    rightExpression,
                    EnumUtil.generateAccessor(
                        typeFactory, left.getRowType(), leftKeys, false),
                    EnumUtil.generateAccessor(
                        typeFactory, right.getRowType(), rightKeys, false),
                    generateSelector(typeFactory)))
                .toBlock();
        }

        Expression generateSelector(JavaTypeFactory typeFactory) {
            // A parameter for each input.
            final List<ParameterExpression> parameters =
                Arrays.asList(
                    Expressions.parameter(
                        EnumUtil.javaRowClass(typeFactory, left.getRowType()),
                        "left"),
                    Expressions.parameter(
                        EnumUtil.javaRowClass(typeFactory, right.getRowType()),
                        "right"));

            // Generate all fields.
            final List<Expression> expressions =
                new ArrayList<Expression>();
            int i = 0;
            for (RelNode rel : getInputs()) {
                RelDataType inputRowType = rel.getRowType();
                final ParameterExpression parameter = parameters.get(i++);
                for (RelDataTypeField field : inputRowType.getFields()) {
                    expressions.add(EnumUtil.fieldReference(parameter, field));
                }
            }
            return Expressions.lambda(
                Function2.class,
                Expressions.newArrayInit(
                    Object.class,
                    expressions),
                parameters);
        }
    }

    /**
     * Utilities for generating programs in the Enumerable (functional)
     * style.
     */
    public static class EnumUtil
    {
        /** Declares a method that overrides another method. */
        static MethodDeclaration overridingMethodDecl(
            Method method,
            Iterable<ParameterExpression> parameters,
            BlockExpression body)
        {
            return Expressions.methodDecl(
                method.getModifiers() & ~Modifier.ABSTRACT,
                method.getReturnType(),
                method.getName(),
                parameters,
                body);
        }

        static Expression generateAccessor(
            JavaTypeFactory typeFactory,
            RelDataType rowType,
            List<Integer> fields,
            boolean primitive)
        {
            assert fields.size() == 1
                : "composite keys not implemented yet";
            int field = fields.get(0);

            // new Function1<Employee, Res> {
            //    public Res apply(Employee v1) {
            //        return v1.<fieldN>;
            //    }
            // }
            ParameterExpression v1 =
                Expressions.parameter(javaRowClass(typeFactory, rowType), "v1");
            Class returnType =
                javaRowClass(
                    typeFactory, rowType.getFieldList().get(field).getType());
            Expression fieldReference =
                Types.castIfNecessary(returnType, fieldReference(v1, field));
            return Expressions.lambda(
                primitive
                    ? Functions.functionClass(returnType)
                    : Function1.class,
                fieldReference,
                v1);
        }

        static Type javaClass(
            JavaTypeFactory typeFactory, RelDataType type)
        {
            final Type clazz = typeFactory.getJavaClass(type);
            return clazz instanceof Class ? clazz : Object[].class;
        }

        static Class javaRowClass(
            JavaTypeFactory typeFactory, RelDataType type)
        {
            if (type.isStruct() && type.getFieldCount() == 1) {
                type = type.getFieldList().get(0).getType();
            }
            final Type clazz = typeFactory.getJavaClass(type);
            return clazz instanceof Class ? (Class) clazz : Object[].class;
        }

        static Type computeOutputJavaType(
            JavaTypeFactory typeFactory, RelDataType outputRowType)
        {
            Type outputJavaType = typeFactory.getJavaClass(outputRowType);
            if (outputJavaType == null || !(outputJavaType instanceof Class)) {
                if (outputRowType.getFieldCount() == 1) {
                    outputJavaType =
                        typeFactory.getJavaClass(
                            outputRowType.getFieldList().get(0).getType());
                }
                if (outputJavaType == null) {
                    outputJavaType = Object.class;
                }
            }
            return outputJavaType;
        }

        static Expression fieldReference(
            Expression expression,
            RelDataTypeField field)
        {
            if (Types.isArray(expression.getType())) {
                return Expressions.arrayIndex(
                    expression, Expressions.constant(field.getIndex()));
            } else {
                return Expressions.field(
                    expression, field.getName());
            }
        }

        static Expression fieldReference(
            Expression expression, int field)
        {
            final Type type = expression.getType();
            if (Types.isArray(type)) {
                return Expressions.arrayIndex(
                    expression, Expressions.constant(field));
            } else {
                return Expressions.field(
                    expression,
                    Types.nthField(field, type));
            }
        }

        static Expression inputFieldReference(
            RelDataType inputRowType,
            Expression expression,
            int field)
        {
            List<RelDataTypeField> fieldList = inputRowType.getFieldList();
            if (fieldList.size() == 1) {
                assert field == 0;
                return expression;
            }
            return EnumUtil.fieldReference(
                expression, fieldList.get(field));
        }
    }

    public static class EnumerableTableAccessRel
        extends TableAccessRelBase
        implements EnumerableRel
    {
        private final Expression expression;

        public EnumerableTableAccessRel(
            RelOptCluster cluster,
            RelOptTable table,
            Expression expression)
        {
            super(
                cluster,
                cluster.traitSetOf(CallingConvention.ENUMERABLE),
                table);
            if (Types.isArray(expression.getType())) {
                expression =
                    Expressions.call(
                        BuiltinMethod.AS_ENUMERABLE.method,
                        expression);
            }
            this.expression = expression;
        }

        public BlockExpression implement(EnumerableRelImplementor implementor) {
            return Blocks.toBlock(expression);
        }
    }

    public static final EnumerableCalcRule ENUMERABLE_CALC_RULE =
        new EnumerableCalcRule();

    /**
     * Rule to convert a {@link CalcRel} to an
     * {@link net.hydromatic.optiq.rules.java.JavaRules.EnumerableCalcRel}.
     */
    private static class EnumerableCalcRule
        extends ConverterRule
    {
        private EnumerableCalcRule()
        {
            super(
                CalcRel.class,
                CallingConvention.NONE,
                CallingConvention.ENUMERABLE,
                "EnumerableCalcRule");
        }

        public RelNode convert(RelNode rel)
        {
            final CalcRel calc = (CalcRel) rel;
            final RelNode convertedChild =
                mergeTraitsAndConvert(
                    calc.getTraitSet(),
                    CallingConvention.ENUMERABLE,
                    calc.getChild());
            if (convertedChild == null) {
                // We can't convert the child, so we can't convert rel.
                return null;
            }

            // If there's a multiset, let FarragoMultisetSplitter work on it
            // first.
            if (RexMultisetUtil.containsMultiset(calc.getProgram())) {
                return null;
            }

            return new EnumerableCalcRel(
                rel.getCluster(),
                rel.getTraitSet(),
                convertedChild,
                calc.getProgram(),
                ProjectRelBase.Flags.Boxed);
        }
    }

    public static class EnumerableCalcRel
        extends SingleRel
        implements EnumerableRel
    {
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
            int flags)
        {
            super(
                cluster,
                traitSet.plus(CallingConvention.ENUMERABLE),
                child);
            this.flags = flags;
            this.program = program;
            this.rowType = program.getOutputRowType();
        }

        public void explain(RelOptPlanWriter pw)
        {
            program.explainCalc(this, pw);
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

        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs)
        {
            return new EnumerableCalcRel(
                getCluster(),
                traitSet,
                sole(inputs),
                program.copy(),
                getFlags());
        }

        public int getFlags()
        {
            return flags;
        }

        public BlockExpression implement(EnumerableRelImplementor implementor) {
            final JavaTypeFactory typeFactory =
                (JavaTypeFactory) implementor.getTypeFactory();
            final BlockBuilder statements = new BlockBuilder();
            RelDataType outputRowType = getRowType();
            RelDataType inputRowType = getChild().getRowType();

            // final Enumerable<Employee> inputEnumerable = <<child impl>>;
            // return new Enumerable<IntString>() {
            //     Enumerator<IntString> enumerator() {
            //         return new Enumerator<IntString>() {
            //             public void reset() {
            // ...
            Type outputJavaType =
                EnumUtil.computeOutputJavaType(typeFactory, outputRowType);
            final Type enumeratorType =
                Types.of(
                    Enumerator.class, outputJavaType);
            Class inputJavaType = EnumUtil.javaRowClass(
                typeFactory, inputRowType);
            ParameterExpression inputEnumerator =
                Expressions.parameter(
                    Types.of(
                        Enumerator.class, inputJavaType),
                "inputEnumerator");
            Expression input =
                Expressions.convert_(
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
                final List<Statement> list = Expressions.list();
                Expression condition =
                    RexToLixTranslator.translateCondition(
                        Collections.<Expression>singletonList(input),
                        program,
                        typeFactory,
                        list);
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
                            Expressions.block(list)),
                        Expressions.return_(
                            null,
                            Expressions.constant(false)));
            }

            final List<Statement> list = Expressions.list();
            List<Expression> expressions =
                RexToLixTranslator.translateProjects(
                    Collections.<Expression>singletonList(input),
                    program,
                    typeFactory,
                    list);
            list.add(
                Expressions.return_(
                    null,
                    expressions.size() == 1
                        ? expressions.get(0)
                        : Expressions.newArrayInit(
                            Object.class,
                            stripCasts(expressions))));
            BlockExpression currentBody =
                Expressions.block(list);

            final Expression inputEnumerable =
                statements.append(
                    "inputEnumerable",
                    implementor.visitChild(
                        this, 0, (EnumerableRel) getChild()));
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

        private Iterable<Expression> stripCasts(List<Expression> expressions) {
            final List<Expression> list = new ArrayList<Expression>();
            for (Expression expression : expressions) {
                while (expression.getNodeType() == ExpressionType.Convert) {
                    expression = ((UnaryExpression) expression).expression;
                }
                list.add(expression);
            }
            return list;
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
        extends ConverterRule
    {
        private EnumerableAggregateRule()
        {
            super(
                AggregateRel.class,
                CallingConvention.NONE,
                CallingConvention.ENUMERABLE,
                "EnumerableAggregateRule");
        }

        public RelNode convert(RelNode rel)
        {
            final AggregateRel agg = (AggregateRel) rel;
            final RelNode convertedChild =
                mergeTraitsAndConvert(
                    agg.getTraitSet(),
                    CallingConvention.ENUMERABLE,
                    agg.getChild());
            if (convertedChild == null) {
                // We can't convert the child, so we can't convert rel.
                return null;
            }

            return new EnumerableAggregateRel(
                rel.getCluster(),
                rel.getTraitSet(),
                convertedChild,
                agg.getGroupSet(),
                agg.getAggCallList());
        }
    }

    public static class EnumerableAggregateRel
        extends AggregateRelBase
        implements EnumerableRel
    {
        public EnumerableAggregateRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode child,
            BitSet groupSet,
            List<AggregateCall> aggCalls)
        {
            super(
                cluster,
                traitSet.plus(CallingConvention.ENUMERABLE),
                child,
                groupSet,
                aggCalls);
        }

        @Override
        public EnumerableAggregateRel copy(
            RelTraitSet traitSet, List<RelNode> inputs)
        {
            return new EnumerableAggregateRel(
                getCluster(),
                traitSet,
                sole(inputs),
                groupSet,
                aggCalls);
        }

        public BlockExpression implement(EnumerableRelImplementor implementor) {
            final JavaTypeFactory typeFactory =
                (JavaTypeFactory) implementor.getTypeFactory();
            final BlockBuilder statements = new BlockBuilder();
            Expression childExp =
                statements.append(
                    "child",
                    implementor.visitChild(
                        this, 0, (EnumerableRel) getChild()));
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
            Class inputJavaType = EnumUtil.javaRowClass(
                typeFactory, inputRowType);

            ParameterExpression parameter =
                Expressions.parameter(inputJavaType, "a0");

            final List<Expression> keyExpressions = Expressions.list();
            for (int groupKey : Util.toIter(groupSet)) {
                keyExpressions.add(
                    EnumUtil.inputFieldReference(
                        inputRowType, parameter, groupKey));
            }
            final Expression keySelector =
                statements.append(
                    "keySelector",
                    inputRowType.getFieldCount() == 1
                        ? Expressions.call(
                            Functions.class,
                            "identitySelector")
                        : Expressions.lambda(
                            Function1.class,
                            keyExpressions.size() == 1
                                ? keyExpressions.get(0)
                                : Expressions.newArrayInit(
                                    Object.class, keyExpressions),
                            parameter));

            final List<RexToLixTranslator.AggregateImplementor2> implementors =
                new ArrayList<RexToLixTranslator.AggregateImplementor2>();
            for (AggregateCall aggCall : aggCalls) {
                RexToLixTranslator.AggregateImplementor2 implementor2 =
                    RexToLixTranslator.ImpTable.INSTANCE.get2(
                        aggCall.getAggregation());
                if (implementor2 == null) {
                    throw new RuntimeException(
                        "cannot implement aggregate " + aggCall);
                }
                implementors.add(implementor2);
            }
            List<Pair<AggregateCall, RexToLixTranslator.AggregateImplementor2>>
                aggImps = Pair.zip(aggCalls, implementors);

            // Function0<Object[]> accumulatorInitializer =
            //     new Function0<Object[]>() {
            //         public Object[] apply() {
            //             return new Object[] {0, 0};
            //         }
            //     };
            final List<Expression> initExpressions =
                new ArrayList<Expression>();
            for (Pair<AggregateCall, RexToLixTranslator.AggregateImplementor2>
                pair : aggImps)
            {
                initExpressions.add(
                    pair.right.implementInit(
                        pair.left.getAggregation(),
                        EnumUtil.javaClass(typeFactory, pair.left.getType()),
                        fieldTypes(
                            typeFactory,
                            inputRowType,
                            pair.left.getArgList())));
            }
            final Expression accumulatorInitializer =
                statements.append(
                    "accumulatorInitializer",
                    Expressions.lambda(
                        Function0.class,
                        Expressions.newArrayInit(
                            Object.class, initExpressions)));

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
                Expressions.parameter(inputJavaType, "in");
            final ParameterExpression accParameter =
                Expressions.parameter(Object[].class, "acc");
            int i = 0;
            for (Pair<AggregateCall, RexToLixTranslator.AggregateImplementor2>
                aggImp : aggImps)
            {
                final Type type = initExpressions.get(i).type;
                final IndexExpression accumulator =
                    Expressions.arrayIndex(
                        accParameter,
                        Expressions.constant(i));
                ++i;
                bb2.add(
                    Expressions.statement(
                        Expressions.assign(
                            accumulator,
                            aggImp.right.implementAdd(
                                aggImp.left.getAggregation(),
                                Types.castIfNecessary(type, accumulator),
                                accessors(
                                    typeFactory,
                                    inputRowType,
                                    inParameter,
                                    aggImp.left)))));
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
            if (keyExpressions.size() == 0) {
                keyParameter = null;
            } else {
                final Type keyType =
                    keyExpressions.size() == 1
                        ? keyExpressions.get(0).type
                        : Object[].class;
                keyParameter = Expressions.parameter(keyType, "key");
                if (keyExpressions.size() == 1) {
                    results.add(keyParameter);
                } else {
                    for (int j = 0; j < keyExpressions.size(); j++) {
                        results.add(
                            Expressions.arrayIndex(
                                keyParameter, Expressions.constant(j)));
                    }
                }
            }
            i = 0;
            for (Pair<AggregateCall, RexToLixTranslator.AggregateImplementor2>
                aggImp : aggImps)
            {
                results.add(
                    aggImp.right.implementResult(
                        aggImp.left.getAggregation(),
                        Expressions.arrayIndex(
                            accParameter,
                            Expressions.constant(i++))));
            }
            if (keyExpressions.size() == 0) {
                final Expression resultSelector =
                    statements.append(
                        "resultSelector",
                        Expressions.lambda(
                            Function1.class,
                            results.size() == 1
                                ? results.get(0)
                                : Expressions.newArrayInit(
                                    Object.class, results),
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
                            Expressions.newArrayInit(
                                Object.class, results),
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
                                .appendIf(
                                    keyExpressions.size() > 1,
                                    Expressions.call(
                                        null,
                                        BuiltinMethod.ARRAY_COMPARER
                                            .method)))));
            }
            return statements.toBlock();
        }

        private List<Type> fieldTypes(
            final JavaTypeFactory typeFactory,
            final RelDataType inputRowType,
            final List<Integer> argList)
        {
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
            JavaTypeFactory typeFactory,
            RelDataType rowType,
            ParameterExpression v1,
            AggregateCall aggCall)
        {
            final List<Expression> expressions = new ArrayList<Expression>();
            for (int field : aggCall.getArgList()) {
                Class returnType =
                    EnumUtil.javaRowClass(
                        typeFactory,
                        rowType.getFieldList().get(field).getType());
                expressions.add(
                    Types.castIfNecessary(
                        returnType, EnumUtil.fieldReference(v1, field)));
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
        extends ConverterRule
    {
        private EnumerableSortRule()
        {
            super(
                SortRel.class,
                CallingConvention.NONE,
                CallingConvention.ENUMERABLE,
                "EnumerableSortRule");
        }

        public RelNode convert(RelNode rel)
        {
            final SortRel sort = (SortRel) rel;
            final RelNode convertedChild =
                mergeTraitsAndConvert(
                    sort.getTraitSet(),
                    CallingConvention.ENUMERABLE,
                    sort.getChild());
            if (convertedChild == null) {
                // We can't convert the child, so we can't convert rel.
                return null;
            }

            return new EnumerableSortRel(
                rel.getCluster(),
                rel.getTraitSet(),
                convertedChild,
                sort.getCollations());
        }
    }

    public static class EnumerableSortRel
        extends SortRel
        implements EnumerableRel
    {
        public EnumerableSortRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode child,
            List<RelFieldCollation> collations)
        {
            super(
                cluster,
                traitSet.plus(CallingConvention.ENUMERABLE),
                child,
                collations);
        }

        @Override
        public EnumerableSortRel copy(
            RelTraitSet traitSet,
            RelNode newInput,
            List<RelFieldCollation> newCollations)
        {
            return new EnumerableSortRel(
                getCluster(),
                traitSet,
                newInput,
                newCollations);
        }

        public BlockExpression implement(EnumerableRelImplementor implementor) {
            final JavaTypeFactory typeFactory =
                (JavaTypeFactory) implementor.getTypeFactory();
            final BlockBuilder statements = new BlockBuilder();
            Expression childExp =
                statements.append(
                    "child",
                    implementor.visitChild(
                        this, 0, (EnumerableRel) getChild()));

            RelDataType inputRowType = getChild().getRowType();
            Class inputJavaType = EnumUtil.javaRowClass(
                typeFactory, inputRowType);

            ParameterExpression parameter =
                Expressions.parameter(inputJavaType, "a0");
            final List<Expression> keyExpressions = Expressions.list();
            for (RelFieldCollation collation : collations) {
                keyExpressions.add(
                    EnumUtil.inputFieldReference(
                        inputRowType, parameter, collation.getFieldIndex()));
            }
            final Expression keySelector =
                statements.append(
                    "keySelector",
                    inputRowType.getFieldCount() == 1
                        ? Expressions.call(
                            Functions.class,
                            "identitySelector")
                        : Expressions.lambda(
                            Function1.class,
                            keyExpressions.size() == 1
                                ? keyExpressions.get(0)
                                : Expressions.newArrayInit(
                                    Object.class, keyExpressions),
                            parameter));

            Expression comparatorExp;
            if (collations.size() == 1) {
                RelFieldCollation collation = collations.get(0);
                switch (collation.getDirection()) {
                case Ascending:
                    comparatorExp = Expressions.constant(null);
                    break;
                default:
                    comparatorExp =
                        Expressions.call(
                            Collections.class,
                            "reverseOrder");
                }
            } else {
                List<Expression> directions =
                    new AbstractList<Expression>() {
                        public Expression get(int index) {
                            return Expressions.constant(
                                collations.get(index).getDirection()
                                == RelFieldCollation.Direction.Descending);
                        }
                        public int size() {
                            return collations.size();
                        }
                    };
                comparatorExp =
                    Expressions.new_(
                        ArrayComparator.class,
                        Collections.<Expression>singletonList(
                            Expressions.newArrayInit(
                                Boolean.TYPE,
                                directions)));
            }
            final Expression comparator =
                statements.append(
                    "comparator",
                    comparatorExp);

            statements.add(
                Expressions.return_(
                    null,
                    Expressions.call(
                        childExp,
                        BuiltinMethod.ORDER_BY.method,
                        keySelector,
                        comparator)));
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
        extends ConverterRule
    {
        private EnumerableUnionRule()
        {
            super(
                UnionRel.class,
                CallingConvention.NONE,
                CallingConvention.ENUMERABLE,
                "EnumerableUnionRule");
        }

        public RelNode convert(RelNode rel)
        {
            final UnionRel union = (UnionRel) rel;
            List<RelNode> convertedChildren = new ArrayList<RelNode>();
            for (RelNode child : union.getInputs()) {
                final RelNode convertedChild =
                    mergeTraitsAndConvert(
                        union.getTraitSet(),
                        CallingConvention.ENUMERABLE,
                        child);
                if (convertedChild == null) {
                    // We can't convert the child, so we can't convert rel.
                    return null;
                }
                convertedChildren.add(convertedChild);
            }
            return new EnumerableUnionRel(
                rel.getCluster(),
                rel.getTraitSet(),
                convertedChildren,
                !union.isDistinct());
        }
    }

    public static class EnumerableUnionRel
        extends UnionRelBase
        implements EnumerableRel
    {
        public EnumerableUnionRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelNode> inputs,
            boolean all)
        {
            super(
                cluster,
                traitSet.plus(CallingConvention.ENUMERABLE),
                inputs,
                all);
        }

        public EnumerableUnionRel copy(
            RelTraitSet traitSet, List<RelNode> inputs, boolean all)
        {
            return new EnumerableUnionRel(
                getCluster(),
                traitSet,
                inputs,
                all);
        }

        public BlockExpression implement(EnumerableRelImplementor implementor) {
            final BlockBuilder statements = new BlockBuilder();
            Expression unionExp = null;
            for (int i = 0; i < inputs.size(); i++) {
                RelNode input = inputs.get(i);
                Expression childExp =
                    statements.append(
                        "child" + i,
                        implementor.visitChild(
                            this, i, (EnumerableRel) input));

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
        extends ConverterRule
    {
        private EnumerableIntersectRule()
        {
            super(
                IntersectRel.class,
                CallingConvention.NONE,
                CallingConvention.ENUMERABLE,
                "EnumerableIntersectRule");
        }

        public RelNode convert(RelNode rel)
        {
            final IntersectRel intersect = (IntersectRel) rel;
            if (!intersect.isDistinct()) {
                return null; // INTERSECT ALL not implemented
            }
            List<RelNode> convertedChildren = new ArrayList<RelNode>();
            for (RelNode child : intersect.getInputs()) {
                final RelNode convertedChild =
                    mergeTraitsAndConvert(
                        intersect.getTraitSet(),
                        CallingConvention.ENUMERABLE,
                        child);
                if (convertedChild == null) {
                    // We can't convert the child, so we can't convert rel.
                    return null;
                }
                convertedChildren.add(convertedChild);
            }
            return new EnumerableIntersectRel(
                rel.getCluster(),
                rel.getTraitSet(),
                convertedChildren,
                !intersect.isDistinct());
        }
    }

    public static class EnumerableIntersectRel
        extends IntersectRelBase
        implements EnumerableRel
    {
        public EnumerableIntersectRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelNode> inputs,
            boolean all)
        {
            super(
                cluster,
                traitSet.plus(CallingConvention.ENUMERABLE),
                inputs,
                all);
            assert !all;
        }

        public EnumerableIntersectRel copy(
            RelTraitSet traitSet, List<RelNode> inputs, boolean all)
        {
            return new EnumerableIntersectRel(
                getCluster(),
                traitSet,
                inputs,
                all);
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
        extends ConverterRule
    {
        private EnumerableMinusRule()
        {
            super(
                MinusRel.class,
                CallingConvention.NONE,
                CallingConvention.ENUMERABLE,
                "EnumerableMinusRule");
        }

        public RelNode convert(RelNode rel)
        {
            final MinusRel minus = (MinusRel) rel;
            if (!minus.isDistinct()) {
                return null; // EXCEPT ALL not implemented
            }
            List<RelNode> convertedChildren = new ArrayList<RelNode>();
            for (RelNode child : minus.getInputs()) {
                final RelNode convertedChild =
                    mergeTraitsAndConvert(
                        minus.getTraitSet(),
                        CallingConvention.ENUMERABLE,
                        child);
                if (convertedChild == null) {
                    // We can't convert the child, so we can't convert rel.
                    return null;
                }
                convertedChildren.add(convertedChild);
            }
            return new EnumerableMinusRel(
                rel.getCluster(),
                rel.getTraitSet(),
                convertedChildren,
                !minus.isDistinct());
        }
    }

    public static class EnumerableMinusRel
        extends MinusRelBase
        implements EnumerableRel
    {
        public EnumerableMinusRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelNode> inputs,
            boolean all)
        {
            super(
                cluster,
                traitSet.plus(CallingConvention.ENUMERABLE),
                inputs,
                all);
            assert !all;
        }

        public EnumerableMinusRel copy(
            RelTraitSet traitSet, List<RelNode> inputs, boolean all)
        {
            return new EnumerableMinusRel(
                getCluster(),
                traitSet,
                inputs,
                all);
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
}

// End JavaRules.java
