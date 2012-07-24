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
import org.eigenbase.sql.fun.SqlStdOperatorTable;

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

    private static final Method JOIN_METHOD =
        Types.lookupMethod(
            ExtendedEnumerable.class,
            "join",
            Enumerable.class,
            Function1.class,
            Function1.class,
            Function2.class);

    private static final Method SELECT_METHOD =
        Types.lookupMethod(
            ExtendedEnumerable.class, "select", Function1.class);

    private static final Method GROUP_BY_METHOD =
        Types.lookupMethod(
            ExtendedEnumerable.class,
            "groupBy",
            Function1.class);

    private static final Method ORDER_BY_METHOD =
        Types.lookupMethod(
            ExtendedEnumerable.class,
            "orderBy",
            Function1.class,
            Comparator.class);

    private static final Method UNION_METHOD =
        Types.lookupMethod(
            ExtendedEnumerable.class,
            "union",
            Enumerable.class);

    private static final Method CONCAT_METHOD =
        Types.lookupMethod(
            ExtendedEnumerable.class, "concat", Enumerable.class);

    private static final Method INTERSECT_METHOD =
        Types.lookupMethod(
            ExtendedEnumerable.class, "intersect", Enumerable.class);

    private static final Method EXCEPT_METHOD =
        Types.lookupMethod(
            ExtendedEnumerable.class, "except", Enumerable.class);

    private static final Method SINGLETON_ENUMERABLE_METHOD =
        Types.lookupMethod(
            Linq4j.class, "singletonEnumerable", Object.class);

    private static final Method ARRAY_COMPARER_METHOD =
        Types.lookupMethod(
            Functions.class, "arrayComparer");

    public static final Method LINQ4J_AS_ENUMERABLE_METHOD =
        Types.lookupMethod(
            Linq4j.class, "asEnumerable", Object[].class);

    public static final Method ENUMERATOR_CURRENT_METHOD =
        Types.lookupMethod(
            Enumerator.class, "current");

    public static final Method ENUMERATOR_MOVE_NEXT_METHOD =
        Types.lookupMethod(
            Enumerator.class, "moveNext");

    public static final Method ENUMERATOR_RESET_METHOD =
        Types.lookupMethod(
            Enumerator.class, "reset");

    public static final Method ENUMERABLE_ENUMERATOR_METHOD =
        Types.lookupMethod(
            Enumerable.class, "enumerator");

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
        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            assert inputs.size() == 2;
            return new EnumerableJoinRel(
                getCluster(),
                traitSet,
                inputs.get(0),
                inputs.get(1),
                condition,
                joinType,
                variablesStopped);
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
                : "EnumerableJoin is equi only"; // TODO: stricter pre-check
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
                    JOIN_METHOD,
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
            RelOptConnection connection,
            Expression expression)
        {
            super(
                cluster,
                cluster.traitSetOf(CallingConvention.ENUMERABLE),
                table,
                connection);
            if (Types.isArray(expression.getType())) {
                expression =
                    Expressions.call(LINQ4J_AS_ENUMERABLE_METHOD, expression);
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
                        ENUMERATOR_CURRENT_METHOD),
                    inputJavaType);

            BlockExpression moveNextBody;
            if (program.getCondition() == null) {
                moveNextBody =
                    Blocks.toFunctionBlock(
                        Expressions.call(
                            inputEnumerator,
                            ENUMERATOR_MOVE_NEXT_METHOD));
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
                                ENUMERATOR_MOVE_NEXT_METHOD),
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
                                ENUMERABLE_ENUMERATOR_METHOD)),
                        EnumUtil.overridingMethodDecl(
                            ENUMERATOR_RESET_METHOD,
                            NO_PARAMS,
                            Blocks.toFunctionBlock(
                                Expressions.call(
                                    inputEnumerator,
                                    ENUMERATOR_RESET_METHOD))),
                        EnumUtil.overridingMethodDecl(
                            ENUMERATOR_MOVE_NEXT_METHOD,
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
                                ENUMERABLE_ENUMERATOR_METHOD.getName(),
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
                agg.getAggCallList(),
                agg.getGroupCount());
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
            List<AggregateCall> aggCalls,
            int groupCount)
        {
            super(
                cluster,
                traitSet.plus(CallingConvention.ENUMERABLE),
                child,
                groupCount,
                aggCalls);
        }

        public EnumerableAggregateRel copy(
            RelTraitSet traitSet, List<RelNode> inputs)
        {
            return new EnumerableAggregateRel(
                getCluster(),
                traitSet,
                sole(inputs),
                aggCalls,
                groupCount);
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
            // Function1<Grouping<Integer, Employee>, Object[]> selector =
            //     new Function1<Grouping<Integer, Employee>, Object[]>() {
            //         public Object[] apply(Grouping<Integer, Employee> a0) {
            //             return new Object[] {
            //                 a0.getKey(),
            //                 a0.sum(
            //                     new IntegerFunction1<Employee>() {
            //                         public int apply(Employee a0) {
            //                             return a0.empid;
            //                         }
            //                     }
            //                 ),
            //                 a0.count()
            //             };
            //         }
            //     };
            // return childEnumerable.groupBy(keySelector)
            //     .select(selector);
            Class inputJavaType = EnumUtil.javaRowClass(
                typeFactory, inputRowType);

            ParameterExpression parameter =
                Expressions.parameter(inputJavaType, "a0");

            final List<Expression> keyExpressions = Expressions.list();
            for (int i = 0; i < groupCount; i++) {
                keyExpressions.add(
                    EnumUtil.inputFieldReference(
                        inputRowType, parameter, i));
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

            ParameterExpression grouping =
                Expressions.parameter(Grouping.class, "grouping");
            final Expressions.FluentList<Statement> statements2 =
                Expressions.list();
            final List<Expression> expressions = Expressions.list();
            switch (groupCount) {
            case 0:
                for (AggregateCall aggCall : aggCalls) {
                    expressions.add(
                        translate(
                            typeFactory, inputRowType, childExp, aggCall));
                }
                statements.add(
                    Expressions.return_(
                        null,
                        Expressions.call(
                            null,
                            SINGLETON_ENUMERABLE_METHOD,
                            expressions.size() == 1
                                ? expressions.get(0)
                                : Expressions.newArrayInit(
                                    Object.class, expressions))));
                return statements.toBlock();
            case 1:
                expressions.add(
                    Expressions.call(
                        grouping, "getKey"));
                break;
            default:
                DeclarationExpression keyDeclaration =
                    Expressions.declare(
                        Modifier.FINAL,
                        "key",
                        Expressions.convert_(
                            Expressions.call(
                                grouping, "getKey"),
                            Object[].class));
                statements2.append(keyDeclaration);
                for (int i = 0; i < groupCount; i++) {
                    expressions.add(
                        Expressions.arrayIndex(
                            keyDeclaration.parameter, Expressions.constant(i)));
                }
                break;
            }
            for (AggregateCall aggCall : aggCalls) {
                expressions.add(
                    translate(typeFactory, inputRowType, grouping, aggCall));
            }
            statements2.add(
                Expressions.return_(
                    null,
                    expressions.size() == 1
                        ? expressions.get(0)
                        : Expressions.newArrayInit(
                            Object.class, expressions)));

            final Expression selector =
                statements.append(
                    "selector",
                    Expressions.lambda(
                        Function1.class,
                        Expressions.block(statements2),
                        grouping));
            statements.add(
                Expressions.return_(
                    null,
                    Expressions.call(
                        Expressions.call(
                            childExp,
                            GROUP_BY_METHOD,
                            Expressions.<Expression>list()
                                .append(keySelector)
                                .appendIf(
                                    keyExpressions.size() > 1,
                                    Expressions.call(
                                        null, ARRAY_COMPARER_METHOD))),
                        SELECT_METHOD,
                        selector)));
            return statements.toBlock();
        }

        private Expression translate(
            JavaTypeFactory typeFactory,
            RelDataType rowType,
            Expression grouping,
            AggregateCall aggCall)
        {
            final Aggregation aggregation = aggCall.getAggregation();
            if (aggregation == SqlStdOperatorTable.countOperator) {
                return Expressions.call(
                    grouping,
                    "count");
            } else if (aggregation == SqlStdOperatorTable.sumOperator) {
                return Expressions.call(
                    grouping,
                    "sum",
                    EnumUtil.generateAccessor(
                        typeFactory,
                        rowType,
                        aggCall.getArgList(),
                        true));
            } else {
                throw new AssertionError("unknown agg " + aggregation);
            }
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

        public EnumerableSortRel copy(
            RelTraitSet traitSet, List<RelNode> inputs)
        {
            return new EnumerableSortRel(
                getCluster(),
                traitSet,
                sole(inputs),
                collations);
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
                        ORDER_BY_METHOD,
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
                            all ? CONCAT_METHOD : UNION_METHOD,
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
                            all ? CONCAT_METHOD : INTERSECT_METHOD,
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
                            EXCEPT_METHOD,
                            childExp);
                }
            }

            statements.add(minusExp);
            return statements.toBlock();
        }
    }
}

// End JavaRules.java
