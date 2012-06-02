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
import net.hydromatic.linq4j.function.Function1;
import net.hydromatic.linq4j.function.Function2;

import net.hydromatic.optiq.impl.java.JavaTypeFactory;

import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.ConverterRule;
import org.eigenbase.rel.metadata.RelMetadataQuery;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexMultisetUtil;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexProgram;

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

    // not used
    public static final RelOptRule JAVA_PROJECT_RULE =
        new RelOptRule(
            new RelOptRuleOperand(
                ProjectRel.class,
                CallingConvention.NONE,
                new RelOptRuleOperand(
                    RelNode.class, RelOptRuleOperand.Dummy.ANY)),
            "JavaProjectRule")
        {
            public void onMatch(RelOptRuleCall call) {
            }
        };

    public static final RelOptRule ENUMERABLE_JOIN_RULE =
        new ConverterRule(
            JoinRel.class,
            CallingConvention.NONE,
            CallingConvention.ENUMERABLE,
            "IterJoinRule")
        {
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
        };
    public static final boolean BRIDGE_METHODS = true;

    public static class EnumerableJoinRel
        extends JoinRelBase
        implements EnumerableRel
    {
        static final Method join;

        static {
            try {
                join = ExtendedEnumerable.class.getMethod(
                    "join",
                    Enumerable.class,
                    Function1.class,
                    Function1.class,
                    Function2.class);
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        }

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

        public Expression implement(EnumerableRelImplementor implementor) {
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
            return Expressions.call(
                implementor.visitChild(this, 0, (EnumerableRel) left),
                join,
                implementor.visitChild(this, 1, (EnumerableRel) right),
                EnumUtil.generateAccessor(
                    typeFactory, left.getRowType(), leftKeys),
                EnumUtil.generateAccessor(
                    typeFactory, right.getRowType(), rightKeys),
                generateSelector(typeFactory));
        }

        Expression generateSelector(JavaTypeFactory typeFactory) {
            // A parameter for each input.
            final List<ParameterExpression> parameters =
                Arrays.asList(
                    Expressions.parameter(
                        EnumUtil.javaClass(typeFactory, left.getRowType()),
                        "left"),
                    Expressions.parameter(
                        EnumUtil.javaClass(typeFactory, right.getRowType()),
                        "right"));

            // Generate all fields.
            final List<Expression> expressions =
                new ArrayList<Expression>();
            int i = 0;
            for (RelNode rel : getInputs()) {
                RelDataType inputRowType = rel.getRowType();
                final ParameterExpression parameter = parameters.get(i++);
                for (RelDataTypeField field : inputRowType.getFields()) {
                    expressions.add(fieldReference(parameter, field));
                }
            }
            return Expressions.lambda(
                Function2.class,
                Expressions.return_(
                    null,
                    Expressions.newArrayInit(
                        Object.class,
                        expressions)),
                parameters);
        }

        private Expression fieldReference(
            ParameterExpression parameter,
            RelDataTypeField field)
        {
            return parameter.getClass().isArray()
                ? Expressions.arrayIndex(
                    parameter,
                    Expressions.constant(field.getIndex()))
                : Expressions.field(
                    parameter,
                    field.getName());
        }
    }

    /**
     * Utilities for generating programs in the Enumerable (functional)
     * style.
     */
    public static class EnumUtil
    {
        static Expression generateAccessor(
            JavaTypeFactory typeFactory,
            RelDataType rowType,
            List<Integer> fields)
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
                Expressions.parameter(typeFactory.getJavaClass(rowType), "v1");
            return Expressions.lambda(
                Function1.class,
                Expressions.return_(
                    null,
                    Expressions.field(v1, Types.nthField(field, v1.getType()))),
                v1);
        }

        static Class javaClass(
            JavaTypeFactory typeFactory, RelDataType type)
        {
            final Class clazz = typeFactory.getJavaClass(type);
            return clazz == null ? Object[].class : clazz;
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
            this.expression = expression;
        }

        public Expression implement(EnumerableRelImplementor implementor) {
            return expression;
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

        public boolean isBoxed()
        {
            return (flags & ProjectRelBase.Flags.Boxed)
                   == ProjectRelBase.Flags.Boxed;
        }

        public Expression implement(EnumerableRelImplementor implementor) {
            final JavaTypeFactory typeFactory =
                (JavaTypeFactory) implementor.getTypeFactory();
            Expression childExp =
                implementor.visitChild(this, 0, (EnumerableRel) getChild());
            RelDataType outputRowType = getRowType();
            RelDataType inputRowType = getChild().getRowType();

            // With calc:
            //
            // new Enumerable<IntString>() {
            //     final Enumerator<Employee> child = <<child impl>>
            //     Enumerator<IntString> enumerator() {
            //         return new Enumerator<IntString() {
            //             public void reset() {
            // ...
            Class outputJavaType = typeFactory.getJavaClass(outputRowType);
            if (outputJavaType == null) {
                if (outputRowType.getFieldCount() == 1) {
                    outputJavaType =
                        typeFactory.getJavaClass(
                            outputRowType.getFieldList().get(0).getType());
                }
                if (outputJavaType == null) {
                    outputJavaType = Object.class;
                }
            }
            final Type enumeratorType =
                Types.of(
                    Enumerator.class, outputJavaType);
            Class inputJavaType = typeFactory.getJavaClass(inputRowType);
            ParameterExpression inputEnumerable =
                Expressions.parameter(
                    Types.of(
                        Enumerable.class,
                        inputJavaType),
                    "inputEnumerable");
            ParameterExpression inputEnumerator =
                Expressions.parameter(
                    Types.of(
                        Enumerator.class, inputJavaType),
                "inputEnumerator");
            Expression input =
                Expressions.convert_(
                    Expressions.call(
                        inputEnumerator,
                        "current",
                        Collections.<Expression>emptyList()),
                    inputJavaType);

            Expression moveNextBody;
            if (program.getCondition() == null) {
                moveNextBody =
                    Expressions.call(
                        inputEnumerator,
                        "moveNext",
                        Collections.<Expression>emptyList());
            } else {
                final List<Expression> list = Expressions.list();
                Expression condition =
                    RexToLixTranslator.translateCondition(
                        Collections.<Expression>singletonList(input),
                        program,
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
                                "moveNext",
                                Collections.<Expression>emptyList()),
                            Expressions.block(list)),
                        Expressions.return_(
                            null,
                            Expressions.constant(false)));
            }

            final List<Expression> list = Expressions.list();
            List<Expression> expressions =
                RexToLixTranslator.translateProjects(
                    Collections.<Expression>singletonList(input),
                    program,
                    list);
            list.add(
                Expressions.return_(
                    null,
                    expressions.size() == 1
                        ? expressions.get(0)
                        : Expressions.newArrayInit(
                            Object.class,
                            expressions)));
            Expression currentBody =
                Expressions.block(list);

            return Expressions.new_(
                ABSTRACT_ENUMERABLE_CTOR,
                // Collections.singletonList(inputRowType), // TODO: generics
                Collections.<Expression>emptyList(),
                Collections.<Member>emptyList(),
                Arrays.<MemberDeclaration>asList(
                    Expressions.fieldDecl(
                        Modifier.FINAL,
                        inputEnumerable,
                        childExp),
                    Expressions.methodDecl(
                        Modifier.PUBLIC,
                        enumeratorType,
                        "enumerator",
                        Collections.<ParameterExpression>emptyList(),
                        Expressions.new_(
                            enumeratorType,
                            Collections.<Expression>emptyList(),
                            Collections.<Member>emptyList(),
                            Expressions.<MemberDeclaration>list(
                                Expressions.fieldDecl(
                                    Modifier.PUBLIC | Modifier.FINAL,
                                    inputEnumerator,
                                    Expressions.call(
                                        inputEnumerable,
                                        "enumerator",
                                        Collections.<Expression>emptyList())),
                                Expressions.methodDecl(
                                    Modifier.PUBLIC,
                                    Void.TYPE,
                                    "reset",
                                    Collections
                                        .<ParameterExpression>emptyList(),
                                    Expressions.call(
                                        inputEnumerator,
                                        "reset",
                                        Collections.<Expression>emptyList())),
                                Expressions.methodDecl(
                                    Modifier.PUBLIC,
                                    Boolean.TYPE,
                                    "moveNext",
                                    Collections
                                        .<ParameterExpression>emptyList(),
                                    moveNextBody),
                                Expressions.methodDecl(
                                    Modifier.PUBLIC,
                                    BRIDGE_METHODS
                                        ? Object.class
                                        : outputJavaType,
                                    "current",
                                    Collections
                                        .<ParameterExpression>emptyList(),
                                    currentBody))))));
        }

        public RexProgram getProgram() {
            return program;
        }
    }

    void foo() {
        new AbstractEnumerable<IntString>() {
            final Enumerable<Employee> childEnumerable =
                Linq4j.emptyEnumerable();
            public Enumerator<IntString> enumerator() {
                final Enumerator<Employee> input = childEnumerable.enumerator();
                return new Enumerator<IntString>() {
                    public IntString current() {
                        int v0 = input.current().empid * 2;
                        String v1 = input.current().name;
                        return new IntString(v0, v1);
                    }

                    public boolean moveNext() {
                        while (input.moveNext()) {
                            int v0 = input.current().empid;
                            boolean v1 = v0 < 200;
                            boolean v2 = v0 > 150;
                            boolean v3 = v1 && v2;
                            if (v3) {
                                return true;
                            }
                        }
                        return false;
                    }

                    public void reset() {
                        input.reset();
                    }
                };
            }
        };
    }

    static class IntString {
        int n;
        String s;

        IntString(int n, String s) {
            this.n = n;
            this.s = s;
        }
    }

    static class Employee {
        int empid;
        String name;
    }
}

// End JavaRules.java
