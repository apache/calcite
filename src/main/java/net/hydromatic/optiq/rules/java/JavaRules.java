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

import net.hydromatic.linq4j.Enumerable;
import net.hydromatic.linq4j.function.Function1;

import openjava.mop.OJClass;
import openjava.ptree.*;

import openjava.ptree.Expression;

import org.eigenbase.oj.rel.JavaRel;
import org.eigenbase.oj.rel.JavaRelImplementor;
import org.eigenbase.oj.util.OJUtil;
import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.ConverterRule;
import org.eigenbase.rel.metadata.RelMetadataQuery;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexLocalRef;
import org.eigenbase.rex.RexMultisetUtil;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexProgram;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Rules and relational operators for the {@link Enumerable} calling convention.
 *
 * @author jhyde
 */
public class JavaRules {
    // not used
    public static final RelOptRule JAVA_PROJECT_ROLE =
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

    public static class EnumerableJoinRel extends JoinRelBase implements JavaRel
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

        public ParseTree implement(JavaRelImplementor implementor) {
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
            return new MethodCall(
                implementor.visitJavaChild(this, 0, (JavaRel) left),
                "join",
                OJUtil.expressionList(
                    implementor.visitJavaChild(this, 1, (JavaRel) right),
                    EnumUtil.generateAccessor(left.getRowType(), leftKeys),
                    EnumUtil.generateAccessor(right.getRowType(), rightKeys),
                    EnumUtil.generateSelector(rowType)));

            /*
            input1.join(

            input2,

             */
        }

    }

    /**
     * Utilities for generating programs in the Enumerable (functional)
     * style.
     */
    public static class EnumUtil
    {
        static Expression generateAccessor(
            RelDataType rowType,
            List<Integer> fields)
        {
            assert fields.size() == 1
                : "composite keys not implemented yet";
            int field = fields.get(0);
            return new AllocationExpression(
                TypeName.forOJClass(OJClass.forClass(Function1.class)),
                new ExpressionList(),
                new MemberDeclarationList(
                    new MethodDeclaration(
                        new ModifierList(ModifierList.PUBLIC),
                        EnumUtil.toTypeName(
                            rowType.getFields()[field].getType()),
                        "apply",
                        new ParameterList(
                            new Parameter(
                                EnumUtil.toTypeName(rowType),
                                "v1")),
                        new TypeName[0],
                        new StatementList(
                            new ReturnStatement()
                        )

                        )
                    )
                );

            /*
            return new Function1<Employee, Integer>() {
                public Integer apply(Employee p0) {
                    return
                }
            }
            */
        }

        private static TypeName toTypeName(RelDataType type) {
            return null;
        }

        public static Expression generateSelector(RelDataType rowType) {
            return null;
        }
    }

    public static class EnumerableTableAccessRel
        extends TableAccessRelBase
        implements JavaRel
    {
        public EnumerableTableAccessRel(
            RelOptCluster cluster,
            RelOptTable table,
            RelOptConnection connection)
        {
            super(
                cluster,
                cluster.traitSetOf(CallingConvention.ENUMERABLE),
                table,
                connection);
        }

        public ParseTree implement(JavaRelImplementor implementor) {
            throw new UnsupportedOperationException();
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

            // REVIEW: want to move canTranslate into RelImplementor
            // and implement it for Java & C++ calcs.
            final JavaRelImplementor relImplementor =
                rel.getCluster().getPlanner().getJavaRelImplementor(rel);
            if (!relImplementor.canTranslate(
                    convertedChild,
                    calc.getProgram()))
            {
                // Some of the expressions cannot be translated into Java
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
        implements JavaRel
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

        public double getRows()
        {
            return FilterRel.estimateFilteredRows(
                getChild(), program);
        }

        public RelOptCost computeSelfCost(RelOptPlanner planner)
        {
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

        /**
         * Burrows into a synthetic record and returns the underlying relation
         * which provides the field called <code>fieldName</code>.
         */
        public JavaRel implementFieldAccess(
            JavaRelImplementor implementor,
            String fieldName)
        {
            if (!isBoxed()) {
                return implementor.implementFieldAccess(
                    (JavaRel) getChild(),
                    fieldName);
            }
            RelDataType type = getRowType();
            int field = type.getFieldOrdinal(fieldName);
            RexLocalRef ref = program.getProjectList().get(field);
            final int index = ref.getIndex();
            return implementor.findRel(
                this,
                program.getExprList().get(index));
        }

        public ParseTree implement(JavaRelImplementor implementor)
        {
            Expression childExp =
                implementor.visitJavaChild(this, 0, (JavaRel) getChild());
            RelDataType outputRowType = getRowType();
            RelDataType inputRowType = getChild().getRowType();

            Variable varInputRow = implementor.newVariable();
            implementor.bind(
                getChild(),
                varInputRow);

            return null /* implementAbstract(
                implementor,
                this,
                childExp,
                varInputRow,
                inputRowType,
                outputRowType,
                program,
                null) */;
        }

        public RexProgram getProgram() {
            return program;
        }

        private static Statement assignInputRow(
            OJClass inputRowClass,
            Variable varInputRow,
            Variable varInputObj)
        {
            return new ExpressionStatement(
                new AssignmentExpression(
                    varInputRow,
                    AssignmentExpression.EQUALS,
                    new CastExpression(
                        TypeName.forOJClass(inputRowClass),
                        varInputObj)));
        }
    }

}

// End JavaRules.java
