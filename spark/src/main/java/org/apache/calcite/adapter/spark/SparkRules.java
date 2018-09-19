/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.spark;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.FilterToCalcRule;
import org.apache.calcite.rel.rules.ProjectToCalcRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexMultisetUtil;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import scala.Tuple2;

import java.lang.reflect.Type;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Random;

/**
 * Rules for the {@link SparkRel#CONVENTION Spark calling convention}.
 *
 * @see JdbcToSparkConverterRule
 */
public abstract class SparkRules {
  private SparkRules() {}

  public static List<RelOptRule> rules() {
    return ImmutableList.of(
        // TODO: add SparkProjectRule, SparkFilterRule, SparkProjectToCalcRule,
        // SparkFilterToCalcRule, and remove the following 2 rules.
        ProjectToCalcRule.INSTANCE,
        FilterToCalcRule.INSTANCE,
        EnumerableToSparkConverterRule.INSTANCE,
        SparkToEnumerableConverterRule.INSTANCE,
        SPARK_VALUES_RULE,
        SPARK_CALC_RULE);
  }

  /** Planner rule that converts from enumerable to Spark convention. */
  static class EnumerableToSparkConverterRule extends ConverterRule {
    public static final EnumerableToSparkConverterRule INSTANCE =
        new EnumerableToSparkConverterRule();

    private EnumerableToSparkConverterRule() {
      super(
          RelNode.class, EnumerableConvention.INSTANCE, SparkRel.CONVENTION,
          "EnumerableToSparkConverterRule");
    }

    @Override public RelNode convert(RelNode rel) {
      return new EnumerableToSparkConverter(rel.getCluster(),
          rel.getTraitSet().replace(SparkRel.CONVENTION), rel);
    }
  }

  /** Planner rule that converts from Spark to enumerable convention. */
  static class SparkToEnumerableConverterRule extends ConverterRule {
    public static final SparkToEnumerableConverterRule INSTANCE =
        new SparkToEnumerableConverterRule();

    private SparkToEnumerableConverterRule() {
      super(
          RelNode.class, SparkRel.CONVENTION, EnumerableConvention.INSTANCE,
          "SparkToEnumerableConverterRule");
    }

    @Override public RelNode convert(RelNode rel) {
      return new SparkToEnumerableConverter(rel.getCluster(),
          rel.getTraitSet().replace(EnumerableConvention.INSTANCE), rel);
    }
  }

  public static final SparkValuesRule SPARK_VALUES_RULE =
      new SparkValuesRule();

  /** Planner rule that implements VALUES operator in Spark convention. */
  public static class SparkValuesRule extends ConverterRule {
    private SparkValuesRule() {
      super(LogicalValues.class, Convention.NONE, SparkRel.CONVENTION,
          "SparkValuesRule");
    }

    @Override public RelNode convert(RelNode rel) {
      LogicalValues values = (LogicalValues) rel;
      return new SparkValues(
          values.getCluster(),
          values.getRowType(),
          values.getTuples(),
          values.getTraitSet().replace(getOutTrait()));
    }
  }

  /** VALUES construct implemented in Spark. */
  public static class SparkValues extends Values implements SparkRel {
    SparkValues(
        RelOptCluster cluster,
        RelDataType rowType,
        ImmutableList<ImmutableList<RexLiteral>> tuples,
        RelTraitSet traitSet) {
      super(cluster, rowType, tuples, traitSet);
    }

    @Override public RelNode copy(
        RelTraitSet traitSet, List<RelNode> inputs) {
      assert inputs.isEmpty();
      return new SparkValues(
          getCluster(), rowType, tuples, traitSet);
    }

    public Result implementSpark(Implementor implementor) {
/*
            return Linq4j.asSpark(
                new Object[][] {
                    new Object[] {1, 2},
                    new Object[] {3, 4}
                });
*/
      final JavaTypeFactory typeFactory =
          (JavaTypeFactory) getCluster().getTypeFactory();
      final BlockBuilder builder = new BlockBuilder();
      final PhysType physType =
          PhysTypeImpl.of(implementor.getTypeFactory(),
              getRowType(),
              JavaRowFormat.CUSTOM);
      final Type rowClass = physType.getJavaRowType();

      final List<Expression> expressions = new ArrayList<>();
      final List<RelDataTypeField> fields = rowType.getFieldList();
      for (List<RexLiteral> tuple : tuples) {
        final List<Expression> literals = new ArrayList<>();
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
          Expressions.return_(null,
              Expressions.call(SparkMethod.ARRAY_TO_RDD.method,
                  Expressions.call(SparkMethod.GET_SPARK_CONTEXT.method,
                      implementor.getRootExpression()),
                  Expressions.newArrayInit(Primitive.box(rowClass),
                      expressions))));
      return implementor.result(physType, builder.toBlock());
    }
  }

  public static final SparkCalcRule SPARK_CALC_RULE =
      new SparkCalcRule();

  /**
   * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalCalc} to an
   * {@link org.apache.calcite.adapter.spark.SparkRules.SparkCalc}.
   */
  private static class SparkCalcRule
      extends ConverterRule {
    private SparkCalcRule() {
      super(
          LogicalCalc.class,
          Convention.NONE,
          SparkRel.CONVENTION,
          "SparkCalcRule");
    }

    public RelNode convert(RelNode rel) {
      final LogicalCalc calc = (LogicalCalc) rel;

      // If there's a multiset, let FarragoMultisetSplitter work on it
      // first.
      final RexProgram program = calc.getProgram();
      if (RexMultisetUtil.containsMultiset(program)
          || program.containsAggs()) {
        return null;
      }

      return new SparkCalc(
          rel.getCluster(),
          rel.getTraitSet().replace(SparkRel.CONVENTION),
          convert(calc.getInput(),
              calc.getInput().getTraitSet().replace(SparkRel.CONVENTION)),
          program);
    }
  }

  /** Implementation of {@link org.apache.calcite.rel.core.Calc}
   * in Spark convention. */
  public static class SparkCalc extends SingleRel implements SparkRel {
    private final RexProgram program;

    public SparkCalc(RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        RexProgram program) {
      super(cluster, traitSet, input);
      assert getConvention() == SparkRel.CONVENTION;
      assert !program.containsAggs();
      this.program = program;
      this.rowType = program.getOutputRowType();
    }

    @Deprecated // to be removed before 2.0
    public SparkCalc(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
        RexProgram program, int flags) {
      this(cluster, traitSet, input, program);
      Util.discard(flags);
    }

    @Override public RelWriter explainTerms(RelWriter pw) {
      return program.explainCalc(super.explainTerms(pw));
    }

    @Override public double estimateRowCount(RelMetadataQuery mq) {
      return RelMdUtil.estimateFilteredRows(getInput(), program, mq);
    }

    @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      double dRows = mq.getRowCount(this);
      double dCpu = mq.getRowCount(getInput())
          * program.getExprCount();
      double dIo = 0;
      return planner.getCostFactory().makeCost(dRows, dCpu, dIo);
    }

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new SparkCalc(
          getCluster(),
          traitSet,
          sole(inputs),
          program);
    }

    @Deprecated // to be removed before 2.0
    public int getFlags() {
      return 1;
    }

    public Result implementSpark(Implementor implementor) {
      final JavaTypeFactory typeFactory = implementor.getTypeFactory();
      final BlockBuilder builder = new BlockBuilder();
      final SparkRel child = (SparkRel) getInput();

      final Result result = implementor.visitInput(this, 0, child);

      final PhysType physType =
          PhysTypeImpl.of(
              typeFactory, getRowType(), JavaRowFormat.CUSTOM);

      // final RDD<Employee> inputRdd = <<child adapter>>;
      // return inputRdd.flatMap(
      //   new FlatMapFunction<Employee, X>() {
      //          public List<X> call(Employee e) {
      //              if (!(e.empno < 10)) {
      //                  return Collections.emptyList();
      //              }
      //              return Collections.singletonList(
      //                  new X(...)));
      //          }
      //      })


      Type outputJavaType = physType.getJavaRowType();
      final Type rddType =
          Types.of(
              JavaRDD.class, outputJavaType);
      Type inputJavaType = result.physType.getJavaRowType();
      final Expression inputRdd_ =
          builder.append(
              "inputRdd",
              result.block);

      BlockBuilder builder2 = new BlockBuilder();

      final ParameterExpression e_ =
          Expressions.parameter(inputJavaType, "e");
      if (program.getCondition() != null) {
        Expression condition =
            RexToLixTranslator.translateCondition(
                program,
                typeFactory,
                builder2,
                new RexToLixTranslator.InputGetterImpl(
                    Collections.singletonList(
                        Pair.of((Expression) e_, result.physType))),
                null, implementor.getConformance());
        builder2.add(
            Expressions.ifThen(
                Expressions.not(condition),
                Expressions.return_(null,
                    Expressions.call(
                        BuiltInMethod.COLLECTIONS_EMPTY_LIST.method))));
      }

      final SqlConformance conformance = SqlConformanceEnum.DEFAULT;
      List<Expression> expressions =
          RexToLixTranslator.translateProjects(
              program,
              typeFactory,
              conformance,
              builder2,
              null,
              DataContext.ROOT,
              new RexToLixTranslator.InputGetterImpl(
                  Collections.singletonList(
                      Pair.of((Expression) e_, result.physType))),
              null);
      builder2.add(
          Expressions.return_(null,
              Expressions.convert_(
                  Expressions.call(
                      BuiltInMethod.COLLECTIONS_SINGLETON_LIST.method,
                      physType.record(expressions)),
                  List.class)));

      final BlockStatement callBody = builder2.toBlock();
      builder.add(
          Expressions.return_(
              null,
              Expressions.call(
                  inputRdd_,
                  SparkMethod.RDD_FLAT_MAP.method,
                  Expressions.lambda(
                      SparkRuntime.CalciteFlatMapFunction.class,
                      callBody,
                      e_))));
      return implementor.result(physType, builder.toBlock());
    }
  }

  // Play area

  public static void main(String[] args) {
    final JavaSparkContext sc = new JavaSparkContext("local[1]", "calcite");
    final JavaRDD<String> file = sc.textFile("/usr/share/dict/words");
    System.out.println(
        file.map(s -> s.substring(0, Math.min(s.length(), 1)))
            .distinct().count());
    file.cache();
    String s =
        file.groupBy((Function<String, String>) s1 -> s1.substring(0, Math.min(s1.length(), 1))
            //CHECKSTYLE: IGNORE 1
        ).map((Function<Tuple2<String, Iterable<String>>, Object>) pair ->
            pair._1() + ":" + Iterables.size(pair._2())).collect().toString();
    System.out.print(s);

    final JavaRDD<Integer> rdd = sc.parallelize(
        new AbstractList<Integer>() {
          final Random random = new Random();
          @Override public Integer get(int index) {
            System.out.println("get(" + index + ")");
            return random.nextInt(100);
          }

          @Override public int size() {
            System.out.println("size");
            return 10;
          }
        });
    System.out.println(
        rdd.groupBy((Function<Integer, Integer>) integer -> integer % 2).collect().toString());
    System.out.println(
        file.flatMap((FlatMapFunction<String, Pair<String, Integer>>) x -> {
          if (!x.startsWith("a")) {
            return Collections.emptyIterator();
          }
          return Collections.singletonList(
              Pair.of(x.toUpperCase(Locale.ROOT), x.length())).iterator();
        })
            .take(5)
            .toString());
  }
}

// End SparkRules.java
