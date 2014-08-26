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
package net.hydromatic.optiq.impl.spark;

import net.hydromatic.linq4j.expressions.*;

import net.hydromatic.optiq.BuiltinMethod;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.rules.java.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.ConverterRule;
import org.eigenbase.rel.metadata.RelMetadataQuery;
import org.eigenbase.rel.rules.FilterToCalcRule;
import org.eigenbase.rel.rules.ProjectToCalcRule;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.rex.RexMultisetUtil;
import org.eigenbase.rex.RexProgram;
import org.eigenbase.util.Pair;

import com.google.common.collect.ImmutableList;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.lang.reflect.Type;
import java.util.*;

import scala.Tuple2;

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

    @Override
    public RelNode convert(RelNode rel) {
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

    @Override
    public RelNode convert(RelNode rel) {
      return new SparkToEnumerableConverter(rel.getCluster(),
          rel.getTraitSet().replace(EnumerableConvention.INSTANCE), rel);
    }
  }

  public static final SparkValuesRule SPARK_VALUES_RULE =
      new SparkValuesRule();

  /** Planner rule that implements VALUES operator in Spark convention. */
  public static class SparkValuesRule extends ConverterRule {
    private SparkValuesRule() {
      super(ValuesRel.class, Convention.NONE, SparkRel.CONVENTION,
          "SparkValuesRule");
    }

    @Override
    public RelNode convert(RelNode rel) {
      ValuesRel valuesRel = (ValuesRel) rel;
      return new SparkValuesRel(
          valuesRel.getCluster(),
          valuesRel.getRowType(),
          valuesRel.getTuples(),
          valuesRel.getTraitSet().replace(getOutTrait()));
    }
  }

  /** VALUES construct implemented in Spark. */
  public static class SparkValuesRel
      extends ValuesRelBase
      implements SparkRel {
    SparkValuesRel(
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
      return new SparkValuesRel(
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
   * Rule to convert a {@link CalcRel} to an
   * {@link net.hydromatic.optiq.impl.spark.SparkRules.SparkCalcRel}.
   */
  private static class SparkCalcRule
      extends ConverterRule {
    private SparkCalcRule() {
      super(
          CalcRel.class,
          Convention.NONE,
          SparkRel.CONVENTION,
          "SparkCalcRule");
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

      return new SparkCalcRel(
          rel.getCluster(),
          rel.getTraitSet().replace(SparkRel.CONVENTION),
          convert(calc.getChild(),
              calc.getChild().getTraitSet().replace(SparkRel.CONVENTION)),
          program,
          ProjectRelBase.Flags.BOXED);
    }
  }

  /** Implementation of {@link CalcRel} in Spark convention. */
  public static class SparkCalcRel
      extends SingleRel
      implements SparkRel {
    private final RexProgram program;

    /**
     * Values defined in {@link org.eigenbase.rel.ProjectRelBase.Flags}.
     */
    protected int flags;

    public SparkCalcRel(RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode child,
        RexProgram program,
        int flags) {
      super(cluster, traitSet, child);
      assert getConvention() == SparkRel.CONVENTION;
      assert !program.containsAggs();
      this.flags = flags;
      this.program = program;
      this.rowType = program.getOutputRowType();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
      return program.explainCalc(super.explainTerms(pw));
    }

    public double getRows() {
      return FilterRel.estimateFilteredRows(getChild(), program);
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner) {
      double dRows = RelMetadataQuery.getRowCount(this);
      double dCpu =
          RelMetadataQuery.getRowCount(getChild())
              * program.getExprCount();
      double dIo = 0;
      return planner.getCostFactory().makeCost(dRows, dCpu, dIo);
    }

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new SparkCalcRel(
          getCluster(),
          traitSet,
          sole(inputs),
          program,
          getFlags());
    }

    public int getFlags() {
      return flags;
    }

    public Result implementSpark(Implementor implementor) {
      final JavaTypeFactory typeFactory = implementor.getTypeFactory();
      final BlockBuilder builder = new BlockBuilder();
      final SparkRel child = (SparkRel) getChild();

      final Result result = implementor.visitInput(this, 0, child);

      final PhysType physType =
          PhysTypeImpl.of(
              typeFactory, getRowType(), JavaRowFormat.CUSTOM);

      // final RDD<Employee> inputRdd = <<child impl>>;
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
                        Pair.of((Expression) e_, result.physType))));
        builder2.add(
            Expressions.ifThen(
                Expressions.not(condition),
                Expressions.return_(null,
                    Expressions.call(
                        BuiltinMethod.COLLECTIONS_EMPTY_LIST.method))));
      }

      List<Expression> expressions =
          RexToLixTranslator.translateProjects(
              program,
              typeFactory,
              builder2,
              null,
              new RexToLixTranslator.InputGetterImpl(
                  Collections.singletonList(
                      Pair.of((Expression) e_, result.physType))));
      builder2.add(
          Expressions.return_(null,
              Expressions.convert_(
                  Expressions.call(
                      BuiltinMethod.COLLECTIONS_SINGLETON_LIST.method,
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
                      SparkRuntime.OptiqFlatMapFunction.class,
                      callBody,
                      e_))));
      return implementor.result(physType, builder.toBlock());
    }
  }

  // Play area

  public static void main(String[] args) {
    final JavaSparkContext sc = new JavaSparkContext("local[1]", "optiq");
    final JavaRDD<String> file = sc.textFile("/usr/share/dict/words");
    System.out.println(
        file.map(
            new Function<String, Object>() {
              @Override
              public Object call(String s) throws Exception {
                return s.substring(0, Math.min(s.length(), 1));
              }
            }).distinct().count());
    file.cache();
    String s =
        file.groupBy(
            new Function<String, String>() {
              @Override
              public String call(String s) throws Exception {
                return s.substring(0, Math.min(s.length(), 1));
              }
            }
            //CHECKSTYLE: IGNORE 1
        ).map(
            new Function<Tuple2<String, List<String>>, Object>() {
              @Override
              public Object call(Tuple2<String, List<String>> pair) {
                return pair._1() + ":" + pair._2().size();
              }
            }).collect().toString();
    System.out.print(s);

    final JavaRDD<Integer> rdd = sc.parallelize(
        new AbstractList<Integer>() {
          final Random random = new Random();
          @Override
          public Integer get(int index) {
            System.out.println("get(" + index + ")");
            return random.nextInt(100);
          }

          @Override
          public int size() {
            System.out.println("size");
            return 10;
          }
        });
    System.out.println(
        rdd.groupBy(
            new Function<Integer, Integer>() {
              public Integer call(Integer integer) {
                return integer % 2;
              }
            }).collect().toString());
    System.out.println(
        file.flatMap(
            new FlatMapFunction<String, Pair<String, Integer>>() {
              public List<Pair<String, Integer>> call(String x) {
                if (!x.startsWith("a")) {
                  return Collections.emptyList();
                }
                return Collections.singletonList(
                    Pair.of(x.toUpperCase(), x.length()));
              }
            })
            .take(5)
            .toString());
  }
}

// End SparkRules.java
