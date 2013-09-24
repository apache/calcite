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
package net.hydromatic.optiq.impl.spark;

import net.hydromatic.linq4j.expressions.*;

import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.rules.java.*;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.ConverterRule;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.util.Pair;

import scala.Tuple2;

import com.google.common.collect.ImmutableList;

import java.lang.reflect.Type;
import java.util.*;

/**
 * Rules for the {@link SparkRel#CONVENTION Spark calling convention}.
 *
 * @see JdbcToSparkConverterRule
 */
public class SparkRules {
  public static List<RelOptRule> rules() {
    return ImmutableList.<RelOptRule>of(
        EnumerableToSparkConverterRule.INSTANCE,
        SparkToEnumerableConverterRule.INSTANCE,
        SPARK_VALUES_RULE);
  }

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
                      implementor.root),
                  Expressions.newArrayInit(Primitive.box(rowClass),
                      expressions))));
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
  }
}

// End SparkRules.java
