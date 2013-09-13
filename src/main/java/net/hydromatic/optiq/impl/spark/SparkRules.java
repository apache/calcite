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

import net.hydromatic.optiq.rules.java.EnumerableConvention;

import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.convert.ConverterRule;
import org.eigenbase.relopt.RelOptRule;

import scala.Tuple2;

import spark.api.java.JavaRDD;
import spark.api.java.JavaSparkContext;
import spark.api.java.function.Function;

import java.util.*;

/**
 * Rules for the {@link SparkRel#CONVENTION Spark calling convention}.
 *
 * @see JdbcToSparkConverterRule
 */
public class SparkRules {
  public static RelOptRule[] RULES = {
      new EnumerableToSparkConverterRule(),
  };

  static class EnumerableToSparkConverterRule extends ConverterRule {
    public EnumerableToSparkConverterRule() {
      super(
          RelNode.class, EnumerableConvention.INSTANCE, SparkRel.CONVENTION,
          "Enumerable to Spark");
    }

    @Override
    public RelNode convert(RelNode rel) {
      throw new UnsupportedOperationException(); // TODO:
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
