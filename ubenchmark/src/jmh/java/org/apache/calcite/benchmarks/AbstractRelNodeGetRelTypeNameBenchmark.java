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
package org.apache.calcite.benchmarks;

import org.apache.calcite.rel.AbstractRelNode;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * A benchmark of alternative implementations for {@link AbstractRelNode#getRelTypeName()}
 * method.
 */
@Fork(value = 1, jvmArgsPrepend = "-Xmx1024m")
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Threads(1)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class AbstractRelNodeGetRelTypeNameBenchmark {

  /**
   * A state holding the full class names of all built-in implementors of the
   * {@link org.apache.calcite.rel.RelNode} interface.
   */
  @State(Scope.Thread)
  public static class ClassNameState {

    private final String[] fullNames = new String[]{
        "org.apache.calcite.interpreter.InterpretableRel",
        "org.apache.calcite.interpreter.BindableRel",
        "org.apache.calcite.adapter.enumerable.EnumerableInterpretable",
        "org.apache.calcite.adapter.enumerable.EnumerableRel",
        "org.apache.calcite.adapter.enumerable.EnumerableLimit",
        "org.apache.calcite.adapter.enumerable.EnumerableUnion",
        "org.apache.calcite.adapter.enumerable.EnumerableCollect",
        "org.apache.calcite.adapter.enumerable.EnumerableTableFunctionScan",
        "org.apache.calcite.adapter.enumerable.EnumerableValues",
        "org.apache.calcite.adapter.enumerable.EnumerableSemiJoin",
        "org.apache.calcite.adapter.enumerable.EnumerableMinus",
        "org.apache.calcite.adapter.enumerable.EnumerableIntersect",
        "org.apache.calcite.adapter.enumerable.EnumerableUncollect",
        "org.apache.calcite.adapter.enumerable.EnumerableMergeJoin",
        "org.apache.calcite.adapter.enumerable.EnumerableProject",
        "org.apache.calcite.adapter.enumerable.EnumerableFilter",
        "org.apache.calcite.adapter.jdbc.JdbcToEnumerableConverter",
        "org.apache.calcite.adapter.enumerable.EnumerableNestedLoopJoin",
        "org.apache.calcite.adapter.enumerable.EnumerableTableScan",
        "org.apache.calcite.adapter.enumerable.EnumerableHashJoin",
        "org.apache.calcite.adapter.enumerable.EnumerableTableModify",
        "org.apache.calcite.adapter.enumerable.EnumerableAggregate",
        "org.apache.calcite.adapter.enumerable.EnumerableCorrelate",
        "org.apache.calcite.adapter.enumerable.EnumerableSort",
        "org.apache.calcite.adapter.enumerable.EnumerableWindow",
        "org.apache.calcite.plan.volcano.VolcanoPlannerTraitTest$FooRel",
        "org.apache.calcite.adapter.enumerable.EnumerableCalc",
        "org.apache.calcite.adapter.enumerable.EnumerableInterpreter",
        "org.apache.calcite.adapter.geode.rel.GeodeToEnumerableConverter",
        "org.apache.calcite.adapter.pig.PigToEnumerableConverter",
        "org.apache.calcite.adapter.mongodb.MongoToEnumerableConverter",
        "org.apache.calcite.adapter.csv.CsvTableScan",
        "org.apache.calcite.adapter.spark.SparkToEnumerableConverter",
        "org.apache.calcite.adapter.elasticsearch.ElasticsearchToEnumerableConverter",
        "org.apache.calcite.adapter.file.FileTableScan",
        "org.apache.calcite.adapter.cassandra.CassandraToEnumerableConverter",
        "org.apache.calcite.adapter.splunk.SplunkTableScan",
        "org.apache.calcite.adapter.jdbc.JdbcRel",
        "org.apache.calcite.adapter.jdbc.JdbcTableScan",
        "org.apache.calcite.adapter.jdbc.JdbcRules$JdbcJoin",
        "org.apache.calcite.adapter.jdbc.JdbcRules$JdbcCalc",
        "org.apache.calcite.adapter.jdbc.JdbcRules$JdbcProject",
        "org.apache.calcite.adapter.jdbc.JdbcRules$JdbcFilter",
        "org.apache.calcite.adapter.jdbc.JdbcRules$JdbcAggregate",
        "org.apache.calcite.adapter.jdbc.JdbcRules$JdbcSort",
        "org.apache.calcite.adapter.jdbc.JdbcRules$JdbcUnion",
        "org.apache.calcite.adapter.jdbc.JdbcRules$JdbcIntersect",
        "org.apache.calcite.adapter.jdbc.JdbcRules$JdbcMinus",
        "org.apache.calcite.adapter.jdbc.JdbcRules$JdbcTableModify",
        "org.apache.calcite.adapter.jdbc.JdbcRules$JdbcValues",
        "org.apache.calcite.tools.PlannerTest$MockJdbcTableScan",
        "org.apache.calcite.rel.AbstractRelNode",
        "org.apache.calcite.rel.rules.MultiJoin",
        "org.apache.calcite.rel.core.TableFunctionScan",
        "org.apache.calcite.rel.BiRel",
        "org.apache.calcite.rel.SingleRel",
        "org.apache.calcite.rel.core.Values",
        "org.apache.calcite.rel.core.TableScan",
        "org.apache.calcite.plan.hep.HepRelVertex",
        "org.apache.calcite.plan.RelOptPlanReaderTest$MyRel",
        "org.apache.calcite.plan.volcano.TraitPropagationTest$PhysTable",
        "org.apache.calcite.plan.volcano.PlannerTests$TestLeafRel",
        "org.apache.calcite.plan.volcano.RelSubset",
        "org.apache.calcite.rel.core.SetOp",
        "org.apache.calcite.plan.volcano.VolcanoPlannerTraitTest$TestLeafRel",
        "org.apache.calcite.adapter.druid.DruidQuery",
        "org.apache.calcite.sql2rel.RelStructuredTypeFlattener$SelfFlatteningRel",
        "org.apache.calcite.rel.convert.Converter",
        "org.apache.calcite.rel.convert.ConverterImpl",
        "org.apache.calcite.plan.volcano.TraitPropagationTest$Phys",
        "org.apache.calcite.plan.volcano.TraitPropagationTest$PhysTable",
        "org.apache.calcite.plan.volcano.TraitPropagationTest$PhysSort",
        "org.apache.calcite.plan.volcano.TraitPropagationTest$PhysAgg",
        "org.apache.calcite.plan.volcano.TraitPropagationTest$PhysProj",
        "org.apache.calcite.interpreter.BindableRel",
        "org.apache.calcite.adapter.enumerable.EnumerableBindable",
        "org.apache.calcite.interpreter.Bindables$BindableTableScan",
        "org.apache.calcite.interpreter.Bindables$BindableFilter",
        "org.apache.calcite.interpreter.Bindables$BindableProject",
        "org.apache.calcite.interpreter.Bindables$BindableSort",
        "org.apache.calcite.interpreter.Bindables$BindableJoin",
        "org.apache.calcite.interpreter.Bindables$BindableUnion",
        "org.apache.calcite.interpreter.Bindables$BindableValues",
        "org.apache.calcite.interpreter.Bindables$BindableAggregate",
        "org.apache.calcite.interpreter.Bindables$BindableWindow",
        "org.apache.calcite.adapter.druid.DruidQuery",
        "org.apache.calcite.adapter.cassandra.CassandraRel",
        "org.apache.calcite.adapter.cassandra.CassandraFilter",
        "org.apache.calcite.adapter.cassandra.CassandraProject",
        "org.apache.calcite.adapter.cassandra.CassandraLimit",
        "org.apache.calcite.adapter.cassandra.CassandraSort",
        "org.apache.calcite.adapter.cassandra.CassandraTableScan",
        "org.apache.calcite.adapter.mongodb.MongoRel",
        "org.apache.calcite.adapter.mongodb.MongoTableScan",
        "org.apache.calcite.adapter.mongodb.MongoProject",
        "org.apache.calcite.adapter.mongodb.MongoFilter",
        "org.apache.calcite.adapter.mongodb.MongoAggregate",
        "org.apache.calcite.adapter.mongodb.MongoSort",
        "org.apache.calcite.adapter.spark.SparkRel",
        "org.apache.calcite.adapter.spark.JdbcToSparkConverter",
        "org.apache.calcite.adapter.spark.SparkRules$SparkValues",
        "org.apache.calcite.adapter.spark.EnumerableToSparkConverter",
        "org.apache.calcite.adapter.spark.SparkRules$SparkCalc",
        "org.apache.calcite.adapter.elasticsearch.ElasticsearchRel",
        "org.apache.calcite.adapter.elasticsearch.ElasticsearchFilter",
        "org.apache.calcite.adapter.elasticsearch.ElasticsearchProject",
        "org.apache.calcite.adapter.elasticsearch.ElasticsearchAggregate",
        "org.apache.calcite.adapter.elasticsearch.ElasticsearchTableScan",
        "org.apache.calcite.adapter.elasticsearch.ElasticsearchSort",
        "org.apache.calcite.adapter.geode.rel.GeodeRel",
        "org.apache.calcite.adapter.geode.rel.GeodeSort",
        "org.apache.calcite.adapter.geode.rel.GeodeTableScan",
        "org.apache.calcite.adapter.geode.rel.GeodeProject",
        "org.apache.calcite.adapter.geode.rel.GeodeFilter",
        "org.apache.calcite.adapter.geode.rel.GeodeAggregate",
        "org.apache.calcite.adapter.pig.PigRel",
        "org.apache.calcite.adapter.pig.PigTableScan",
        "org.apache.calcite.adapter.pig.PigAggregate",
        "org.apache.calcite.adapter.pig.PigJoin",
        "org.apache.calcite.adapter.pig.PigFilter",
        "org.apache.calcite.adapter.pig.PigProject"
    };

    @Param({"11", "31", "63"})
    private long seed;

    private Random r = null;

    /**
     * Sets up the random number generator at the beginning of each iteration.
     *
     * <p>To have relatively comparable results the generator should always use
     * the same seed for the whole duration of the benchmark.
     */
    @Setup(Level.Iteration)
    public void setupRandom() {
      r = new Random(seed);
    }

    /**
     * Returns a pseudo-random class name that corresponds to an implementor of the RelNode
     * interface.
     */
    public String nextName() {
      return fullNames[r.nextInt(fullNames.length)];
    }
  }

  @Benchmark
  public String useStringLastIndexOfTwoTimesV1(ClassNameState state) {
    String cn = state.nextName();
    int i = cn.lastIndexOf("$");
    if (i >= 0) {
      return cn.substring(i + 1);
    }
    i = cn.lastIndexOf(".");
    if (i >= 0) {
      return cn.substring(i + 1);
    }
    return cn;
  }

  @Benchmark
  public String useStringLastIndexOfTwoTimeV2(ClassNameState state) {
    String cn = state.nextName();
    int i = cn.lastIndexOf('$');
    if (i >= 0) {
      return cn.substring(i + 1);
    }
    i = cn.lastIndexOf('.');
    if (i >= 0) {
      return cn.substring(i + 1);
    }
    return cn;
  }

  @Benchmark
  public String useCustomLastIndexOf(ClassNameState state) {
    String cn = state.nextName();
    int i = cn.length();
    while (--i >= 0) {
      if (cn.charAt(i) == '$' || cn.charAt(i) == '.') {
        return cn.substring(i + 1);
      }
    }
    return cn;
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(AbstractRelNodeGetRelTypeNameBenchmark.class.getName())
        .detectJvmArgs()
        .build();

    new Runner(opt).run();
  }
}

// End AbstractRelNodeGetRelTypeNameBenchmark.java
