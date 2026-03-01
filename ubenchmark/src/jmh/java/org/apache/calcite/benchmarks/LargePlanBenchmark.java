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

import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

import com.google.common.collect.ImmutableList;

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

import java.util.concurrent.TimeUnit;

/**
 * Benchmark that constructs a synthetic query plan consisting of a large plan.
 *
 * <p>This benchmark primarily measures planner overhead under heavy rule activity: matching and
 * firing rules, replacing RelNodes, and traversing the evolving plan during optimization.
 * It also simulates a multiphase optimization flow by running multiple HepPrograms sequentially.
 *
 * <p>Each UNION input branch contains layers of Projects and Filters that are intentionally
 *  simplifiable, so that typical planner rules (e.g., Project/Filter simplification)
 *  are repeatedly applicable.
 */

@Fork(value = 1, jvmArgsPrepend = {"-Xss200m"})
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Threads(1)
public class LargePlanBenchmark {

  @Param({"100", "1000", "5000", "10000"})
  int unionNum;

  private RelBuilder builder;

  @Setup(Level.Trial)
  public void setup() {
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    rootSchema.add("EMP", new AbstractTable() {
      @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
            .add("EMPNO", SqlTypeName.INTEGER)
            .add("ENAME", SqlTypeName.VARCHAR)
            .add("JOB", SqlTypeName.VARCHAR)
            .add("MGR", SqlTypeName.INTEGER)
            .add("HIREDATE", SqlTypeName.DATE)
            .add("SAL", SqlTypeName.INTEGER)
            .add("COMM", SqlTypeName.INTEGER)
            .add("DEPTNO", SqlTypeName.INTEGER)
            .build();
      }
    });

    builder =
        RelBuilder.create(Frameworks.newConfigBuilder()
            .defaultSchema(rootSchema)
            .build());
  }

  // select ENAME, i as cat_id, 'i' as cat_name, i as require_free_postage,
  // 0 as require_15return, 0 as require_48hour, 1 as require_insurance
  // from emp
  // where EMPNO = i and MGR >= 0 and MGR <= 0 and ENAME = 'Y' and SAL = i
  private RelNode makeSelectBranch(int i) {
    return builder.scan("EMP")
        .filter(
            builder.and(
                builder.equals(builder.field("EMPNO"), builder.literal(i)),
                builder.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                    builder.field("MGR"), builder.literal(0)),
                builder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                    builder.field("MGR"), builder.literal(0)),
                builder.equals(builder.field("ENAME"), builder.literal("Y")),
                builder.equals(builder.field("SAL"), builder.literal(i))
            )
        )
        .project(
            builder.field("ENAME"),
            builder.alias(builder.literal(i), "cat_id"),
            builder.alias(builder.literal(String.valueOf(i)), "cat_name"),
            builder.alias(builder.literal(i), "require_free_postage"),
            builder.alias(builder.literal(0), "require_15return"),
            builder.alias(builder.literal(0), "require_48hour"),
            builder.alias(builder.literal(1), "require_insurance")
        )
        .build();
  }

  private RelNode makeUnionTree(int unionNum) {
    RelNode union = makeSelectBranch(0);
    for (int i = 1; i < unionNum; i++) {
      RelNode right = makeSelectBranch(i);
      union = LogicalUnion.create(ImmutableList.of(union, right), true);
    }
    union = LogicalUnion.create(ImmutableList.of(union, makeSelectBranch(unionNum)), true);
    return union;
  }

  @Benchmark
  public void testLargeUnionPlan() {
    RelNode root = makeUnionTree(unionNum);

    HepProgram filterReduce = HepProgram.builder()
        .addMatchOrder(HepMatchOrder.DEPTH_FIRST)
        .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .build();

    HepProgram projectReduce = HepProgram.builder()
        .addMatchOrder(HepMatchOrder.DEPTH_FIRST)
        .addRuleInstance(CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .build();

    // Phrase 1
    HepPlanner planner = new HepPlanner(filterReduce);
    planner.setRoot(root);
    root = planner.findBestExp();
    planner.clear();

    // ... do some things cannot be done in planner.findBestExp() ...
    // Phrase 2
    planner = new HepPlanner(projectReduce);
    planner.setRoot(root);
    root = planner.findBestExp();
    planner.clear();

    // TODO LATER large plan optimization
    // TODO LATER set "-Dcalcite.disable.generate.type.digest.string=true"
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(LargePlanBenchmark.class.getSimpleName())
        .detectJvmArgs()
        .build();

    new Runner(opt).run();
  }
}
