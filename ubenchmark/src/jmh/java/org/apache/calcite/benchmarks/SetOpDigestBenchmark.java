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

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks {@code HepPlanner}'s digest-based deduplication for
 * {@code UNION}/{@code INTERSECT}/{@code MINUS} trees. The rule program is
 * empty, so no rules run. The benchmark includes {@link HepPlanner}'s own
 * graph/digest-map bookkeeping on every node insert: a real
 * {@code deepHashCode} and, on a digest-map hit, a real {@code deepEquals}.
 */
@Fork(value = 1, jvmArgsPrepend = {"-Xss200m"})
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Threads(1)
public class SetOpDigestBenchmark {

  private static final HepProgram EMPTY_PROGRAM = HepProgram.builder().build();

  @Param({"UNION", "INTERSECT", "MINUS"})
  String setOp;

  /** Number of {@code SetOp} nodes in the tree. */
  @Param({"2", "5", "10", "100", "1000", "10000"})
  int nodeCount;

  /**
   * 0 = every branch is unique, so every digest lookup misses. 100 = every
   * branch duplicates one of a handful of templates, so lookups hit and
   * {@code deepEquals} runs.
   */
  @Param({"0", "50", "100"})
  int duplicatePercent;

  private RelBuilder builder;
  private List<RelOptTable> templateTables;
  private RelNode root;

  @Setup(Level.Trial)
  public void setupCluster() {
    int templateCount = templateCount();
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    for (int i = 0; i < templateCount; i++) {
      rootSchema.add("EMP" + i, new AbstractTable() {
        @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
          return typeFactory.builder()
              .add("EMPNO", SqlTypeName.INTEGER)
              .build();
        }
      });
    }

    builder =
        RelBuilder.create(Frameworks.newConfigBuilder()
            .defaultSchema(rootSchema)
            .build());

    templateTables = new ArrayList<>(templateCount);
    for (int i = 0; i < templateCount; i++) {
      templateTables.add(
          builder.getRelOptSchema().getTableForMember(ImmutableList.of("EMP" + i)));
    }
  }

  private int templateCount() {
    int totalBranches = nodeCount + 1;
    return Math.max(1, totalBranches * (100 - duplicatePercent) / 100);
  }

  private RelNode makeBranch(int templateIndex) {
    return LogicalTableScan.create(
        builder.getCluster(), templateTables.get(templateIndex), ImmutableList.of());
  }

  private RelNode combine(RelNode left, RelNode right) {
    List<RelNode> inputs = ImmutableList.of(left, right);
    switch (setOp) {
    case "INTERSECT":
      return LogicalIntersect.create(inputs, true);
    case "MINUS":
      return LogicalMinus.create(inputs, true);
    default:
      return LogicalUnion.create(inputs, true);
    }
  }

  /** Combines nodes pairwise into a balanced binary tree. */
  private RelNode balancedReduce(List<RelNode> nodes) {
    if (nodes.size() == 1) {
      return nodes.get(0);
    }
    List<RelNode> next = new ArrayList<>((nodes.size() + 1) / 2);
    int i = 0;
    for (; i + 1 < nodes.size(); i += 2) {
      next.add(combine(nodes.get(i), nodes.get(i + 1)));
    }
    if (i < nodes.size()) {
      // Odd one out: carried up a level unchanged.
      next.add(nodes.get(i));
    }
    return balancedReduce(next);
  }

  /** Builds a fresh tree; {@code duplicatePercent} controls the branch templates used. */
  private RelNode newTree() {
    int totalBranches = nodeCount + 1;
    int templateCount = templateCount();
    List<RelNode> branches = new ArrayList<>(totalBranches);
    for (int i = 0; i < totalBranches; i++) {
      branches.add(makeBranch(i % templateCount));
    }
    return balancedReduce(branches);
  }

  /** Builds a fresh tree so {@link #dedupOnInsert} always sees no cached digest hash. */
  @Setup(Level.Invocation)
  public void prepareInvocation() {
    root = newTree();
  }

  @Benchmark
  public RelNode dedupOnInsert() {
    HepPlanner planner = new HepPlanner(EMPTY_PROGRAM);
    planner.setLargePlanMode(true);
    planner.setRoot(root);
    return planner.getRoot();
  }
}
