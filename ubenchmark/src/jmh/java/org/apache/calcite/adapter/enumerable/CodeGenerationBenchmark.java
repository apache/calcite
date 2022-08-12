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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.tree.ClassDeclaration;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.ArrayBindable;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.runtime.Typed;
import org.apache.calcite.runtime.Utilities;
import org.apache.calcite.tools.RelBuilder;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IClassBodyEvaluator;
import org.codehaus.commons.compiler.ICompilerFactory;
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
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A benchmark of the main methods that are dynamically generating and compiling
 * Java code at runtime.
 *
 * <p>The benchmark examines the behavior of existing methods and evaluates the
 * potential of adding a caching layer on top.
 */
@Fork(value = 1, jvmArgsPrepend = "-Xmx1024m")
@Measurement(iterations = 10, time = 1)
@Warmup(iterations = 0)
@Threads(1)
@OutputTimeUnit(TimeUnit.SECONDS)
@BenchmarkMode(Mode.Throughput)
public class CodeGenerationBenchmark {

  /**
   * State holding the generated queries/plans and additional information
   * exploited by the embedded compiler in order to dynamically build a Java class.
   */
  @State(Scope.Thread)
  public static class QueryState {
    /**
     * The number of distinct queries to be generated.
     */
    @Param({"1", "10", "100", "1000"})
    int queries;

    /**
     * The number of joins for each generated query.
     */
    @Param({"1", "10", "20"})
    int joins;

    /**
     * The number of disjunctions for each generated query.
     */
    @Param({"1", "10", "100"})
    int whereClauseDisjunctions;

    /**
     * The necessary plan information for every generated query.
     */
    PlanInfo[] planInfos;

    ICompilerFactory compilerFactory;

    private int currentPlan = 0;

    @Setup(Level.Trial)
    public void setup() {
      planInfos = new PlanInfo[queries];
      VolcanoPlanner planner = new VolcanoPlanner();
      planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
      planner.addRule(CoreRules.FILTER_TO_CALC);
      planner.addRule(CoreRules.PROJECT_TO_CALC);
      planner.addRule(EnumerableRules.ENUMERABLE_CALC_RULE);
      planner.addRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
      planner.addRule(EnumerableRules.ENUMERABLE_VALUES_RULE);

      RelDataTypeFactory typeFactory =
          new JavaTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
      RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
      RelTraitSet desiredTraits =
          cluster.traitSet().replace(EnumerableConvention.INSTANCE);

      RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create(cluster, null);
      // Generates queries of the following form depending on the configuration parameters.
      // SELECT `t`.`name`
      // FROM (VALUES (1, 'Value0')) AS `t` (`id`, `name`)
      // INNER JOIN (VALUES (1, 'Value1')) AS `t` (`id`, `name`) AS `t0` ON `t`.`id` = `t0`.`id`
      // INNER JOIN (VALUES (2, 'Value2')) AS `t` (`id`, `name`) AS `t1` ON `t`.`id` = `t1`.`id`
      // INNER JOIN (VALUES (3, 'Value3')) AS `t` (`id`, `name`) AS `t2` ON `t`.`id` = `t2`.`id`
      // INNER JOIN ...
      // WHERE
      //  `t`.`name` = 'name0' OR
      //  `t`.`name` = 'name1' OR
      //  `t`.`name` = 'name2' OR
      //  ...
      //  OR `t`.`id` = 0
      // The last disjunction (i.e, t.id = $i) is what makes the queries different from one another
      // by assigning a different constant literal.
      for (int i = 0; i < queries; i++) {
        relBuilder.values(new String[]{"id", "name"}, 1, "Value" + 0);
        for (int j = 1; j <= joins; j++) {
          relBuilder
              .values(new String[]{"id", "name"}, j, "Value" + j)
              .join(JoinRelType.INNER, "id");
        }

        List<RexNode> disjunctions = new ArrayList<>();
        for (int j = 0; j < whereClauseDisjunctions; j++) {
          disjunctions.add(
              relBuilder.equals(
                  relBuilder.field("name"),
                  relBuilder.literal("name" + j)));
        }
        disjunctions.add(
            relBuilder.equals(
                relBuilder.field("id"),
                relBuilder.literal(i)));
        RelNode query =
            relBuilder
                .filter(relBuilder.or(disjunctions))
                .project(relBuilder.field("name"))
                .build();

        RelNode query0 = planner.changeTraits(query, desiredTraits);
        planner.setRoot(query0);

        PlanInfo info = new PlanInfo();
        EnumerableRel plan = (EnumerableRel) planner.findBestExp();

        EnumerableRelImplementor relImplementor =
            new EnumerableRelImplementor(plan.getCluster().getRexBuilder(), new HashMap<>());
        info.classExpr = relImplementor.implementRoot(plan, EnumerableRel.Prefer.ARRAY);
        info.javaCode =
            Expressions.toString(info.classExpr.memberDeclarations, "\n", false);
        info.plan = plan;
        planInfos[i] = info;
      }

      try {
        compilerFactory = CompilerFactoryFactory.getDefaultCompilerFactory(
            CodeGenerationBenchmark.class.getClassLoader());
      } catch (Exception e) {
        throw new IllegalStateException(
            "Unable to instantiate java compiler", e);
      }

    }

    Bindable compile(EnumerableRel plan, String className, String code)
        throws CompileException, IOException {
      final StringReader stringReader = new StringReader(code);
      IClassBodyEvaluator cbe = compilerFactory.newClassBodyEvaluator();
      cbe.setClassName(className);
      cbe.setExtendedClass(Utilities.class);
      cbe.setImplementedInterfaces(
          plan.getRowType().getFieldCount() == 1
              ? new Class[]{Bindable.class, Typed.class}
              : new Class[]{ArrayBindable.class});
      cbe.setParentClassLoader(EnumerableInterpretable.class.getClassLoader());
      return (Bindable) cbe.createInstance(stringReader);
    }

    int nextPlan() {
      int ret = currentPlan;
      currentPlan = (currentPlan + 1) % queries;
      return ret;
    }
  }

  /** Plan information. */
  private static class PlanInfo {
    ClassDeclaration classExpr;
    EnumerableRel plan;
    String javaCode;
  }

  /**
   * State holding a cache that is initialized
   * once at the beginning of each iteration.
   */
  @State(Scope.Thread)
  public static class CacheState {
    @Param({"10", "100", "1000"})
    int cacheSize;

    Cache<String, Bindable> cache;

    @Setup(Level.Iteration)
    public void setup() {
      cache = CacheBuilder.newBuilder().maximumSize(cacheSize).concurrencyLevel(1).build();
    }

  }


  /**
   * Benchmarks the part creating Bindable instances from
   * {@link EnumerableInterpretable#getBindable(ClassDeclaration, String, int)}
   * method without any additional caching layer.
   */
  @Benchmark
  public Bindable<?> getBindableNoCache(QueryState state) throws Exception {
    PlanInfo info = state.planInfos[state.nextPlan()];
    return state.compile(info.plan, info.classExpr.name, info.javaCode);
  }

  /**
   * Benchmarks the part of creating Bindable instances from
   * {@link EnumerableInterpretable#getBindable(ClassDeclaration, String, int)}
   * method with an additional cache layer.
   */
  @Benchmark
  public Bindable<?> getBindableWithCache(
      QueryState jState,
      CacheState chState) throws Exception {
    PlanInfo info = jState.planInfos[jState.nextPlan()];
    Cache<String, Bindable> cache = chState.cache;

    EnumerableInterpretable.StaticFieldDetector detector =
        new EnumerableInterpretable.StaticFieldDetector();
    info.classExpr.accept(detector);
    if (!detector.containsStaticField) {
      return cache.get(
          info.javaCode,
          () -> jState.compile(info.plan, info.classExpr.name, info.javaCode));
    }
    throw new IllegalStateException("Benchmark queries should not arrive here");
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(CodeGenerationBenchmark.class.getName())
        .addProfiler(GCProfiler.class)
        .detectJvmArgs()
        .build();

    new Runner(opt).run();
  }
}
