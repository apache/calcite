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

import java.util.concurrent.TimeUnit;

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmarks JavaCC-generated SqlBabelParserImpl and ImmutableSqlParser instantiation.
 * The instantiation time of parsers should not depend on the call stack depth.
 * See https://lists.apache.org/thread/xw35sdy1w1k8lvn1q1lr7xb93bkj0lpq
 */
@Fork(value = 1, jvmArgsPrepend = "-Xmx128m")
@Measurement(iterations = 7, time = 1, timeUnit = TimeUnit.SECONDS)
@Warmup(iterations = 7, time = 1, timeUnit = TimeUnit.SECONDS)
@State(Scope.Thread)
@Threads(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class ParserInstantiationBenchmark {

  int callStackDepth;
  SqlParser.Config babelConfig;
  SqlParser.Config coreConfig;
  String sqlExpression;

  @Setup
  public void setup() throws SqlParseException {
    babelConfig = SqlParser.config().withParserFactory(SqlBabelParserImpl.FACTORY);
    coreConfig = SqlParser.config();
    // not important in this benchmark, see ParserBenchmark for varying string length
    sqlExpression = "SELECT 1";
    callStackDepth = 100;
  }

  @Benchmark
  public void instantiateBabelParser(Blackhole bh) {
    bh.consume(SqlParser.create(sqlExpression, babelConfig));
  }

  @Benchmark
  public void instantiateBabelParserInDeepCallStack(Blackhole bh) {
    recursiveCall(
        () -> bh.consume(SqlParser.create(sqlExpression, babelConfig)),
        callStackDepth
    );
  }

  @Benchmark
  public void instantiateCoreParser(Blackhole bh) {
    bh.consume(SqlParser.create(sqlExpression, coreConfig));
  }

  @Benchmark
  public void instantiateCoreParserInDeepCallStack(Blackhole bh) {
    recursiveCall(
        () -> bh.consume(SqlParser.create(sqlExpression, babelConfig)),
        callStackDepth
    );
  }

  // used to increase the stack depth
  private void recursiveCall(final Runnable r, final int callStackSize) {
    if (callStackSize == 0) {
      r.run();
      return;
    }
    recursiveCall(r, callStackSize - 1);
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder().include(ParserInstantiationBenchmark.class.getSimpleName())
        .detectJvmArgs()
        .build();

    new Runner(opt).run();
  }
}
