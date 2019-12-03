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

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
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

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks JavaCC-generated SQL parser
 */
@Fork(value = 1, jvmArgsPrepend = "-Xmx128m")
@Measurement(iterations = 7, time = 1, timeUnit = TimeUnit.SECONDS)
@Warmup(iterations = 7, time = 1, timeUnit = TimeUnit.SECONDS)
@State(Scope.Thread)
@Threads(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class ParserBenchmark {

  @Param({ "1000" })
  int length;

  @Param({ "true" })
  boolean comments;

  String sql;
  SqlParser parser;

  @Setup
  public void setup() throws SqlParseException {
    StringBuilder sb = new StringBuilder((int) (length * 1.2));
    sb.append("select 1");
    Random rnd = new Random();
    rnd.setSeed(424242);
    for (; sb.length() < length;) {
      for (int i = 0; i < 7 && sb.length() < length; i++) {
        sb.append(", ");
        switch (rnd.nextInt(3)) {
        case 0:
          sb.append("?");
          break;
        case 1:
          sb.append(rnd.nextInt());
          break;
        case 2:
          sb.append('\'').append(rnd.nextLong()).append(rnd.nextLong())
              .append('\'');
          break;
        }
      }
      if (comments && sb.length() < length) {
        sb.append("// sb.append('\\'').append(rnd.nextLong()).append(rnd.nextLong()).append(rnd"
            + ".nextLong())");
      }
      sb.append('\n');
    }
    sb.append(" from dual");
    parser = SqlParser.create("values(1)");
    sql = sb.toString();
  }

  @Benchmark
  public SqlNode parseCached() throws SqlParseException {
    return parser.parseQuery(sql);
  }

  @Benchmark
  public SqlNode parseNonCached() throws SqlParseException {
    return SqlParser.create(sql).parseQuery();
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(ParserBenchmark.class.getSimpleName())
        .addProfiler(GCProfiler.class)
        .addProfiler(FlightRecorderProfiler.class)
        .detectJvmArgs()
        .build();

    new Runner(opt).run();
  }

}

// End ParserBenchmark.java
