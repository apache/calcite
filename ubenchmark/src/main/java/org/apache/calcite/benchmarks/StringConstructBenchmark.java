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
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.concurrent.TimeUnit;

/**
 * A benchmark of the most common patterns that are used to construct gradually
 * String objects.
 *
 * <p>The benchmark emphasizes on the build patterns that appear in the Calcite
 * project.
 */
@Fork(value = 1, jvmArgsPrepend = "-Xmx2048m")
@Measurement(iterations = 10, time = 100, timeUnit = TimeUnit.MILLISECONDS)
@Warmup(iterations = 10, time = 100, timeUnit = TimeUnit.MILLISECONDS)
@Threads(1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.Throughput)
public class StringConstructBenchmark {

  /**
   * A state holding a Writer object which is initialized only once at the beginning of the
   * benchmark.
   */
  @State(Scope.Thread)
  public static class WriterState {
    public Writer writer;

    @Setup(Level.Trial)
    public void setup() {
      this.writer = new StringWriter();
    }
  }

  /**
   * A state holding an Appendable object which is initialized after a fixed number of append
   * operations.
   */
  @State(Scope.Thread)
  public static class AppenderState {
    /**
     * The type of the appender to be initialised.
     */
    @Param({"StringBuilder", "StringWriter", "PrintWriter"})
    public String appenderType;

    /**
     * The maximum number of appends before resetting the appender.
     *
     * <p>If the value is small then the appender is reinitialized very often,
     * making the instantiation of the appender the dominant operation of the
     * benchmark.
     */
    @Param({"1", "256", "512", "1024"})
    public int maxAppends;

    /**
     * The appender that is currently used.
     */
    private Appendable appender;

    /**
     * The number of append operations performed so far.
     */
    private int nAppends = 0;

    @Setup(Level.Iteration)
    public void setup() {
      reset();
    }

    private void reset() {
      nAppends = 0;
      if (appenderType.equals("StringBuilder")) {
        this.appender = new StringBuilder();
      } else if (appenderType.equals("StringWriter")) {
        this.appender = new StringWriter();
      } else if (appenderType.equals("PrintWriter")) {
        this.appender = new PrintWriter(new StringWriter());
      } else {
        throw new IllegalStateException(
            "The specified appender type (" + appenderType + ") is not supported.");
      }
    }

    Appendable getOrCreateAppender() {
      if (nAppends >= maxAppends) {
        reset();
      }
      nAppends++;
      return appender;
    }

  }

  @Benchmark
  public StringBuilder initStringBuilder() {
    return new StringBuilder();
  }

  @Benchmark
  public StringWriter initStringWriter() {
    return new StringWriter();
  }

  @Benchmark
  public PrintWriter initPrintWriter(WriterState writerState) {
    return new PrintWriter(writerState.writer);
  }

  /**
   * Benchmarks the performance of instantiating different {@link Appendable} objects and appending
   * the same string a fixed number of times.
   *
   * @param bh blackhole used as an optimization fence
   * @param appenderState the state holds the type of the appender and the number of appends that
   * need to be performed before resetting the appender
   * @throws IOException if the append operation encounters an I/O problem
   */
  @Benchmark
  public void appendString(Blackhole bh, AppenderState appenderState) throws IOException {
    bh.consume(appenderState.getOrCreateAppender().append("placeholder"));
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(StringConstructBenchmark.class.getName())
        .detectJvmArgs()
        .build();

    new Runner(opt).run();
  }
}

// End StringConstructBenchmark.java
