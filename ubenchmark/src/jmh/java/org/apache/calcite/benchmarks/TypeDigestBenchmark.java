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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

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

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark for {@link RelDataType} digest generation and comparison.
 */
@Fork(value = 1, jvmArgsPrepend = "-Dcalcite.disable.generate.type.digest.string=true")
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Threads(1)
public class TypeDigestBenchmark {

  @Param({"1", "50", "500", "5000", "50000"})
  int topN;

  RelDataType type;
  RelDataType type2;

  @Setup(Level.Trial)
  public void setup() {
    type = createType(topN);
    type2 = createType(topN);
  }

  private RelDataType createType(int n) {
    RelBuilder builder =
        RelBuilder.create(Frameworks.newConfigBuilder()
            .defaultSchema(Frameworks.createRootSchema(true))
            .build());
    final RelDataTypeFactory typeFactory = builder.getTypeFactory();

    RelDataType varchar =
        typeFactory.createTypeWithCharsetAndCollation(typeFactory
                .createSqlType(SqlTypeName.VARCHAR, 100),
            StandardCharsets.UTF_8, SqlCollation.IMPLICIT);

    RelDataType leafObj = typeFactory.builder().add("k", varchar).add("v", varchar)
        .add("attrs", typeFactory.createMapType(varchar, varchar))
        .add("tags", typeFactory.createArrayType(varchar, -1)).build();

    final RelDataTypeFactory.Builder root = typeFactory.builder();
    for (int i = 0; i < n; i++) {
      int depth = 1 + (i % 8);
      RelDataType t = leafObj;

      for (int d = 0; d < depth; d++) {
        RelDataType arrObj = typeFactory.createArrayType(t, -1);
        RelDataType mapObj = typeFactory.createMapType(varchar, t);

        t =
            typeFactory.builder().add("lvl" + d, t).add("arr" + d, arrObj).add("map" + d, mapObj)
                .add("s" + d, varchar).build();
      }

      if ((i % 11) == 0) {
        root.add("f" + i, typeFactory.createArrayType(t, -1));
      } else if ((i % 11) == 1) {
        root.add("f" + i, typeFactory.createMapType(varchar, t));
      } else {
        root.add("f" + i, t);
      }
    }

    return root.build();
  }

  @Benchmark
  public boolean testEquals() {
    return type.hashCode() == type2.hashCode() && type.equals(type2);
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(TypeDigestBenchmark.class.getSimpleName())
        .detectJvmArgs()
        .build();

    new Runner(opt).run();
  }
}
