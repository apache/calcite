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

import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.config.Lex;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Programs;

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
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks Conversion of Sql To RelNode and conversion of SqlNode to RelNode.
 */
@Fork(value = 1, jvmArgsPrepend = "-Xmx2048m")
@Measurement(iterations = 10, time = 100, timeUnit = TimeUnit.MILLISECONDS)
@Warmup(iterations = 10, time = 100, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Threads(1)
public class RelNodeConversionBenchmark {

  /**
   * A common state needed for this benchmark.
   */
  public abstract static class RelNodeConversionBenchmarkState {
    String sql;
    Planner p;

    public void setup(int length, int columnLength) {
      setup(length, columnLength, SqlTypeName.INTEGER);
    }

    public void setup(int length, int columnLength,
        SqlTypeName columnType) {
      // Create Sql
      StringBuilder sb = new StringBuilder();
      sb.append("select 1 ");
      Random rnd = new Random();
      rnd.setSeed(424242);
      for (int i = 0; i < length; i++) {
        sb.append(", ");
        sb.append(
            String.format(Locale.ROOT, "c%s / CASE WHEN c%s > %d THEN c%s ELSE c%s END ",
                String.valueOf(rnd.nextInt(columnLength)), String.valueOf(i % columnLength),
                rnd.nextInt(columnLength), String.valueOf(rnd.nextInt(columnLength)),
                String.valueOf(rnd.nextInt(columnLength))));
      }
      sb.append(" FROM test1");
      sql = sb.toString();

      // Create Schema and Table

      AbstractTable t = new AbstractQueryableTable(Integer.class) {
        final List<Integer> items = ImmutableList.of();
        final Enumerable<Integer> enumerable = Linq4j.asEnumerable(items);

        @Override public <E> Queryable<E> asQueryable(
            QueryProvider queryProvider, SchemaPlus schema, String tableName) {
          return (Queryable<E>) enumerable.asQueryable();
        }

        @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
          RelDataTypeFactory.Builder builder = typeFactory.builder();
          for (int i = 0; i < columnLength; i++) {
            builder.add(String.format(Locale.ROOT, "c%d", i), columnType);
          }
          return builder.build();
        }
      };

      // Create Planner
      final SchemaPlus schema = Frameworks.createRootSchema(true);
      schema.add("test1", t);

      final FrameworkConfig config = Frameworks.newConfigBuilder()
          .parserConfig(SqlParser.config().withLex(Lex.MYSQL))
          .defaultSchema(schema)
          .programs(Programs.ofRules(Programs.RULE_SET))
          .build();
      p = Frameworks.getPlanner(config);
    }
  }

  /**
   * A state holding information needed to parse.
   */
  @State(Scope.Thread)
  public static class SqlToRelNodeBenchmarkState extends RelNodeConversionBenchmarkState {
    @Param({"10000"})
    int length;

    @Param({"10", "100", "1000"})
    int columnLength;

    @Setup(Level.Iteration)
    public void setUp() {
      super.setup(length, columnLength);
    }

    public RelNode parse() throws Exception {
      SqlNode n = p.parse(sql);
      n = p.validate(n);
      RelNode rel = p.rel(n).project();
      p.close();
      p.reset();
      return rel;
    }
  }

  @Benchmark
  public RelNode parse(SqlToRelNodeBenchmarkState state) throws Exception {
    return state.parse();
  }

  /**
   * State for converting SQL that compares a VARCHAR to a large string array.
   *
   * <p>This benchmark targets allocation from traversing a large operand list.
   * Run with {@code -prof gc} and compare {@code gc.alloc.rate.norm}; latency is
   * not its primary metric.
   */
  @State(Scope.Thread)
  public static class LargeVarcharSomeArraySqlToRelNodeBenchmarkState
      extends RelNodeConversionBenchmarkState {
    @Param({"100000"})
    int length;

    @Setup(Level.Iteration)
    public void setUp() {
      super.setup(0, 1, SqlTypeName.VARCHAR);

      final StringBuilder builder = new StringBuilder(length * 8);
      builder.append("select c0 from test1 where c0 = some (array[");
      for (int i = 0; i < length; i++) {
        if (i > 0) {
          builder.append(", ");
        }
        builder.append('\'').append(i).append('\'');
      }
      sql = builder.append("])").toString();
    }

    public RelNode parse() throws Exception {
      SqlNode node = p.parse(sql);
      node = p.validate(node);
      RelNode rel = p.rel(node).project();
      p.close();
      p.reset();
      return rel;
    }
  }

  @Benchmark
  public RelNode parseLargeVarcharSomeArray(
      LargeVarcharSomeArraySqlToRelNodeBenchmarkState state) throws Exception {
    return state.parse();
  }

  /** State for traversing a large string ARRAY with {@link SqlShuttle}. */
  @State(Scope.Thread)
  public static class LargeVarcharArraySqlShuttleBenchmarkState {
    @Param({"100000"})
    int length;

    SqlCall arrayCall;
    SqlShuttle unchangedShuttle;
    SqlShuttle changedShuttle;

    @Setup(Level.Trial)
    public void setUp() {
      final SqlNode[] operands = new SqlNode[length];
      for (int i = 0; i < length; i++) {
        operands[i] =
            SqlLiteral.createCharString(Integer.toString(i), SqlParserPos.ZERO);
      }
      arrayCall =
          SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR.createCall(SqlParserPos.ZERO, operands);

      final SqlNode target = operands[length / 2];
      final SqlLiteral replacement =
          SqlLiteral.createCharString("replacement", SqlParserPos.ZERO);
      unchangedShuttle = new SqlShuttle();
      changedShuttle = new SqlShuttle() {
        @Override public SqlNode visit(SqlLiteral literal) {
          return literal == target ? replacement : literal;
        }
      };
    }
  }

  @Benchmark
  public SqlNode visitLargeVarcharArrayUnchanged(
      LargeVarcharArraySqlShuttleBenchmarkState state) {
    return state.arrayCall.accept(state.unchangedShuttle);
  }

  @Benchmark
  public SqlNode visitLargeVarcharArrayChanged(
      LargeVarcharArraySqlShuttleBenchmarkState state) {
    return state.arrayCall.accept(state.changedShuttle);
  }

  /**
   * A state holding information needed to convert To Rel.
   */
  @State(Scope.Thread)
  public static class SqlNodeToRelNodeBenchmarkState extends RelNodeConversionBenchmarkState {
    @Param({"10000"})
    int length;

    @Param({"10", "100", "1000"})
    int columnLength;
    SqlNode sqlNode;

    @Setup(Level.Iteration)
    public void setUp() {
      super.setup(length, columnLength);
      try {
        sqlNode = p.validate(p.parse(sql));
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    public RelNode convertToRel() throws Exception {
      return p.rel(sqlNode).project();
    }
  }

  @Benchmark
  public RelNode convertToRel(SqlNodeToRelNodeBenchmarkState state) throws Exception {
    return state.convertToRel();
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(RelNodeConversionBenchmark.class.getSimpleName())
        .addProfiler(GCProfiler.class)
        .addProfiler(FlightRecorderProfiler.class)
        .detectJvmArgs()
        .build();

    new Runner(opt).run();
  }

}
