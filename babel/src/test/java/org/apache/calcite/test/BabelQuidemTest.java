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
package org.apache.calcite.test;

import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.materialize.MaterializationService;
import org.apache.calcite.sql.dialect.BigQuerySqlDialect;
import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

import com.google.common.collect.ImmutableSet;

import net.hydromatic.quidem.Command;
import net.hydromatic.quidem.CommandHandler;
import net.hydromatic.quidem.Quidem;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.BeforeEach;

import java.sql.Connection;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Unit tests for the Babel SQL parser.
 */
class BabelQuidemTest extends QuidemTest {
  /** Runs a test from the command line.
   *
   * <p>For example:
   *
   * <blockquote>
   *   <code>java BabelQuidemTest sql/table.iq</code>
   * </blockquote> */
  public static void main(String[] args) throws Exception {
    for (String arg : args) {
      Unsafe.setDefaultLocale(Locale.US);
      new BabelQuidemTest().test(arg);
    }
  }

  @BeforeEach public void setup() {
    MaterializationService.setThreadLocal();
  }

  /** For {@link QuidemTest#test(String)} parameters. */
  @Override public Collection<String> getPath() {
    // Start with a test file we know exists, then find the directory and list
    // its files.
    final String first = "sql/select.iq";
    return data(first);
  }

  @Override protected Quidem.ConnectionFactory createConnectionFactory() {
    return new QuidemConnectionFactory() {
      @Override public Connection connect(String name, boolean reference)
          throws Exception {
        switch (name) {
        case "babel":
          return BabelTest.connect();
        case "scott-babel":
          return CalciteAssert.that()
              .with(CalciteAssert.Config.SCOTT)
              .with(CalciteConnectionProperty.PARSER_FACTORY,
                  SqlBabelParserImpl.class.getName() + "#FACTORY")
              .with(CalciteConnectionProperty.CONFORMANCE,
                  SqlConformanceEnum.BABEL)
              .connect();
        case "scott-redshift":
          return CalciteAssert.that()
              .with(CalciteAssert.Config.SCOTT)
              .with(CalciteConnectionProperty.FUN, "standard,redshift")
              .with(CalciteConnectionProperty.PARSER_FACTORY,
                  SqlBabelParserImpl.class.getName() + "#FACTORY")
              .with(CalciteConnectionProperty.CONFORMANCE,
                  SqlConformanceEnum.BABEL)
              .with(CalciteConnectionProperty.LENIENT_OPERATOR_LOOKUP, true)
              .connect();
        case "scott-big-query":
          return CalciteAssert.that()
              .with(CalciteAssert.Config.SCOTT)
              .with(CalciteConnectionProperty.FUN, "standard,bigquery")
              .with(CalciteConnectionProperty.LEX, Lex.BIG_QUERY)
              .with(CalciteConnectionProperty.PARSER_FACTORY,
                  SqlBabelParserImpl.class.getName() + "#FACTORY")
              .with(CalciteConnectionProperty.CONFORMANCE,
                  SqlConformanceEnum.BABEL)
              .with(CalciteConnectionProperty.LENIENT_OPERATOR_LOOKUP, true)
              .with(CalciteConnectionProperty.TYPE_SYSTEM,
                  BigQuerySqlDialect.class.getName() + "#TYPE_SYSTEM")
              .with(
                  ConnectionFactories.addType("DATETIME", typeFactory ->
                      typeFactory.createSqlType(SqlTypeName.TIMESTAMP)))
              // More type aliases could be added to emulate Big Query more precisely
              .with(
                  ConnectionFactories.addType("INT64", typeFactory ->
                      typeFactory.createSqlType(SqlTypeName.BIGINT)))
              .with(
                  ConnectionFactories.addType("TIMESTAMP", typeFactory ->
                      typeFactory.createSqlType(
                          SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)))
              .connect();
        case "scott-postgresql":
          return CalciteAssert.that()
              .with(CalciteAssert.SchemaSpec.SCOTT)
              .with(CalciteConnectionProperty.FUN, "standard,postgresql")
              .with(CalciteConnectionProperty.PARSER_FACTORY,
                  BabelDdlExecutor.class.getName() + "#PARSER_FACTORY")
              .with(CalciteConnectionProperty.CONFORMANCE,
                  SqlConformanceEnum.BABEL)
              .with(CalciteConnectionProperty.LENIENT_OPERATOR_LOOKUP, true)
              .connect();
        case "scott-spark":
          return CalciteAssert.that()
              .with(CalciteAssert.SchemaSpec.SCOTT)
              .with(CalciteConnectionProperty.FUN, "standard,spark")
              .with(CalciteConnectionProperty.PARSER_FACTORY,
                  BabelDdlExecutor.class.getName() + "#PARSER_FACTORY")
              .with(CalciteConnectionProperty.CONFORMANCE,
                  SqlConformanceEnum.BABEL)
              .with(CalciteConnectionProperty.LENIENT_OPERATOR_LOOKUP, true)
              .connect();
        default:
          return super.connect(name, reference);
        }
      }
    };
  }

  @Override protected CommandHandler createCommandHandler() {
    return new BabelCommandHandler();
  }

  /** Command handler that adds a "!explain-validated-on dialect..." command
   * (see {@link QuidemTest.ExplainValidatedCommand}). */
  private static class BabelCommandHandler implements CommandHandler {
    @Override public @Nullable Command parseCommand(List<String> lines,
        List<String> content, String line) {
      final String prefix = "explain-validated-on";
      if (line.startsWith(prefix)) {
        final Pattern pattern =
            Pattern.compile("explain-validated-on( [-_+a-zA-Z0-9]+)*?");
        final Matcher matcher = pattern.matcher(line);
        if (matcher.matches()) {
          final ImmutableSet.Builder<String> set = ImmutableSet.builder();
          for (int i = 0; i < matcher.groupCount(); i++) {
            set.add(matcher.group(i + 1));
          }
          return new QuidemTest.ExplainValidatedCommand(
              SqlBabelParserImpl.FACTORY, lines, content, set.build());
        }
      }
      return null;
    }
  }
}
