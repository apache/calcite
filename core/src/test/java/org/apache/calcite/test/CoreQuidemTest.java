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
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

import com.google.common.collect.ImmutableSet;

import net.hydromatic.quidem.Command;
import net.hydromatic.quidem.CommandHandler;
import net.hydromatic.quidem.Quidem;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.Connection;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.calcite.util.Util.discard;

/**
 * Test that runs every Quidem file in the "core" module as a test.
 */
class CoreQuidemTest extends QuidemTest {
  /** Runs a test from the command line.
   *
   * <p>For example:
   *
   * <blockquote>
   *   <code>java CoreQuidemTest sql/dummy.iq</code>
   * </blockquote> */
  public static void main(String[] args) throws Exception {
    for (String arg : args) {
      new CoreQuidemTest().test(arg);
    }
  }

  /** For {@link QuidemTest#test(String)} parameters. */
  @Override public Collection<String> getPath() {
    // Start with a test file we know exists, then find the directory and list
    // its files.
    final String first = "sql/agg.iq";
    return data(first);
  }

  @Override protected Quidem.ConnectionFactory createConnectionFactory() {
    return new QuidemConnectionFactory() {
      @Override public Connection connect(String name, boolean reference) throws Exception {
        switch (name) {
        case "blank":
          return CalciteAssert.that()
              .with(CalciteConnectionProperty.PARSER_FACTORY,
                  ExtensionDdlExecutor.class.getName() + "#PARSER_FACTORY")
              .with(CalciteAssert.SchemaSpec.BLANK)
              .connect();
        case "scott":
          return CalciteAssert.that()
              .with(CalciteConnectionProperty.PARSER_FACTORY,
                  ExtensionDdlExecutor.class.getName() + "#PARSER_FACTORY")
              .with(CalciteConnectionProperty.FUN, SqlLibrary.CALCITE.fun)
              .with(CalciteAssert.Config.SCOTT)
              .connect();
        case "scott-spark":
          discard(CustomTypeSystems.SPARK_TYPE_SYSTEM);
          return CalciteAssert.that()
              .with(CalciteConnectionProperty.PARSER_FACTORY,
                  ExtensionDdlExecutor.class.getName() + "#PARSER_FACTORY")
              .with(CalciteConnectionProperty.FUN, SqlLibrary.CALCITE.fun)
              .with(CalciteConnectionProperty.TYPE_SYSTEM,
                  CustomTypeSystems.class.getName() + "#SPARK_TYPE_SYSTEM")
              .with(CalciteAssert.Config.SCOTT)
              .connect();
        case "scott-checked-rounding-half-up":
          discard(CustomTypeSystems.ROUNDING_MODE_HALF_UP);
          return CalciteAssert.that()
              .with(CalciteConnectionProperty.PARSER_FACTORY,
                  ExtensionDdlExecutor.class.getName() + "#PARSER_FACTORY")
              // Use bigquery conformance, which forces checked arithmetic
              .with(CalciteConnectionProperty.CONFORMANCE, SqlConformanceEnum.BIG_QUERY)
              .with(CalciteConnectionProperty.FUN, SqlLibrary.CALCITE.fun)
              .with(CalciteConnectionProperty.TYPE_SYSTEM,
                  CustomTypeSystems.class.getName() + "#ROUNDING_MODE_HALF_UP")
              .with(CalciteAssert.Config.SCOTT)
              .connect();
        case "scott-negative-scale":
          discard(CustomTypeSystems.NEGATIVE_SCALE);
          return CalciteAssert.that()
              .with(CalciteConnectionProperty.PARSER_FACTORY,
                  ExtensionDdlExecutor.class.getName() + "#PARSER_FACTORY")
              .with(CalciteConnectionProperty.FUN, SqlLibrary.CALCITE.fun)
              .with(CalciteConnectionProperty.TYPE_SYSTEM,
                  CustomTypeSystems.class.getName() + "#NEGATIVE_SCALE")
              .with(CalciteAssert.Config.SCOTT)
              .connect();
        case "scott-negative-scale-rounding-half-up":
          discard(CustomTypeSystems.NEGATIVE_SCALE_ROUNDING_MODE_HALF_UP);
          return CalciteAssert.that()
              .with(CalciteConnectionProperty.PARSER_FACTORY,
                  ExtensionDdlExecutor.class.getName() + "#PARSER_FACTORY")
              .with(CalciteConnectionProperty.FUN, SqlLibrary.CALCITE.fun)
              .with(CalciteConnectionProperty.TYPE_SYSTEM,
                  CustomTypeSystems.class.getName()
                      + "#NEGATIVE_SCALE_ROUNDING_MODE_HALF_UP")
              .with(CalciteAssert.Config.SCOTT)
              .connect();
        case "scott-lenient":
          // Same as "scott", but uses LENIENT conformance.
          // TODO: add a way to change conformance without defining a new
          // connection
          return CalciteAssert.that()
              .with(CalciteConnectionProperty.PARSER_FACTORY,
                  ExtensionDdlExecutor.class.getName() + "#PARSER_FACTORY")
              .with(CalciteConnectionProperty.CONFORMANCE,
                  SqlConformanceEnum.LENIENT)
              .with(CalciteAssert.Config.SCOTT)
              .connect();
        case "scott-babel":
          // Same as "scott", but uses BABEL conformance.
          // connection
          return CalciteAssert.that()
              .with(CalciteConnectionProperty.PARSER_FACTORY,
                  ExtensionDdlExecutor.class.getName() + "#PARSER_FACTORY")
              .with(CalciteConnectionProperty.CONFORMANCE,
                  SqlConformanceEnum.BABEL)
              .with(CalciteAssert.Config.SCOTT)
              .connect();
        case "scott-mysql":
          // Same as "scott", but uses MySQL conformance.
          return CalciteAssert.that()
              .with(CalciteConnectionProperty.PARSER_FACTORY,
                  ExtensionDdlExecutor.class.getName() + "#PARSER_FACTORY")
              .with(CalciteConnectionProperty.CONFORMANCE,
                  SqlConformanceEnum.MYSQL_5)
              .with(CalciteAssert.Config.SCOTT)
              .connect();
        case "scott-oracle":
          // Same as "scott", but uses Oracle conformance.
          return CalciteAssert.that()
              .with(CalciteConnectionProperty.PARSER_FACTORY,
                  ExtensionDdlExecutor.class.getName() + "#PARSER_FACTORY")
              .with(CalciteConnectionProperty.CONFORMANCE,
                  SqlConformanceEnum.ORACLE_10)
              .with(CalciteAssert.Config.SCOTT)
              .connect();
        case "scott-mssql":
          // Same as "scott", but uses SQL_SERVER_2008 conformance.
          return CalciteAssert.that()
              .with(CalciteConnectionProperty.PARSER_FACTORY,
                  ExtensionDdlExecutor.class.getName() + "#PARSER_FACTORY")
              .with(CalciteConnectionProperty.CONFORMANCE,
                  SqlConformanceEnum.SQL_SERVER_2008)
              .with(CalciteAssert.Config.SCOTT)
              .connect();
        case "steelwheels":
          return CalciteAssert.that()
              .with(CalciteConnectionProperty.PARSER_FACTORY,
                  ExtensionDdlExecutor.class.getName() + "#PARSER_FACTORY")
              .with(CalciteConnectionProperty.FUN, SqlLibrary.CALCITE.fun)
              .with(CalciteAssert.SchemaSpec.STEELWHEELS)
              .with(Lex.BIG_QUERY)
              .connect();
        default:
          return super.connect(name, reference);
        }
      }
    };
  }

  /** Override settings for "sql/misc.iq". */
  public void testSqlMisc(String path) throws Exception {
    switch (CalciteAssert.DB) {
    case ORACLE:
      // There are formatting differences (e.g. "4.000" vs "4") when using
      // Oracle as the JDBC data source.
      return;
    }
    checkRun(path);
  }

  @Override protected CommandHandler createCommandHandler() {
    return new ExtendedCommandHandler();
  }

  /** Command handler that adds a "!explain-validated-on dialect..." command
   * (see {@link QuidemTest.ExplainValidatedCommand}). */
  private static class ExtendedCommandHandler implements CommandHandler {
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
              SqlParserImpl.FACTORY, lines, content, set.build());
        }
      }
      return null;
    }
  }
}
