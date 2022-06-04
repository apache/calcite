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
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.materialize.MaterializationService;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import net.hydromatic.quidem.AbstractCommand;
import net.hydromatic.quidem.Command;
import net.hydromatic.quidem.CommandHandler;
import net.hydromatic.quidem.Quidem;

import org.junit.jupiter.api.BeforeEach;

import java.sql.Connection;
import java.util.Collection;
import java.util.List;
import java.util.Set;
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
      new BabelQuidemTest().test(arg);
    }
  }

  @BeforeEach public void setup() {
    MaterializationService.setThreadLocal();
  }

  /** For {@link QuidemTest#test(String)} parameters. */
  public static Collection<Object[]> data() {
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
              .with(CalciteConnectionProperty.FUN, "standard,postgresql,oracle")
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

  /** Command that prints the validated parse tree of a SQL statement. */
  static class ExplainValidatedCommand extends AbstractCommand {
    private final ImmutableList<String> lines;
    private final ImmutableList<String> content;
    private final Set<String> productSet;

    ExplainValidatedCommand(List<String> lines, List<String> content,
        Set<String> productSet) {
      this.lines = ImmutableList.copyOf(lines);
      this.content = ImmutableList.copyOf(content);
      this.productSet = ImmutableSet.copyOf(productSet);
    }

    @Override public void execute(Context x, boolean execute) throws Exception {
      if (execute) {
        // use Babel parser
        final SqlParser.Config parserConfig =
            SqlParser.config().withParserFactory(SqlBabelParserImpl.FACTORY);

        // extract named schema from connection and use it in planner
        final CalciteConnection calciteConnection =
            x.connection().unwrap(CalciteConnection.class);
        final String schemaName = calciteConnection.getSchema();
        final SchemaPlus schema =
            schemaName != null
                ? calciteConnection.getRootSchema().getSubSchema(schemaName)
                : calciteConnection.getRootSchema();
        final Frameworks.ConfigBuilder config =
            Frameworks.newConfigBuilder()
                .defaultSchema(schema)
                .parserConfig(parserConfig)
                .context(Contexts.of(calciteConnection.config()));

        // parse, validate and un-parse
        final Quidem.SqlCommand sqlCommand = x.previousSqlCommand();
        final Planner planner = Frameworks.getPlanner(config.build());
        final SqlNode node = planner.parse(sqlCommand.sql);
        final SqlNode validateNode = planner.validate(node);
        final SqlWriter sqlWriter = new SqlPrettyWriter();
        validateNode.unparse(sqlWriter, 0, 0);
        x.echo(ImmutableList.of(sqlWriter.toSqlString().getSql()));
      } else {
        x.echo(content);
      }
      x.echo(lines);
    }
  }

  /** Command handler that adds a "!explain-validated-on dialect..." command
   * (see {@link ExplainValidatedCommand}). */
  private static class BabelCommandHandler implements CommandHandler {
    @Override public Command parseCommand(List<String> lines,
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
          return new ExplainValidatedCommand(lines, content, set.build());
        }
      }
      return null;
    }
  }
}
