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
package org.apache.calcite.sql.test;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.test.DiffRepository;
import org.apache.calcite.util.Litmus;

import org.junit.Ignore;
import org.junit.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link SqlPrettyWriter}.
 *
 * <p>You must provide the system property "source.dir".
 */
public class SqlPrettyWriterTest {
  //~ Static fields/initializers ---------------------------------------------

  //~ Constructors -----------------------------------------------------------

  public SqlPrettyWriterTest() {
  }

  //~ Methods ----------------------------------------------------------------

  // ~ Helper methods -------------------------------------------------------

  protected DiffRepository getDiffRepos() {
    return DiffRepository.lookup(SqlPrettyWriterTest.class);
  }

  /**
   * Parses a SQL query. To use a different parser, override this method.
   */
  protected SqlNode parseQuery(String sql) {
    SqlNode node;
    try {
      node = SqlParser.create(sql).parseQuery();
    } catch (SqlParseException e) {
      String message = "Received error while parsing SQL '" + sql + "'"
          + "; error is:\n"
          + e.toString();
      throw new AssertionError(message);
    }
    return node;
  }

  /** Helper. */
  class Sql {
    private final String sql;
    private final boolean expr;
    private final String desc;
    private final String formatted;
    private final UnaryOperator<SqlPrettyWriter> transform;

    Sql(String sql, boolean expr, String desc, String formatted,
        UnaryOperator<SqlPrettyWriter> transform) {
      this.sql = Objects.requireNonNull(sql);
      this.expr = expr;
      this.desc = desc;
      this.formatted = Objects.requireNonNull(formatted);
      this.transform = Objects.requireNonNull(transform);
    }

    Sql withWriter(Consumer<SqlPrettyWriter> consumer) {
      Objects.requireNonNull(consumer);
      return new Sql(sql, expr, desc, formatted, w -> {
        final SqlPrettyWriter w2 = transform.apply(w);
        consumer.accept(w2);
        return w2;
      });
    }

    Sql expectingDesc(String desc) {
      return Objects.equals(this.desc, desc)
          ? this
          : new Sql(sql, expr, desc, formatted, transform);
    }

    Sql withExpr(boolean expr) {
      return this.expr == expr
          ? this
          : new Sql(sql, expr, desc, formatted, transform);
    }

    Sql expectingFormatted(String formatted) {
      return Objects.equals(this.formatted, formatted)
          ? this
          : new Sql(sql, expr, desc, formatted, transform);
    }

    Sql check() {
      final SqlPrettyWriter prettyWriter =
          transform.apply(new SqlPrettyWriter(AnsiSqlDialect.DEFAULT));
      final SqlNode node;
      if (expr) {
        final SqlCall valuesCall = (SqlCall) parseQuery("VALUES (" + sql + ")");
        final SqlCall rowCall = valuesCall.operand(0);
        node = rowCall.operand(0);
      } else {
        node = parseQuery(sql);
      }

      // Describe settings
      if (desc != null) {
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw);
        prettyWriter.describe(pw, true);
        pw.flush();
        final String desc = sw.toString();
        getDiffRepos().assertEquals("desc", this.desc, desc);
      }

      // Format
      final String formatted = prettyWriter.format(node);
      getDiffRepos().assertEquals("formatted", this.formatted, formatted);

      // Now parse the result, and make sure it is structurally equivalent
      // to the original.
      final String actual2 = formatted.replaceAll("`", "\"");
      final SqlNode node2;
      if (expr) {
        final SqlCall valuesCall =
            (SqlCall) parseQuery("VALUES (" + actual2 + ")");
        final SqlCall rowCall = valuesCall.operand(0);
        node2 = rowCall.operand(0);
      } else {
        node2 = parseQuery(actual2);
      }
      assertTrue(node.equalsDeep(node2, Litmus.THROW));

      return this;
    }
  }

  private Sql simple() {
    return sql("select x as a, b as b, c as c, d,"
        + " 'mixed-Case string',"
        + " unquotedCamelCaseId,"
        + " \"quoted id\" "
        + "from"
        + " (select *"
        + " from t"
        + " where x = y and a > 5"
        + " group by z, zz"
        + " window w as (partition by c),"
        + "  w1 as (partition by c,d order by a, b"
        + "   range between interval '2:2' hour to minute preceding"
        + "    and interval '1' day following)) "
        + "order by gg");
  }

  private Sql sql(String sql) {
    return new Sql(sql, false, "${desc}", "${formatted}", w -> w);
  }

  private Sql expr(String sql) {
    return sql(sql).withExpr(true).expectingDesc(null);
  }

  // ~ Tests ----------------------------------------------------------------

  @Test public void testDefault() {
    simple().check();
  }

  @Test public void testIndent8() {
    simple()
        .withWriter(w -> w.setIndentation(8))
        .check();
  }

  @Test public void testClausesNotOnNewLine() {
    simple()
        .withWriter(w -> w.setClauseStartsLine(false))
        .check();
  }

  @Test public void testSelectListItemsOnSeparateLines() {
    simple()
        .withWriter(w -> w.setSelectListItemsOnSeparateLines(true))
        .check();
  }

  @Test public void testSelectListExtraIndentFlag() {
    simple()
        .withWriter(w -> w.setSelectListItemsOnSeparateLines(true))
        .withWriter(w -> w.setSelectListExtraIndentFlag(false))
        .check();
  }

  @Test public void testKeywordsLowerCase() {
    simple()
        .withWriter(w -> w.setKeywordsLowerCase(true))
        .check();
  }

  @Test public void testParenthesizeAllExprs() {
    simple()
        .withWriter(w -> w.setAlwaysUseParentheses(true))
        .check();
  }

  @Test public void testOnlyQuoteIdentifiersWhichNeedIt() {
    simple()
        .withWriter(w -> w.setQuoteAllIdentifiers(false))
        .check();
  }

  @Test public void testBlackSubQueryStyle() {
    // Note that ( is at the indent, SELECT is on the same line, and ) is
    // below it.
    simple()
        .withWriter(w -> w.setSubQueryStyle(SqlWriter.SubQueryStyle.BLACK))
        .check();
  }

  @Ignore("default SQL parser cannot parse DDL")
  @Test public void testExplain() {
    sql("explain select * from t")
        .check();
  }

  @Test public void testCase() {
    // Note that CASE is rewritten to the searched form. Wish it weren't
    // so, but that's beyond the control of the pretty-printer.
    // todo: indent should be 4 not 8
    final String sql = "case 1\n"
        + " when 2 + 3 then 4\n"
        + " when case a when b then c else d end then 6\n"
        + " else 7\n"
        + "end";
    final String formatted = "CASE\n"
        + "WHEN 1 = 2 + 3\n"
        + "THEN 4\n"
        + "WHEN 1 = CASE\n"
        + "        WHEN `A` = `B`\n" // todo: indent should be 4 not 8
        + "        THEN `C`\n"
        + "        ELSE `D`\n"
        + "        END\n"
        + "THEN 6\n"
        + "ELSE 7\n"
        + "END";
    expr(sql)
        .withWriter(w -> w.setCaseClausesOnNewLines(true))
        .expectingFormatted(formatted)
        .check();
  }

  @Test public void testCase2() {
    final String sql = "case 1"
        + " when 2 + 3 then 4"
        + " when case a when b then c else d end then 6"
        + " else 7 end";
    final String formatted = "CASE WHEN 1 = 2 + 3 THEN 4"
        + " WHEN 1 = CASE WHEN `A` = `B` THEN `C` ELSE `D` END THEN 6"
        + " ELSE 7 END";
    expr(sql)
        .expectingFormatted(formatted)
        .check();
  }

  @Test public void testBetween() {
    // todo: remove leading
    expr("x not between symmetric y and z")
        .expectingFormatted("`X` NOT BETWEEN SYMMETRIC `Y` AND `Z`")
        .check();

    // space
  }

  @Test public void testCast() {
    expr("cast(x + y as decimal(5, 10))")
        .expectingFormatted("CAST(`X` + `Y` AS DECIMAL(5, 10))")
        .check();
  }

  @Test public void testLiteralChain() {
    final String sql = "'x' /* comment */ 'y'\n"
        + "  'z' ";
    final String formatted = "'x'\n"
        + "'y'\n"
        + "'z'";
    expr(sql).expectingFormatted(formatted).check();
  }

  @Test public void testOverlaps() {
    final String sql = "(x,xx) overlaps (y,yy) or x is not null";
    final String formatted = "PERIOD (`X`, `XX`) OVERLAPS PERIOD (`Y`, `YY`)"
        + " OR `X` IS NOT NULL";
    expr(sql).expectingFormatted(formatted).check();
  }

  @Test public void testUnion() {
    // todo: SELECT should not be indented from UNION, like this:
    // UNION
    //     SELECT *
    //     FROM `W`
    final String sql = "select * from t "
        + "union select * from ("
        + "  select * from u "
        + "  union select * from v) "
        + "union select * from w "
        + "order by a, b";
    sql(sql)
        .expectingDesc(null)
        .check();
  }

  @Test public void testMultiset() {
    sql("values (multiset (select * from t))")
        .expectingDesc(null)
        .check();
  }

  @Test public void testInnerJoin() {
    sql("select * from x inner join y on x.k=y.k")
        .expectingDesc(null)
        .check();
  }

  @Test public void testWhereListItemsOnSeparateLinesOr() {
    final String sql = "select x"
        + " from y"
        + " where h is not null and i < j"
        + " or ((a or b) is true) and d not in (f,g)"
        + " or x <> z";
    sql(sql)
        .withWriter(w -> w.setSelectListItemsOnSeparateLines(true))
        .withWriter(w -> w.setSelectListExtraIndentFlag(false))
        .withWriter(w -> w.setWhereListItemsOnSeparateLines(true))
        .check();
  }

  @Test public void testWhereListItemsOnSeparateLinesAnd() {
    final String sql = "select x"
        + " from y"
        + " where h is not null and (i < j"
        + " or ((a or b) is true)) and (d not in (f,g)"
        + " or v <> ((w * x) + y) * z)";
    sql(sql)
        .withWriter(w -> w.setSelectListItemsOnSeparateLines(true))
        .withWriter(w -> w.setSelectListExtraIndentFlag(false))
        .withWriter(w -> w.setWhereListItemsOnSeparateLines(true))
        .check();
  }
}

// End SqlPrettyWriterTest.java
