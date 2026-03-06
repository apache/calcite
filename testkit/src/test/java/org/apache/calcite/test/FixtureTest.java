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

import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.sql.parser.SqlParserFixture;
import org.apache.calcite.sql.test.SqlOperatorFixture;

import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/** Tests test fixtures.
 *
 * <p>The key feature of fixtures is that they work outside of Calcite core
 * tests, and of course this test cannot verify that. So, additional tests will
 * be needed elsewhere. The code might look similar in these additional tests,
 * but the most likely breakages will be due to classes not being on the path.
 *
 * @see Fixtures */
public class FixtureTest {

  public static final String DIFF_REPOS_MESSAGE = "diffRepos is null; if you require a "
      + "DiffRepository, set it in "
      + "your test's fixture() method";

  /** Tests that you can write parser tests via {@link Fixtures#forParser()}. */
  @Test void testParserFixture() {
    // 'as' as identifier is invalid with Core parser
    final SqlParserFixture f = Fixtures.forParser();
    f.sql("select ^as^ from t")
        .fails("(?s)Encountered \"as\".*");

    // Postgres cast is invalid with core parser
    f.sql("select 1 ^:^: integer as x")
        .fails("(?s).*Encountered \":\" at .*");

    // Backtick fails
    f.sql("select ^`^foo` from `bar``")
        .fails("(?s)Lexical error at line 1, column 8.  "
            + "Encountered: \"`\" \\(96\\), .*");

    // After changing config, backtick succeeds
    f.sql("select `foo` from `bar`")
        .withConfig(c -> c.withQuoting(Quoting.BACK_TICK))
        .ok("SELECT `foo`\n"
            + "FROM `bar`");
  }

  /** Tests that you can run validator tests via
   * {@link Fixtures#forValidator()}. */
  @Test void testValidatorFixture() {
    final SqlValidatorFixture f = Fixtures.forValidator();
    f.withSql("select ^1 + date '2002-03-04'^")
        .fails("(?s).*Cannot apply '\\+' to arguments of"
            + " type '<INTEGER> \\+ <DATE>'.*");

    f.withSql("select 1 + 2 as three")
        .type("RecordType(INTEGER NOT NULL THREE) NOT NULL");
  }

  /** Tests that you can run operator tests via
   * {@link Fixtures#forValidator()}. */
  @Test void testOperatorFixture() {
    // The first fixture only validates, does not execute.
    final SqlOperatorFixture validateFixture = Fixtures.forOperators(false);
    final SqlOperatorFixture executeFixture = Fixtures.forOperators(true);

    // Passes with and without execution
    validateFixture.checkBoolean("1 < 5", true);
    executeFixture.checkBoolean("1 < 5", true);

    // The fixture that executes fails, because the result value is incorrect.
    validateFixture.checkBoolean("1 < 5", false);
    assertFails(() -> executeFixture.checkBoolean("1 < 5", false),
        "Query: values (1 < 5)", "<false>", "<true>");

    // The fixture that executes fails, because the result value is incorrect.
    validateFixture.checkScalarExact("1 + 2", "INTEGER NOT NULL", "foo");
    assertFails(() -> validateFixture.checkScalarExact("1 + 2", "DATE", "foo"),
        "Query: values (1 + 2)", "\"DATE\"", "\"INTEGER NOT NULL\"");

    // Both fixtures pass.
    validateFixture.checkScalarExact("1 + 2", "INTEGER NOT NULL", "3");
    executeFixture.checkScalarExact("1 + 2", "INTEGER NOT NULL", "3");

    // Both fixtures fail, because the type is incorrect.
    assertFails(() -> validateFixture.checkScalarExact("1 + 2", "DATE", "foo"),
        "Query: values (1 + 2)", "\"DATE\"", "\"INTEGER NOT NULL\"");
    assertFails(() -> executeFixture.checkScalarExact("1 + 2", "DATE", "foo"),
        "Query: values (1 + 2)", "\"DATE\"", "\"INTEGER NOT NULL\"");
  }

  static void assertFails(Runnable runnable, String msg, String expected, String actual) {
    try {
      runnable.run();
      fail("expected error");
    } catch (AssertionError e) {
      String expectedMessage = msg + "\n"
          + "Expected: is " + expected + "\n"
          + "     but: was " + actual;
      assertThat(e.getMessage(), is(expectedMessage));
    }
  }

  /** Tests that you can run SQL-to-Rel tests via
   * {@link Fixtures#forSqlToRel()}. */
  @Test void testSqlToRelFixture() {
    final SqlToRelFixture f =
        Fixtures.forSqlToRel()
            .withDiffRepos(DiffRepository.lookup(FixtureTest.class));
    final String sql = "select 1 from emp";
    f.withSql(sql).ok();
  }

  /** Tests that we get a good error message if a test needs a diff repository.
   *
   * @see DiffRepository#castNonNull(DiffRepository) */
  @Test void testSqlToRelFixtureNeedsDiffRepos() {
    try {
      final SqlToRelFixture f = Fixtures.forSqlToRel();
      final String sql = "select 1 from emp";
      f.withSql(sql).ok();
      throw new AssertionError("expected error");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is(DIFF_REPOS_MESSAGE));
    }
  }

  /** Tests the {@link SqlToRelFixture#ensuring(Predicate, UnaryOperator)}
   * test infrastructure. */
  @Test void testSqlToRelFixtureEnsure() {
    final SqlToRelFixture f = Fixtures.forSqlToRel();

    // Case 1. Predicate is true at first, remedy not needed
    f.ensuring(f2 -> true, f2 -> {
      throw new AssertionError("remedy not needed");
    });

    // Case 2. Predicate is false at first, true after we invoke the remedy.
    final AtomicInteger b = new AtomicInteger(0);
    assertThat(b.intValue(), is(0));
    f.ensuring(f2 -> b.intValue() > 0, f2 -> {
      b.incrementAndGet();
      return f2;
    });
    assertThat(b.intValue(), is(1));

    // Case 3. Predicate is false at first, remains false after the "remedy" is
    // invoked.
    try {
      f.ensuring(f2 -> b.intValue() < 0, f2 -> {
        b.incrementAndGet();
        return f2;
      });
      throw new AssertionFailedError("expected AssertionError");
    } catch (AssertionError e) {
      String expectedMessage = "remedy failed\n"
          + "Expected: is <true>\n"
          + "     but: was <false>";
      assertThat(e.getMessage(), is(expectedMessage));
    }
    assertThat("Remedy should be called, even though it is unsuccessful",
        b.intValue(), is(2));
  }

  /** Tests that you can run RelRule tests via
   * {@link Fixtures#forValidator()}. */
  @Test void testRuleFixture() {
    final String sql = "select * from dept\n"
        + "union\n"
        + "select * from dept";
    final RelOptFixture f =
        Fixtures.forRules()
            .withDiffRepos(DiffRepository.lookup(FixtureTest.class));
    f.sql(sql)
        .withRule(CoreRules.UNION_TO_DISTINCT)
        .check();
  }

  /** As {@link #testSqlToRelFixtureNeedsDiffRepos} but for
   * {@link Fixtures#forRules()}. */
  @Test void testRuleFixtureNeedsDiffRepos() {
    try {
      final String sql = "select * from dept\n"
          + "union\n"
          + "select * from dept";
      final RelOptFixture f = Fixtures.forRules();
      f.sql(sql)
          .withRule(CoreRules.UNION_TO_DISTINCT)
          .check();
      throw new AssertionError("expected error");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is(DIFF_REPOS_MESSAGE));
    }
  }

  /** Tests metadata. */
  @Test void testMetadata() {
    final RelMetadataFixture f = Fixtures.forMetadata();
    f.withSql("select dname from dept")
          .assertColumnOriginSingle("DEPT", "DNAME", false);
    f.withSql("select upper(dname) as dname from dept")
        .assertColumnOriginSingle("DEPT", "DNAME", true);
    f.withSql("select dname||ename from dept,emp")
        .assertColumnOriginDouble("DEPT", "DNAME", "EMP", "ENAME", true);
    f.withSql("select 'Minstrelsy' as dname from dept")
        .assertColumnOriginIsEmpty();
  }
}
