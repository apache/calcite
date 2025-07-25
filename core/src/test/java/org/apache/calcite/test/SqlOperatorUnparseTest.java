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

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.parser.StringAndPos;
import org.apache.calcite.sql.test.SqlOperatorFixture;
import org.apache.calcite.sql.test.SqlTestFactory;

import org.junit.jupiter.api.Disabled;

import java.util.function.Consumer;
import java.util.function.UnaryOperator;

/**
 * Version of a SqlOperatorTest which first parses and unparses
 * the test program before executing it. Although similar to
 * {@link org.apache.calcite.sql.parser.SqlUnParserTest},
 * this test also validates the code after unparsing.
 */
@SuppressWarnings("JavadocReference")
public class SqlOperatorUnparseTest extends CalciteSqlOperatorTest {
  /** Fixture that runs an operator test after parsing and unparsing a query. */
  static class SqlOperatorFixtureUnparseImpl extends SqlOperatorFixtureImpl {
    SqlOperatorFixtureUnparseImpl(SqlTestFactory factory) {
      super(factory, new UnparseTester(factory), false);
    }

    /**
     * Retrieve the tester as an UnparseTester.  A downcast is needed because
     * our tester implements a richer API than a regular SqlTester -- for example,
     * it has a method withFactory.
     */
    UnparseTester getUnparseTester() {
      return (UnparseTester) this.getTester();
    }

    public static final SqlOperatorFixtureImpl DEFAULT =
        new SqlOperatorFixtureUnparseImpl(SqlTestFactory.INSTANCE);

    @Override public SqlOperatorFixture withFactory(UnaryOperator<SqlTestFactory> transform) {
      return super
          .withFactory(transform)
          // Pass the transform to the tester
          .withTester(t -> this.getUnparseTester().withFactory(transform));
    }
  }

  @Override protected SqlOperatorFixture fixture() {
    return SqlOperatorFixtureUnparseImpl.DEFAULT;
  }

  /** A tester which parses, unparses, and then tests a query. */
  static class UnparseTester extends TesterImpl {
    public final SqlTestFactory factory;

    UnparseTester(SqlTestFactory factory) {
      this.factory = factory;
    }

    TesterImpl withFactory(UnaryOperator<SqlTestFactory> transform) {
      return new UnparseTester(transform.apply(this.factory));
    }

    String rewrite(StringAndPos sap) throws SqlParseException {
      final SqlParser parser = factory.createParser(sap.sql);
      final SqlNode sqlNode = parser.parseStmt();
      String result = sqlNode.toSqlString(c -> c.withClauseStartsLine(false)).getSql();
      return SqlParserUtil.escapeCarets(result);
    }

    @Override public void forEachQuery(
        SqlTestFactory factory, String expressionWithCarets, Consumer<String> consumer) {
      String query = buildQuery2(factory, expressionWithCarets);
      query = SqlParserUtil.escapeCarets(query);
      consumer.accept(query);
    }

    @Override public void check(
        SqlTestFactory factory, String queryWithCarets, TypeChecker typeChecker,
        ParameterChecker parameterChecker, ResultChecker resultChecker) {
      try {
        StringAndPos sap = StringAndPos.of(queryWithCarets);
        String optQuery = this.rewrite(sap);
        super.check(factory, optQuery, typeChecker, parameterChecker, resultChecker);
      } catch (SqlParseException e) {
        throw new RuntimeException(e);
      }
    }
  }

  // Every test that is Disabled below corresponds to a bug.
  // These tests should just be deleted when the corresponding bugs are fixed.

  @Override @Disabled("https://issues.apache.org/jira/browse/CALCITE-5998 "
      + "The SAFE_OFFSET operator can cause an index out of bounds exception")
  void testSafeOffsetOperator() {
    super.testSafeOffsetOperator();
  }

  @Override @Disabled("https://issues.apache.org/jira/browse/CALCITE-6002 "
      + "CONTAINS_SUBSTR does not unparse correctly")
  void testContainsSubstrFunc() {
    super.testContainsSubstrFunc();
  }
}
