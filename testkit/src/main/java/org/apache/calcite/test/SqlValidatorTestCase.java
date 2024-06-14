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

import org.apache.calcite.sql.parser.StringAndPos;
import org.apache.calcite.sql.test.SqlTestFactory;
import org.apache.calcite.sql.test.SqlValidatorTester;
import org.apache.calcite.sql.validate.SqlValidator;

/**
 * An abstract base class for implementing tests against {@link SqlValidator}.
 *
 * <p>A derived class can refine this test in two ways. First, it can add
 * {@code testXxx()} methods, to test more functionality.
 *
 * <p>Second, it can override the {@link #fixture()} method to return a
 * different implementation of the {@link SqlValidatorFixture} object. This
 * encapsulates the differences between test environments, for example, which
 * SQL parser or validator to use.
 */
public class SqlValidatorTestCase {
  public static final SqlValidatorFixture FIXTURE =
      new SqlValidatorFixture(SqlValidatorTester.DEFAULT,
          SqlTestFactory.INSTANCE, StringAndPos.of("?"), false, false);

  /** Creates a test case. */
  public SqlValidatorTestCase() {
  }

  //~ Methods ----------------------------------------------------------------

  /** Creates a test fixture. Derived classes can override this method to
   * run the same set of tests in a different testing environment. */
  public SqlValidatorFixture fixture() {
    return FIXTURE;
  }

  /**
   * Creates a test context with a SQL query.
   * Default catalog: {@link org.apache.calcite.test.catalog.MockCatalogReaderSimple#init()}.
   */
  public final SqlValidatorFixture sql(String sql) {
    return fixture().withSql(sql);
  }

  /** Creates a test context with a SQL expression. */
  public final SqlValidatorFixture expr(String sql) {
    return fixture().withExpr(sql);
  }

  /** Creates a test context with a SQL expression.
   * If an error occurs, the error is expected to span the entire expression. */
  public final SqlValidatorFixture wholeExpr(String sql) {
    return expr(sql).withWhole(true);
  }

  public final SqlValidatorFixture winSql(String sql) {
    return sql(sql);
  }

  public final SqlValidatorFixture win(String sql) {
    return sql("select * from emp " + sql);
  }

  public SqlValidatorFixture winExp(String sql) {
    return winSql("select " + sql + " from emp window w as (order by deptno)");
  }

  public SqlValidatorFixture winExp2(String sql) {
    return winSql("select " + sql + " from emp");
  }

}
