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

import org.apache.calcite.sql.test.SqlTestFactory;
import org.apache.calcite.sql.test.SqlTester;
import org.apache.calcite.sql.test.SqlValidatorTester;
import org.apache.calcite.test.catalog.MockCatalogReaderDynamic;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

/**
 * Concrete child class of {@link SqlValidatorTestCase}, containing lots of unit
 * tests.
 *
 * <p>If you want to run these same tests in a different environment, create a
 * derived class whose {@link #getTester} returns a different implementation of
 * {@link SqlTester}.
 */
public class SqlValidatorDynamicTest extends SqlValidatorTestCase {
  //~ Static fields/initializers ---------------------------------------------

  /**
   * @deprecated Deprecated so that usages of this constant will show up in
   * yellow in Intellij and maybe someone will fix them.
   */
  protected static final boolean TODO = false;
  private static final String ANY = "(?s).*";

  protected static final Logger LOGGER =
      LoggerFactory.getLogger(SqlValidatorDynamicTest.class);

  private static final String ERR_IN_VALUES_INCOMPATIBLE =
      "Values in expression list must have compatible types";

  private static final String ERR_IN_OPERANDS_INCOMPATIBLE =
      "Values passed to IN operator must have compatible types";

  private static final String ERR_AGG_IN_GROUP_BY =
      "Aggregate expression is illegal in GROUP BY clause";

  private static final String ERR_AGG_IN_ORDER_BY =
      "Aggregate expression is illegal in ORDER BY clause of non-aggregating SELECT";

  private static final String ERR_NESTED_AGG =
      "Aggregate expressions cannot be nested";

  private static final String EMP_RECORD_TYPE =
      "RecordType(INTEGER NOT NULL EMPNO,"
          + " VARCHAR(20) NOT NULL ENAME,"
          + " VARCHAR(10) NOT NULL JOB,"
          + " INTEGER MGR,"
          + " TIMESTAMP(0) NOT NULL HIREDATE,"
          + " INTEGER NOT NULL SAL,"
          + " INTEGER NOT NULL COMM,"
          + " INTEGER NOT NULL DEPTNO,"
          + " BOOLEAN NOT NULL SLACKER) NOT NULL";

  private static final String STR_AGG_REQUIRES_MONO =
      "Streaming aggregation requires at least one monotonic expression in GROUP BY clause";

  private static final String STR_ORDER_REQUIRES_MONO =
      "Streaming ORDER BY must start with monotonic expression";

  private static final String STR_SET_OP_INCONSISTENT =
      "Set operator cannot combine streaming and non-streaming inputs";

  private static final String ROW_RANGE_NOT_ALLOWED_WITH_RANK =
      "ROW/RANGE not allowed with RANK, DENSE_RANK or ROW_NUMBER functions";

  //~ Constructors -----------------------------------------------------------

  public SqlValidatorDynamicTest() {
    super();
  }

  @Override public SqlTester getTester() {
    // Dymamic schema should not be reused since it is mutable, so
    // we create new SqlTestFactory for each test
    return new SqlValidatorTester(SqlTestFactory.INSTANCE
        .withCatalogReader(MockCatalogReaderDynamic::new));
  }
//~ Methods ----------------------------------------------------------------

  @BeforeClass public static void setUSLocale() {
    // This ensures numbers in exceptions are printed as in asserts.
    // For example, 1,000 vs 1 000
    Locale.setDefault(Locale.US);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1150">[CALCITE-1150]
   * Dynamic Table / Dynamic Star support</a>. */
  @Test public void testAmbiguousDynamicStar() throws Exception {
    final String sql = "select ^n_nation^\n"
        + "from (select * from \"SALES\".NATION),\n"
        + " (select * from \"SALES\".CUSTOMER)";
    sql(sql).fails("Column 'N_NATION' is ambiguous");
  }

  @Test public void testAmbiguousDynamicStar2() throws Exception {
    final String sql = "select ^n_nation^\n"
        + "from (select * from \"SALES\".NATION, \"SALES\".CUSTOMER)";
    sql(sql).fails("Column 'N_NATION' is ambiguous");
  }

  @Test public void testAmbiguousDynamicStar3() throws Exception {
    final String sql = "select ^nc.n_nation^\n"
        + "from (select * from \"SALES\".NATION, \"SALES\".CUSTOMER) as nc";
    sql(sql).fails("Column 'N_NATION' is ambiguous");
  }

  @Test public void testAmbiguousDynamicStar4() throws Exception {
    final String sql = "select n.n_nation\n"
        + "from (select * from \"SALES\".NATION) as n,\n"
        + " (select * from \"SALES\".CUSTOMER)";
    sql(sql).type("RecordType(ANY N_NATION) NOT NULL");
  }

  /** When resolve column reference, regular field has higher priority than
   * dynamic star columns. */
  @Test public void testDynamicStar2() throws Exception {
    final String sql = "select newid from (\n"
        + "  select *, NATION.N_NATION + 100 as newid\n"
        + "  from \"SALES\".NATION, \"SALES\".CUSTOMER)";
    sql(sql).type("RecordType(ANY NEWID) NOT NULL");
  }

}

// End SqlValidatorDynamicTest.java
