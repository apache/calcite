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
package org.eigenbase.test;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.sql2rel.*;
import org.eigenbase.util.*;

import net.hydromatic.linq4j.expressions.Expression;

import net.hydromatic.optiq.prepare.Prepare;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import static org.junit.Assert.*;

/**
 * SqlToRelTestBase is an abstract base for tests which involve conversion from
 * SQL to relational algebra.
 *
 * <p>SQL statements to be translated can use the schema defined in {@link
 * MockCatalogReader}; note that this is slightly different from Farrago's SALES
 * schema. If you get a parser or validator error from your test SQL, look down
 * in the stack until you see "Caused by", which will usually tell you the real
 * error.
 */
public abstract class SqlToRelTestBase {
  //~ Static fields/initializers ---------------------------------------------

  protected static final String NL = System.getProperty("line.separator");

  //~ Instance fields --------------------------------------------------------

  protected final Tester tester = createTester();

  //~ Methods ----------------------------------------------------------------

  public SqlToRelTestBase() {
    super();
  }

  protected Tester createTester() {
    return new TesterImpl(getDiffRepos(), true, false, null);
  }

  /**
   * Returns the default diff repository for this test, or null if there is
   * no repository.
   *
   * <p>The default implementation returns null.
   *
   * <p>Sub-classes that want to use a diff repository can override.
   * Sub-sub-classes can override again, inheriting test cases and overriding
   * selected test results.
   *
   * <p>And individual test cases can override by providing a different
   * tester object.
   *
   * @return Diff repository
   */
  protected DiffRepository getDiffRepos() {
    return null;
  }

  //~ Inner Interfaces -------------------------------------------------------

  /**
   * Helper class which contains default implementations of methods used for
   * running sql-to-rel conversion tests.
   */
  public interface Tester {
    /**
     * Converts a SQL string to a {@link RelNode} tree.
     *
     * @param sql SQL statement
     * @return Relational expression, never null
     */
    RelNode convertSqlToRel(String sql);

    SqlNode parseQuery(String sql) throws Exception;

    /**
     * Factory method to create a {@link SqlValidator}.
     */
    SqlValidator createValidator(
        SqlValidatorCatalogReader catalogReader,
        RelDataTypeFactory typeFactory);

    /**
     * Factory method for a
     * {@link net.hydromatic.optiq.prepare.Prepare.CatalogReader}.
     */
    Prepare.CatalogReader createCatalogReader(
        RelDataTypeFactory typeFactory);

    RelOptPlanner createPlanner();

    /**
     * Returns the {@link SqlOperatorTable} to use.
     */
    SqlOperatorTable getOperatorTable();

    /**
     * Returns the SQL dialect to test.
     */
    SqlConformance getConformance();

    /**
     * Checks that a SQL statement converts to a given plan.
     *
     * @param sql  SQL query
     * @param plan Expected plan
     */
    void assertConvertsTo(
        String sql,
        String plan);

    /**
     * Checks that a SQL statement converts to a given plan, optionally
     * trimming columns that are not needed.
     *
     * @param sql  SQL query
     * @param plan Expected plan
     * @param trim Whether to trim columns that are not needed
     */
    void assertConvertsTo(
        String sql,
        String plan,
        boolean trim);

    /**
     * Returns the diff repository.
     *
     * @return Diff repository
     */
    DiffRepository getDiffRepos();

    /**
     * Returns the validator.
     *
     * @return Validator
     */
    SqlValidator getValidator();

    /** Returns a tester that optionally decorrelates queries. */
    Tester withDecorrelation(boolean enable);

    Tester withCatalogReaderFactory(
        Function<RelDataTypeFactory, Prepare.CatalogReader> factory);

    /** Returns a tester that optionally trims unused fields. */
    Tester withTrim(boolean enable);
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Mock implementation of {@link RelOptSchema}.
   */
  protected static class MockRelOptSchema implements RelOptSchemaWithSampling {
    private final SqlValidatorCatalogReader catalogReader;
    private final RelDataTypeFactory typeFactory;

    public MockRelOptSchema(
        SqlValidatorCatalogReader catalogReader,
        RelDataTypeFactory typeFactory) {
      this.catalogReader = catalogReader;
      this.typeFactory = typeFactory;
    }

    public RelOptTable getTableForMember(List<String> names) {
      final SqlValidatorTable table = catalogReader.getTable(names);
      final RelDataType rowType = table.getRowType();
      final List<RelCollation> collationList = deduceMonotonicity(table);
      if (names.size() < 3) {
        String[] newNames2 = {"CATALOG", "SALES", ""};
        List<String> newNames = new ArrayList<String>();
        int i = 0;
        while (newNames.size() < newNames2.length) {
          newNames.add(i, newNames2[i]);
          ++i;
        }
        names = newNames;
      }
      return createColumnSet(table, names, rowType, collationList);
    }

    private List<RelCollation> deduceMonotonicity(SqlValidatorTable table) {
      final RelDataType rowType = table.getRowType();
      final List<RelCollation> collationList =
          new ArrayList<RelCollation>();

      // Deduce which fields the table is sorted on.
      int i = -1;
      for (RelDataTypeField field : rowType.getFieldList()) {
        ++i;
        final SqlMonotonicity monotonicity =
            table.getMonotonicity(field.getName());
        if (monotonicity != SqlMonotonicity.NOT_MONOTONIC) {
          final RelFieldCollation.Direction direction =
              monotonicity.isDecreasing()
                  ? RelFieldCollation.Direction.DESCENDING
                  : RelFieldCollation.Direction.ASCENDING;
          collationList.add(
              RelCollationImpl.of(
                  new RelFieldCollation(
                      i,
                      direction,
                      RelFieldCollation.NullDirection.UNSPECIFIED)));
        }
      }
      return collationList;
    }

    public RelOptTable getTableForMember(
        List<String> names,
        final String datasetName,
        boolean[] usedDataset) {
      final RelOptTable table = getTableForMember(names);

      // If they're asking for a sample, just for test purposes,
      // assume there's a table called "<table>:<sample>".
      RelOptTable datasetTable =
          new DelegatingRelOptTable(table) {
            public List<String> getQualifiedName() {
              final List<String> list =
                  new ArrayList<String>(super.getQualifiedName());
              list.set(
                  list.size() - 1,
                  list.get(list.size() - 1) + ":" + datasetName);
              return ImmutableList.copyOf(list);
            }
          };
      if (usedDataset != null) {
        assert usedDataset.length == 1;
        usedDataset[0] = true;
      }
      return datasetTable;
    }

    protected MockColumnSet createColumnSet(
        SqlValidatorTable table,
        List<String> names,
        final RelDataType rowType,
        final List<RelCollation> collationList) {
      return new MockColumnSet(names, rowType, collationList);
    }

    public RelDataTypeFactory getTypeFactory() {
      return typeFactory;
    }

    public void registerRules(RelOptPlanner planner) throws Exception {
    }

    protected class MockColumnSet implements RelOptTable {
      private final List<String> names;
      private final RelDataType rowType;
      private final List<RelCollation> collationList;

      protected MockColumnSet(
          List<String> names,
          RelDataType rowType,
          final List<RelCollation> collationList) {
        this.names = ImmutableList.copyOf(names);
        this.rowType = rowType;
        this.collationList = collationList;
      }

      public <T> T unwrap(Class<T> clazz) {
        if (clazz.isInstance(this)) {
          return clazz.cast(this);
        }
        return null;
      }

      public List<String> getQualifiedName() {
        return names;
      }

      public double getRowCount() {
        // use something other than 0 to give costing tests
        // some room, and make emps bigger than depts for
        // join asymmetry
        if (Iterables.getLast(names).equals("EMP")) {
          return 1000;
        } else {
          return 100;
        }
      }

      public RelDataType getRowType() {
        return rowType;
      }

      public RelOptSchema getRelOptSchema() {
        return MockRelOptSchema.this;
      }

      public RelNode toRel(
          ToRelContext context) {
        return new TableAccessRel(context.getCluster(), this);
      }

      public List<RelCollation> getCollationList() {
        return collationList;
      }

      public boolean isKey(BitSet columns) {
        return false;
      }

      public Expression getExpression(Class clazz) {
        return null;
      }
    }
  }

  private static class DelegatingRelOptTable implements RelOptTable {
    private final RelOptTable parent;

    public DelegatingRelOptTable(RelOptTable parent) {
      this.parent = parent;
    }

    public <T> T unwrap(Class<T> clazz) {
      if (clazz.isInstance(this)) {
        return clazz.cast(this);
      }
      return parent.unwrap(clazz);
    }

    public Expression getExpression(Class clazz) {
      return parent.getExpression(clazz);
    }

    public List<String> getQualifiedName() {
      return parent.getQualifiedName();
    }

    public double getRowCount() {
      return parent.getRowCount();
    }

    public RelDataType getRowType() {
      return parent.getRowType();
    }

    public RelOptSchema getRelOptSchema() {
      return parent.getRelOptSchema();
    }

    public RelNode toRel(ToRelContext context) {
      return new TableAccessRel(context.getCluster(), this);
    }

    public List<RelCollation> getCollationList() {
      return parent.getCollationList();
    }

    public boolean isKey(BitSet columns) {
      return parent.isKey(columns);
    }
  }

  /**
   * Default implementation of {@link Tester}, using mock classes {@link
   * MockRelOptSchema} and {@link MockRelOptPlanner}.
   */
  public static class TesterImpl implements Tester {
    private RelOptPlanner planner;
    private SqlOperatorTable opTab;
    private final DiffRepository diffRepos;
    private final boolean enableDecorrelate;
    private final boolean enableTrim;
    private final Function<RelDataTypeFactory, Prepare.CatalogReader>
    catalogReaderFactory;
    private RelDataTypeFactory typeFactory;

    /**
     * Creates a TesterImpl.
     *
     * @param diffRepos Diff repository
     * @param enableDecorrelate Whether to decorrelate
     * @param enableTrim Whether to trim unused fields
     * @param catalogReaderFactory Function to create catalog reader, or null
     */
    protected TesterImpl(DiffRepository diffRepos, boolean enableDecorrelate,
        boolean enableTrim,
        Function<RelDataTypeFactory, Prepare.CatalogReader>
            catalogReaderFactory) {
      this.diffRepos = diffRepos;
      this.enableDecorrelate = enableDecorrelate;
      this.enableTrim = enableTrim;
      this.catalogReaderFactory = catalogReaderFactory;
    }

    public RelNode convertSqlToRel(String sql) {
      Util.pre(sql != null, "sql != null");
      final SqlNode sqlQuery;
      try {
        sqlQuery = parseQuery(sql);
      } catch (Exception e) {
        throw Util.newInternal(e); // todo: better handling
      }
      final RelDataTypeFactory typeFactory = getTypeFactory();
      final Prepare.CatalogReader catalogReader =
          createCatalogReader(typeFactory);
      final SqlValidator validator =
          createValidator(
              catalogReader, typeFactory);
      final SqlToRelConverter converter =
          createSqlToRelConverter(
              validator,
              catalogReader,
              typeFactory);
      converter.setTrimUnusedFields(true);
      final SqlNode validatedQuery = validator.validate(sqlQuery);
      RelNode rel =
          converter.convertQuery(validatedQuery, false, true);
      assert rel != null;
      if (enableDecorrelate || enableTrim) {
        rel = converter.flattenTypes(rel, true);
      }
      if (enableDecorrelate) {
        rel = converter.decorrelate(sqlQuery, rel);
      }
      if (enableTrim) {
        converter.setTrimUnusedFields(true);
        rel = converter.trimUnusedFields(rel);
      }
      return rel;
    }

    protected SqlToRelConverter createSqlToRelConverter(
        final SqlValidator validator,
        final Prepare.CatalogReader catalogReader,
        final RelDataTypeFactory typeFactory) {
      return
          new SqlToRelConverter(
              null,
              validator,
              catalogReader,
              getPlanner(),
              new RexBuilder(typeFactory),
              StandardConvertletTable.INSTANCE);
    }

    protected final RelDataTypeFactory getTypeFactory() {
      if (typeFactory == null) {
        typeFactory = createTypeFactory();
      }
      return typeFactory;
    }

    protected RelDataTypeFactory createTypeFactory() {
      return new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    }

    protected final RelOptPlanner getPlanner() {
      if (planner == null) {
        planner = createPlanner();
      }
      return planner;
    }

    public SqlNode parseQuery(String sql) throws Exception {
      SqlParser parser = SqlParser.create(sql);
      SqlNode sqlNode = parser.parseQuery();
      return sqlNode;
    }

    public SqlConformance getConformance() {
      return SqlConformance.DEFAULT;
    }

    public SqlValidator createValidator(
        SqlValidatorCatalogReader catalogReader,
        RelDataTypeFactory typeFactory) {
      return new FarragoTestValidator(
          getOperatorTable(),
          createCatalogReader(typeFactory),
          typeFactory,
          getConformance());
    }

    public final SqlOperatorTable getOperatorTable() {
      if (opTab == null) {
        opTab = createOperatorTable();
      }
      return opTab;
    }

    /**
     * Creates an operator table.
     *
     * @return New operator table
     */
    protected SqlOperatorTable createOperatorTable() {
      final MockSqlOperatorTable opTab =
          new MockSqlOperatorTable(SqlStdOperatorTable.instance());
      MockSqlOperatorTable.addRamp(opTab);
      return opTab;
    }

    public Prepare.CatalogReader createCatalogReader(
        RelDataTypeFactory typeFactory) {
      if (this.catalogReaderFactory != null) {
        return catalogReaderFactory.apply(typeFactory);
      }
      return new MockCatalogReader(typeFactory, true).init();
    }

    public RelOptPlanner createPlanner() {
      return new MockRelOptPlanner();
    }

    public void assertConvertsTo(
        String sql,
        String plan) {
      assertConvertsTo(sql, plan, false);
    }

    public void assertConvertsTo(
        String sql,
        String plan,
        boolean trim) {
      String sql2 = getDiffRepos().expand("sql", sql);
      RelNode rel = convertSqlToRel(sql2);

      assertTrue(rel != null);
      assertValid(rel);

      if (trim) {
        final RelFieldTrimmer trimmer = createFieldTrimmer();
        rel = trimmer.trim(rel);
        assertTrue(rel != null);
        assertValid(rel);
      }

      // NOTE jvs 28-Mar-2006:  insert leading newline so
      // that plans come out nicely stacked instead of first
      // line immediately after CDATA start
      String actual = NL + RelOptUtil.toString(rel);
      diffRepos.assertEquals("plan", plan, actual);
    }

    /**
     * Creates a RelFieldTrimmer.
     *
     * @return Field trimmer
     */
    public RelFieldTrimmer createFieldTrimmer() {
      return new RelFieldTrimmer(getValidator());
    }

    /**
     * Checks that every node of a relational expression is valid.
     *
     * @param rel Relational expression
     */
    protected void assertValid(RelNode rel) {
      SqlToRelConverterTest.RelValidityChecker checker =
          new SqlToRelConverterTest.RelValidityChecker();
      checker.go(rel);
      assertEquals(0, checker.invalidCount);
    }

    public DiffRepository getDiffRepos() {
      return diffRepos;
    }

    public SqlValidator getValidator() {
      final RelDataTypeFactory typeFactory = getTypeFactory();
      final SqlValidatorCatalogReader catalogReader =
          createCatalogReader(typeFactory);
      return createValidator(catalogReader, typeFactory);
    }

    public TesterImpl withDecorrelation(boolean enable) {
      return this.enableDecorrelate == enable ? this
          : new TesterImpl(diffRepos, enable, enableTrim, catalogReaderFactory);
    }

    public Tester withTrim(boolean enable) {
      return this.enableTrim == enable ? this
          : new TesterImpl(diffRepos, enableDecorrelate, enable,
              catalogReaderFactory);
    }

    public Tester withCatalogReaderFactory(
        Function<RelDataTypeFactory, Prepare.CatalogReader> factory) {
      return new TesterImpl(diffRepos, enableDecorrelate, false, factory);
    }
  }

  private static class FarragoTestValidator extends SqlValidatorImpl {
    public FarragoTestValidator(
        SqlOperatorTable opTab,
        SqlValidatorCatalogReader catalogReader,
        RelDataTypeFactory typeFactory,
        SqlConformance conformance) {
      super(opTab, catalogReader, typeFactory, conformance);
    }

    // override SqlValidator
    public boolean shouldExpandIdentifiers() {
      return true;
    }
  }
}

// End SqlToRelTestBase.java
