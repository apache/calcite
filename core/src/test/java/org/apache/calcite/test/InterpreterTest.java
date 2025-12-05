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

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumUtils;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.interpreter.Interpreter;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.schema.impl.TableFunctionImpl;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.Smalls;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import static java.util.Objects.requireNonNull;

/**
 * Unit tests for {@link org.apache.calcite.interpreter.Interpreter}.
 */
class InterpreterTest {
  private @Nullable SchemaPlus rootSchema;

  /** Implementation of {@link DataContext} for executing queries without a
   * connection. */
  private static class MyDataContext implements DataContext {
    private final SchemaPlus rootSchema;
    private final JavaTypeFactory typeFactory;

    MyDataContext(SchemaPlus rootSchema, RelNode rel) {
      this.rootSchema = rootSchema;
      this.typeFactory = (JavaTypeFactory) rel.getCluster().getTypeFactory();
    }

    public SchemaPlus getRootSchema() {
      return rootSchema;
    }

    public JavaTypeFactory getTypeFactory() {
      return typeFactory;
    }

    public @Nullable QueryProvider getQueryProvider() {
      return null;
    }

    public @Nullable Object get(String name) {
      return null;
    }
  }

  /** Fluent class that contains information necessary to run a test. */
  private static class Sql {
    private final String sql;
    private final SchemaPlus rootSchema;
    private final boolean project;
    private final @Nullable Function<RelBuilder, RelNode> relFn;
    private final UnaryOperator<SqlToRelConverter.Config> sqlToRelTransform;

    Sql(String sql, SchemaPlus rootSchema, boolean project,
        @Nullable Function<RelBuilder, RelNode> relFn,
        UnaryOperator<SqlToRelConverter.Config> sqlToRelTransform) {
      this.sql = sql;
      this.rootSchema = rootSchema;
      this.project = project;
      this.relFn = relFn;
      this.sqlToRelTransform = sqlToRelTransform;
    }

    Sql withSql(String sql) {
      return new Sql(sql, rootSchema, project, relFn, sqlToRelTransform);
    }

    @SuppressWarnings("SameParameterValue")
    Sql withProject(boolean project) {
      return new Sql(sql, rootSchema, project, relFn, sqlToRelTransform);
    }

    Sql withRel(Function<RelBuilder, RelNode> relFn) {
      return new Sql(sql, rootSchema, project, relFn, sqlToRelTransform);
    }

    Sql withSqlToRel(UnaryOperator<SqlToRelConverter.Config> transform) {
      final UnaryOperator<SqlToRelConverter.Config> newTransform = c ->
          transform.apply(this.sqlToRelTransform.apply(c));
      return new Sql(sql, rootSchema, project, relFn, newTransform);
    }

    /** Interprets the sql and checks result with specified rows, ordered. */
    @SuppressWarnings("UnusedReturnValue")
    Sql returnsRows(String... rows) {
      return returnsRows(false, rows);
    }

    /** Interprets the sql and checks result with specified rows, unordered. */
    @SuppressWarnings("UnusedReturnValue")
    Sql returnsRowsUnordered(String... rows) {
      return returnsRows(true, rows);
    }

    private Planner createPlanner() {
      final FrameworkConfig config = Frameworks.newConfigBuilder()
          .parserConfig(SqlParser.Config.DEFAULT)
          .sqlToRelConverterConfig(
              sqlToRelTransform.apply(SqlToRelConverter.config()))
          .defaultSchema(
              CalciteAssert.addSchema(rootSchema,
                  CalciteAssert.SchemaSpec.JDBC_SCOTT,
                  CalciteAssert.SchemaSpec.HR))
          .build();
      return Frameworks.getPlanner(config);
    }

    /** Performs an action that requires a {@link RelBuilder}, and returns the
     * result. */
    private <T> T withRelBuilder(Function<RelBuilder, T> fn) {
      final FrameworkConfig config = Frameworks.newConfigBuilder()
          .defaultSchema(rootSchema)
          .build();
      final RelBuilder relBuilder = RelBuilder.create(config);
      return fn.apply(relBuilder);
    }

    /** Interprets the sql and checks result with specified rows. */
    private Sql returnsRows(boolean unordered, String[] rows) {
      try (Planner planner = createPlanner()) {
        final RelNode convert;
        if (relFn != null) {
          convert = withRelBuilder(relFn);
        } else {
          SqlNode parse = planner.parse(sql);
          SqlNode validate = planner.validate(parse);
          final RelRoot root = planner.rel(validate);
          convert = project ? root.project() : root.rel;
        }
        final MyDataContext dataContext =
            new MyDataContext(rootSchema, convert);
        assertInterpret(convert, dataContext, unordered, rows);
        return this;
      } catch (ValidationException
          | SqlParseException
          | RelConversionException e) {
        throw Util.throwAsRuntime(e);
      }
    }
  }

  /** Creates a {@link Sql}. */
  private Sql fixture() {
    return new Sql("?", rootSchema(), false, null, UnaryOperator.identity());
  }

  private SchemaPlus rootSchema() {
    return requireNonNull(rootSchema, "rootSchema");
  }

  private Sql sql(String sql) {
    return fixture().withSql(sql);
  }

  private void reset() {
    rootSchema = Frameworks.createRootSchema(true);
  }

  @BeforeEach public void setUp() {
    reset();
  }

  @AfterEach public void tearDown() {
    rootSchema = null;
  }

  /** Tests executing a simple plan using an interpreter. */
  @Test void testInterpretProjectFilterValues() {
    final String sql = "select y, x\n"
        + "from (values (1, 'a'), (2, 'b'), (3, 'c')) as t(x, y)\n"
        + "where x > 1";
    sql(sql).returnsRows("[b, 2]", "[c, 3]");
  }

  /** Tests NULLIF operator. (NULLIF is an example of an operator that
   * is implemented by expanding to simpler operators - in this case, CASE.) */
  @Test void testInterpretNullif() {
    final String sql = "select nullif(x, 2), x\n"
        + "from (values (1, 'a'), (2, 'b'), (3, 'c')) as t(x, y)";
    sql(sql).returnsRows("[1, 1]", "[null, 2]", "[3, 3]");
  }

  /** Tests a plan where the sort field is projected away. */
  @Test void testInterpretOrder() {
    final String sql = "select y\n"
        + "from (values (1, 'a'), (2, 'b'), (3, 'c')) as t(x, y)\n"
        + "order by -x";
    sql(sql).withProject(true).returnsRows("[c]", "[b]", "[a]");
  }

  @Test void testInterpretMultiset() {
    final String sql = "select multiset['a', 'b', 'c']";
    sql(sql).withProject(true).returnsRows("[[a, b, c]]");
  }

  private static void assertInterpret(RelNode rel, DataContext dataContext,
      boolean unordered, String... rows) {
    try (Interpreter interpreter = new Interpreter(dataContext, rel)) {
      final List<RelDataType> fieldTypes =
          Util.transform(rel.getRowType().getFieldList(),
              RelDataTypeField::getType);
      final Function<@Nullable Object[], List<@Nullable Object>> converter =
          EnumUtils.toExternal(fieldTypes, DateTimeUtils.DEFAULT_ZONE);
      assertRows(interpreter, converter, unordered, rows);
    }
  }

  private static void assertRows(Interpreter interpreter,
      Function<Object[], List<Object>> converter,
      boolean unordered, String... rows) {
    final List<String> list = new ArrayList<>();
    for (Object[] row : interpreter) {
      list.add(converter.apply(row).toString());
    }
    final List<String> expected = Arrays.asList(rows);
    if (unordered) {
      Collections.sort(list);
      Collections.sort(expected);
    }
    assertThat(list, equalTo(expected));
  }

  /** Tests executing a simple plan using an interpreter. */
  @Test void testInterpretTable() {
    sql("select * from \"hr\".\"emps\" order by \"empid\"")
        .returnsRows("[100, 10, Bill, 10000.0, 1000]",
            "[110, 10, Theodore, 11500.0, 250]",
            "[150, 10, Sebastian, 7000.0, null]",
            "[200, 20, Eric, 8000.0, 500]");
  }

  /** Tests executing a plan on a
   * {@link org.apache.calcite.schema.ScannableTable} using an interpreter. */
  @Test void testInterpretScannableTable() {
    rootSchema().add("beatles", new ScannableTableTest.BeatlesTable());
    sql("select * from \"beatles\" order by \"i\"")
        .returnsRows("[4, John, 1940]", "[4, Paul, 1942]", "[5, Ringo, 1940]",
            "[6, George, 1943]");
  }

  /** Tests executing a plan on a
   * {@link org.apache.calcite.schema.ScannableTable} using an interpreter. */
  @Test void testInterpretScannableTable2() {
    final AtomicInteger scanCount = new AtomicInteger();
    final AtomicInteger enumerateCount = new AtomicInteger();
    final AtomicInteger closeCount = new AtomicInteger();
    rootSchema().add("counting",
        ScannableTableTest.countingTable(scanCount, enumerateCount,
            closeCount));
    sql("select * from \"counting\" order by \"i\"")
        .returnsRows("[0]", "[10]", "[20]", "[30]");
    assertThat(scanCount.get(), is(1));
    assertThat(enumerateCount.get(), is(1));
    assertThat("close is called twice: on last fetch, and interpreter close",
        closeCount.get(), is(2));
  }

  @Test void testAggregateCount() {
    rootSchema().add("beatles", new ScannableTableTest.BeatlesTable());
    sql("select count(*) from \"beatles\"")
        .returnsRows("[4]");
  }

  @Test void testAggregateMax() {
    rootSchema().add("beatles", new ScannableTableTest.BeatlesTable());
    sql("select max(\"i\") from \"beatles\"")
        .returnsRows("[6]");
  }

  @Test void testAggregateMin() {
    rootSchema().add("beatles", new ScannableTableTest.BeatlesTable());
    sql("select min(\"i\") from \"beatles\"")
        .returnsRows("[4]");
  }

  @Test void testAggregateGroup() {
    rootSchema().add("beatles", new ScannableTableTest.BeatlesTable());
    sql("select \"j\", count(*) from \"beatles\" group by \"j\"")
        .returnsRowsUnordered("[George, 1]", "[Paul, 1]", "[John, 1]",
            "[Ringo, 1]");
  }

  @Test void testAggregateGroupFilter() {
    rootSchema().add("beatles", new ScannableTableTest.BeatlesTable());
    final String sql = "select \"j\",\n"
        + "  count(*) filter (where char_length(\"j\") > 4)\n"
        + "from \"beatles\" group by \"j\"";
    sql(sql)
        .returnsRowsUnordered("[George, 1]",
            "[Paul, 0]",
            "[John, 0]",
            "[Ringo, 1]");
  }

  /** Tests a GROUP BY query that uses
   * {@link org.apache.calcite.sql.fun.SqlInternalOperators#LITERAL_AGG}. */
  @Test void testAggregateLiteralAgg() {
    rootSchema().add("beatles", new ScannableTableTest.BeatlesTable());
    final Function<RelBuilder, RelNode> relFn =
        b -> b.scan("beatles")
            .aggregate(b.groupKey("k"),
                b.count().as("c"),
                b.literalAgg(true).as("t"),
                b.literalAgg(-3).as("minus3"))
            .build();
    fixture().withRel(relFn)
        .returnsRows("[1940, 2, true, -3]", "[1942, 1, true, -3]",
            "[1943, 1, true, -3]");
  }

  /** Tests executing a plan on a single-column
   * {@link org.apache.calcite.schema.ScannableTable} using an interpreter. */
  @Test void testInterpretSimpleScannableTable() {
    rootSchema().add("simple", new ScannableTableTest.SimpleTable());
    sql("select * from \"simple\" limit 2")
        .returnsRows("[0]", "[10]");
  }

  /** Tests executing a UNION ALL query using an interpreter. */
  @Test void testInterpretUnionAll() {
    rootSchema().add("simple", new ScannableTableTest.SimpleTable());
    final String sql = "select * from \"simple\"\n"
        + "union all\n"
        + "select * from \"simple\"";
    sql(sql).returnsRowsUnordered("[0]", "[10]", "[20]", "[30]", "[0]", "[10]",
        "[20]", "[30]");
  }

  /** Tests executing a UNION query using an interpreter. */
  @Test void testInterpretUnion() {
    rootSchema().add("simple", new ScannableTableTest.SimpleTable());
    final String sql = "select * from \"simple\"\n"
        + "union\n"
        + "select * from \"simple\"";
    sql(sql).returnsRowsUnordered("[0]", "[10]", "[20]", "[30]");
  }

  @Test void testInterpretUnionWithNullValue() {
    final String sql = "select * from\n"
        + "(select x, y from (values (cast(NULL as int), cast(NULL as varchar(1))),\n"
        + "(cast(NULL as int), cast(NULL as varchar(1)))) as t(x, y))\n"
        + "union\n"
        + "(select x, y from (values (cast(NULL as int), cast(NULL as varchar(1)))) as t2(x, y))";
    sql(sql).returnsRows("[null, null]");
  }

  @Test void testInterpretUnionAllWithNullValue() {
    final String sql = "select * from\n"
        + "(select x, y from (values (cast(NULL as int), cast(NULL as varchar(1))),\n"
        + "(cast(NULL as int), cast(NULL as varchar(1)))) as t(x, y))\n"
        + "union all\n"
        + "(select x, y from (values (cast(NULL as int), cast(NULL as varchar(1)))) as t2(x, y))";
    sql(sql).returnsRows("[null, null]", "[null, null]", "[null, null]");
  }

  @Test void testInterpretIntersect() {
    final String sql = "select * from\n"
        + "(select x, y from (values (1, 'a'), (1, 'a'), (2, 'b'), (3, 'c')) as t(x, y))\n"
        + "intersect\n"
        + "(select x, y from (values (1, 'a'), (2, 'c'), (4, 'x')) as t2(x, y))";
    sql(sql).returnsRows("[1, a]");
  }

  @Test void testInterpretIntersectAll() {
    final String sql = "select * from\n"
        + "(select x, y from (values (1, 'a'), (1, 'a'), (2, 'b'), (3, 'c')) as t(x, y))\n"
        + "intersect all\n"
        + "(select x, y from (values (1, 'a'), (2, 'c'), (4, 'x')) as t2(x, y))";
    sql(sql).returnsRows("[1, a]");
  }

  @Test void testInterpretIntersectWithNullValue() {
    final String sql = "select * from\n"
        + "(select x, y from (values (cast(NULL as int), cast(NULL as varchar(1))),\n"
        + " (cast(NULL as int), cast(NULL as varchar(1)))) as t(x, y))\n"
        + "intersect\n"
        + "(select x, y from (values (cast(NULL as int), cast(NULL as varchar(1)))) as t2(x, y))";
    sql(sql).returnsRows("[null, null]");
  }

  @Test void testInterpretIntersectAllWithNullValue() {
    final String sql = "select * from\n"
        + "(select x, y from (values (cast(NULL as int), cast(NULL as varchar(1))),\n"
        + " (cast(NULL as int), cast(NULL as varchar(1)))) as t(x, y))\n"
        + "intersect all\n"
        + "(select x, y from (values (cast(NULL as int), cast(NULL as varchar(1)))) as t2(x, y))";
    sql(sql).returnsRows("[null, null]");
  }

  @Test void testInterpretMinus() {
    final String sql = "select * from\n"
        + "(select x, y from (values (1, 'a'), (2, 'b'), (2, 'b'), (3, 'c')) as t(x, y))\n"
        + "except\n"
        + "(select x, y from (values (1, 'a'), (2, 'c'), (4, 'x')) as t2(x, y))";
    sql(sql).returnsRows("[2, b]", "[3, c]");
  }

  @Test void testDuplicateRowInterpretMinus() {
    final String sql = "select * from\n"
        + "(select x, y from (values (2, 'b'), (2, 'b')) as t(x, y))\n"
        + "except\n"
        + "(select x, y from (values (2, 'b')) as t2(x, y))";
    sql(sql).returnsRows();
  }

  @Test void testInterpretMinusAll() {
    final String sql = "select * from\n"
        + "(select x, y from (values (1, 'a'), (2, 'b'), (2, 'b'), (3, 'c')) as t(x, y))\n"
        + "except all\n"
        + "(select x, y from (values (1, 'a'), (2, 'c'), (4, 'x')) as t2(x, y))";
    sql(sql).returnsRows("[2, b]", "[2, b]", "[3, c]");
  }

  @Test void testDuplicateRowInterpretMinusAll() {
    final String sql = "select * from\n"
        + "(select x, y from (values (2, 'b'), (2, 'b')) as t(x, y))\n"
        + "except all\n"
        + "(select x, y from (values (2, 'b')) as t2(x, y))\n";
    sql(sql).returnsRows("[2, b]");
  }

  @Test void testInterpretMinusAllWithNullValue() {
    final String sql = "select * from\n"
        + "(select x, y from (values (cast(NULL as int), cast(NULL as varchar(1))),\n"
        + " (cast(NULL as int), cast(NULL as varchar(1)))) as t(x, y))\n"
        + "except all\n"
        + "(select x, y from (values (cast(NULL as int), cast(NULL as varchar(1)))) as t2(x, y))\n";
    sql(sql).returnsRows("[null, null]");
  }

  @Test void testInterpretMinusWithNullValue() {
    final String sql = "select * from\n"
        + "(select x, y from (values (cast(NULL as int), cast(NULL as varchar(1))),\n"
        + "(cast(NULL as int), cast(NULL as varchar(1)))) as t(x, y))\n"
        + "except\n"
        + "(select x, y from (values (cast(NULL as int), cast(NULL as varchar(1)))) as t2(x, y))\n";
    sql(sql).returnsRows();
  }

  @Test void testInterpretInnerJoin() {
    final String sql = "select * from\n"
        + "(select x, y from (values (1, 'a'), (2, 'b'), (3, 'c')) as t(x, y)) t\n"
        + "join\n"
        + "(select x, y from (values (1, 'd'), (2, 'c')) as t2(x, y)) t2\n"
        + "on t.x = t2.x";
    sql(sql).returnsRows("[1, a, 1, d]", "[2, b, 2, c]");
  }

  @Test void testInterpretLeftOutJoin() {
    final String sql = "select * from\n"
        + "(select x, y from (values (1, 'a'), (2, 'b'), (3, 'c')) as t(x, y)) t\n"
        + "left join\n"
        + "(select x, y from (values (1, 'd')) as t2(x, y)) t2\n"
        + "on t.x = t2.x";
    sql(sql).returnsRows("[1, a, 1, d]", "[2, b, null, null]", "[3, c, null, null]");
  }

  @Test void testInterpretRightOutJoin() {
    final String sql = "select * from\n"
        + "(select x, y from (values (1, 'd')) as t2(x, y)) t2\n"
        + "right join\n"
        + "(select x, y from (values (1, 'a'), (2, 'b'), (3, 'c')) as t(x, y)) t\n"
        + "on t2.x = t.x";
    sql(sql).returnsRows("[1, d, 1, a]", "[null, null, 2, b]", "[null, null, 3, c]");
  }

  @Test void testInterpretSemanticSemiJoin() {
    final String sql = "select x, y from (values (1, 'a'), (2, 'b'), (3, 'c')) as t(x, y)\n"
        + "where x in\n"
        + "(select x from (values (1, 'd'), (3, 'g')) as t2(x, y))";
    sql(sql).withSqlToRel(c -> c.withExpand(true))
        .returnsRows("[1, a]", "[3, c]");
  }

  @Test void testInterpretSemiJoin() {
    final String sql = "select x, y from (values (1, 'a'), (2, 'b'), (3, 'c')) as t(x, y)\n"
        + "where x in\n"
        + "(select x from (values (1, 'd'), (3, 'g')) as t2(x, y))";
    try (Planner planner =
             sql(sql).withSqlToRel(c -> c.withExpand(true)).createPlanner()) {
      SqlNode validate = planner.validate(planner.parse(sql));
      RelNode convert = planner.rel(validate).rel;
      final HepProgram program = new HepProgramBuilder()
          .addRuleInstance(CoreRules.PROJECT_TO_SEMI_JOIN)
          .build();
      final HepPlanner hepPlanner = new HepPlanner(program);
      hepPlanner.setRoot(convert);
      final RelNode relNode = hepPlanner.findBestExp();
      final MyDataContext dataContext =
          new MyDataContext(rootSchema(), relNode);
      assertInterpret(relNode, dataContext, true, "[1, a]", "[3, c]");
    } catch (ValidationException
        | SqlParseException
        | RelConversionException e) {
      throw Util.throwAsRuntime(e);
    }
  }

  @Test void testInterpretAntiJoin() {
    final String sql = "select x, y from (values (1, 'a'), (2, 'b'), (3, 'c')) as t(x, y)\n"
        + "where x not in\n"
        + "(select x from (values (1, 'd')) as t2(x, y))";
    sql(sql).withSqlToRel(c -> c.withExpand(true))
        .returnsRows("[2, b]", "[3, c]");
  }

  @Test void testInterpretFullJoin() {
    final String sql = "select * from\n"
        + "(select x, y from (values (1, 'a'), (2, 'b'), (3, 'c')) as t(x, y)) t\n"
        + "full join\n"
        + "(select x, y from (values (1, 'd'), (2, 'c'), (4, 'x')) as t2(x, y)) t2\n"
        + "on t.x = t2.x";
    sql(sql).returnsRows(
        "[1, a, 1, d]",
        "[2, b, 2, c]",
        "[3, c, null, null]",
        "[null, null, 4, x]");
  }

  @Test void testInterpretDecimalAggregate() {
    final String sql = "select x, min(y), max(y), sum(y), avg(y)\n"
        + "from (values ('a', -1.2), ('a', 2.3), ('a', 15)) as t(x, y)\n"
        + "group by x";
    sql(sql).returnsRows("[a, -1.2, 15.0, 16.1, 5.3]");
  }

  @Test void testInterpretUnnest() {
    sql("select * from unnest(array[1, 2])").returnsRows("[1]", "[2]");

    reset();
    sql("select * from unnest(multiset[1, 2])").returnsRowsUnordered("[1]", "[2]");

    reset();
    sql("select * from unnest(map['a', 12])").returnsRows("[a, 12]");

    reset();
    sql("select * from unnest(\n"
        + "select * from (values array[10, 20], array[30, 40]))\n"
        + "with ordinality as t(i, o)")
        .returnsRows("[10, 1]", "[20, 2]", "[30, 1]", "[40, 2]");

    reset();
    sql("select * from unnest(map['a', 12, 'b', 13]) with ordinality as t(a, b, o)")
        .returnsRows("[a, 12, 1]", "[b, 13, 2]");

    reset();
    sql("select * from unnest(\n"
        + "select * from (values multiset[10, 20], multiset[30, 40]))\n"
        + "with ordinality as t(i, o)")
        .returnsRows("[10, 1]", "[20, 2]", "[30, 1]", "[40, 2]");

    reset();
    sql("select * from unnest(array[cast(null as integer), 10])")
        .returnsRows("[null]", "[10]");

    reset();
    sql("select * from unnest(map[cast(null as integer), 10, 10, cast(null as integer)])")
        .returnsRowsUnordered("[null, 10]", "[10, null]");

    reset();
    sql("select * from unnest(multiset[cast(null as integer), 10])")
        .returnsRowsUnordered("[null]", "[10]");

    try {
      reset();
      sql("select * from unnest(cast(null as int array))").returnsRows("");
    } catch (NullPointerException e) {
      assertThat(e.getMessage(), equalTo("NULL value for unnest."));
    }
  }

  @Test void testInterpretJdbc() {
    sql("select empno, hiredate from jdbc_scott.emp")
        .returnsRows("[7369, 1980-12-17]", "[7499, 1981-02-20]",
            "[7521, 1981-02-22]", "[7566, 1981-02-04]", "[7654, 1981-09-28]",
            "[7698, 1981-01-05]", "[7782, 1981-06-09]", "[7788, 1987-04-19]",
            "[7839, 1981-11-17]", "[7844, 1981-09-08]", "[7876, 1987-05-23]",
            "[7900, 1981-12-03]", "[7902, 1981-12-03]", "[7934, 1982-01-23]");
  }

  /** Tests a user-defined scalar function that is non-static. */
  @Test void testInterpretFunction() {
    SchemaPlus schema = rootSchema().add("s", new AbstractSchema());
    final ScalarFunction f =
        ScalarFunctionImpl.create(Smalls.MY_PLUS_EVAL_METHOD);
    schema.add("myPlus", f);
    final String sql = "select x, \"s\".\"myPlus\"(x, 1)\n"
        + "from (values (2), (4), (7)) as t (x)";
    String[] rows = {"[2, 3]", "[4, 5]", "[7, 8]"};
    final int n = Smalls.MyPlusFunction.INSTANCE_COUNT.get().get();
    sql(sql).returnsRows(rows);
    final int n2 = Smalls.MyPlusFunction.INSTANCE_COUNT.get().get();
    assertThat(n2, is(n + 1)); // instantiated once per run, not once per row
  }

  /** Tests a user-defined scalar function that is non-static and has a
   * constructor that uses
   * {@link org.apache.calcite.schema.FunctionContext}. */
  @Test void testInterpretFunctionWithInitializer() {
    SchemaPlus schema = rootSchema().add("s", new AbstractSchema());
    final ScalarFunction f =
        ScalarFunctionImpl.create(Smalls.MY_PLUS_INIT_EVAL_METHOD);
    schema.add("myPlusInit", f);
    final String sql = "select x, \"s\".\"myPlusInit\"(x, 1)\n"
        + "from (values (2), (4), (7)) as t (x)";
    String[] rows = {"[2, 3]", "[4, 5]", "[7, 8]"};
    final int n = Smalls.MyPlusInitFunction.INSTANCE_COUNT.get().get();
    sql(sql).returnsRows(rows);
    final int n2 = Smalls.MyPlusInitFunction.INSTANCE_COUNT.get().get();
    assertThat(n2, is(n + 1)); // instantiated once per run, not once per row
    final String digest = Smalls.MyPlusInitFunction.THREAD_DIGEST.get();
    String expectedDigest = "parameterCount=2; "
        + "argument 0 is not constant; "
        + "argument 1 is constant and has value 1";
    assertThat(digest, is(expectedDigest));

    // Similar, but replace '1' with the expression '3 - 2'
    final String sql2 = "select x, \"s\".\"myPlusInit\"(x, 3 - 2)\n"
        + "from (values (2), (4), (7)) as t (x)";
    String[] rows2 = {"[2, 102]", "[4, 104]", "[7, 107]"};
    sql(sql2).returnsRows(rows2);
    final int n3 = Smalls.MyPlusInitFunction.INSTANCE_COUNT.get().get();
    assertThat(n3, is(n2 + 1)); // instantiated once per run, not once per row
    final String digest2 = Smalls.MyPlusInitFunction.THREAD_DIGEST.get();
    String expectedDigest2 = "parameterCount=2; "
        + "argument 0 is not constant; "
        + "argument 1 is not constant";
    assertThat(digest2, is(expectedDigest2));
  }

  /** Tests a table function. */
  @Test void testInterpretTableFunction() {
    assertThat(rootSchema, notNullValue());
    SchemaPlus schema = rootSchema.add("s", new AbstractSchema());
    final TableFunction table1 = TableFunctionImpl.create(Smalls.MAZE_METHOD);
    assertThat(table1, notNullValue());
    schema.add("Maze", table1);
    final String sql = "select *\n"
        + "from table(\"s\".\"Maze\"(5, 3, 1))";
    String[] rows = {"[abcde]", "[xyz]", "[generate(w=5, h=3, s=1)]"};
    sql(sql).returnsRows(rows);
  }

  /** Tests a table function that takes zero arguments.
   *
   * <p>Note that we use {@link Smalls#FIBONACCI_LIMIT_100_TABLE_METHOD}; if we
   * used {@link Smalls#FIBONACCI_TABLE_METHOD}, even with {@code LIMIT 6},
   * we would run out of memory, due to
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4478">[CALCITE-4478]
   * In interpreter, support infinite relations</a>. */
  @Test void testInterpretNilaryTableFunction() {
    SchemaPlus schema = rootSchema().add("s", new AbstractSchema());
    final TableFunction table1 =
        TableFunctionImpl.create(Smalls.FIBONACCI_LIMIT_100_TABLE_METHOD);
    assertThat(table1, notNullValue());
    schema.add("fibonacciLimit100", table1);
    final String sql = "select *\n"
        + "from table(\"s\".\"fibonacciLimit100\"())\n"
        + "limit 6";
    String[] rows = {"[1]", "[1]", "[2]", "[3]", "[5]", "[8]"};
    sql(sql).returnsRows(rows);
  }

  /** Tests a table function whose row type is determined by parsing a JSON
   * argument. */
  @Test void testInterpretTableFunctionWithDynamicType() {
    SchemaPlus schema = rootSchema().add("s", new AbstractSchema());
    final TableFunction table1 =
        TableFunctionImpl.create(Smalls.DYNAMIC_ROW_TYPE_TABLE_METHOD);
    assertThat(table1, notNullValue());
    schema.add("dynamicRowTypeTable", table1);
    final String sql = "select *\n"
        + "from table(\"s\".\"dynamicRowTypeTable\"('"
        + "{\"nullable\":false,\"fields\":["
        + "  {\"name\":\"i\",\"type\":\"INTEGER\",\"nullable\":false},"
        + "  {\"name\":\"d\",\"type\":\"DATE\",\"nullable\":true}"
        + "]}', 0))\n"
        + "where \"i\" < 0 and \"d\" is not null";
    sql(sql).returnsRows();
  }

  /** Tests a table function that is a non-static class. */
  @Test void testInterpretNonStaticTableFunction() {
    SchemaPlus schema = rootSchema().add("s", new AbstractSchema());
    final TableFunction tableFunction =
        requireNonNull(TableFunctionImpl.create(Smalls.MyTableFunction.class));
    schema.add("t", tableFunction);
    final String sql = "select *\n"
        + "from table(\"s\".\"t\"('=100='))";
    sql(sql).returnsRows("[1]", "[3]", "[100]");
  }

  /** Tests projecting zero fields. */
  @Test void testZeroFields() {
    final List<RexLiteral> row = ImmutableList.of();
    final Function<RelBuilder, RelNode> relFn =
        b -> b.values(ImmutableList.of(row, row),
                b.getTypeFactory().builder().build())
            .build();
    fixture().withRel(relFn)
        .returnsRows("[]", "[]");
  }
}
