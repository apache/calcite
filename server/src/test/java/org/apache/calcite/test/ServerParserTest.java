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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlCreateFunction;
import org.apache.calcite.sql.ddl.SqlCreateSchema;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlCreateType;
import org.apache.calcite.sql.ddl.SqlDropFunction;
import org.apache.calcite.sql.ddl.SqlDropMaterializedView;
import org.apache.calcite.sql.ddl.SqlDropSchema;
import org.apache.calcite.sql.ddl.SqlDropTable;
import org.apache.calcite.sql.ddl.SqlDropType;
import org.apache.calcite.sql.ddl.SqlDropView;
import org.apache.calcite.sql.ddl.SqlTruncateTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserFixture;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.SqlParserTest;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.sql.util.SqlShuttle;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;

import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertTrue;

import static java.util.Objects.requireNonNull;

/**
 * Tests SQL parser extensions for DDL.
 *
 * <p>Remaining tasks:
 * <ul>
 *
 * <li>"create table x (a int) as values 1, 2" should fail validation;
 * data type not allowed in "create table ... as".
 *
 * <li>"create table x (a int, b int as (a + 1)) stored"
 * should not allow b to be specified in insert;
 * should generate check constraint on b;
 * should populate b in insert as if it had a default
 *
 * <li>"create table as select" should store constraints
 * deduced by planner
 *
 * <li>during CREATE VIEW, check for a table and a materialized view
 * with the same name (they have the same namespace)
 *
 * </ul>
 */
class ServerParserTest extends SqlParserTest {

  @Override public SqlParserFixture fixture() {
    return super.fixture()
        .withConfig(c -> c.withParserFactory(SqlDdlParserImpl.FACTORY));
  }

  @Test void testCreateSchema() {
    sql("create schema x")
        .ok("CREATE SCHEMA `X`");
  }

  @Test void testProcessCreateTableWithDefault() {
    String sql = "create table tdef (i int not null, j int default 100)";
    String expected = "CREATE TABLE `TDEF` (`I` INTEGER NOT NULL,"
        + " `J` INTEGER DEFAULT 100)";
    sql(sql).ok(expected);
  }

  @Test void testCreateOrReplaceSchema() {
    sql("create or replace schema x")
        .ok("CREATE OR REPLACE SCHEMA `X`");
  }

  @Test void testCreateForeignSchema() {
    final String sql = "create or replace foreign schema x\n"
        + "type 'jdbc'\n"
        + "options (\n"
        + "  aBoolean true,\n"
        + "  anInteger -45,\n"
        + "  aDate DATE '1970-03-21',\n"
        + "  \"quoted.id\" TIMESTAMP '1970-03-21 12:4:56.78',\n"
        + "  aString 'foo''bar')";
    final String expected = "CREATE OR REPLACE FOREIGN SCHEMA `X` TYPE 'jdbc' "
        + "OPTIONS (`ABOOLEAN` TRUE,"
        + " `ANINTEGER` -45,"
        + " `ADATE` DATE '1970-03-21',"
        + " `quoted.id` TIMESTAMP '1970-03-21 12:4:56.78',"
        + " `ASTRING` 'foo''bar')";
    sql(sql).ok(expected);
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-7339">[CALCITE-7339]
   * Most classes in SqlDdlNodes use an incorrect SqlCallFactory</a>. */
  @Test void testShuttle() throws SqlParseException {
    // A shuttle which modified a SqlCall's position
    final SqlShuttle shuttle = new SqlShuttle() {
      @Override public @Nullable SqlNode visit(SqlCall call) {
        SqlNode newCall = super.visit(call);
        return requireNonNull(newCall, "newCall").clone(SqlParserPos.ZERO);
      }
    };

    BiConsumer<SqlParserFixture, Function<SqlNode, Boolean>> tester =
        (fixture, function) -> {
          SqlNode node = fixture.node();
          assertTrue(function.apply(node));
          SqlNode newNode = shuttle.visitNode(node);
          assertTrue(function.apply(newNode));
        };

    String sql = "CREATE TYPE T AS (x INT)";
    SqlParserFixture fixture = sql(sql);
    fixture.ok("CREATE TYPE `T` AS (`X` INTEGER)");
    tester.accept(fixture, n -> n instanceof SqlCreateType);

    // The following also checks SqlCheckConstraint, SqlColumnDeclaration, SqlAttributeDefinition
    sql = "CREATE TABLE X (I INTEGER NOT NULL, CONSTRAINT C1 CHECK (I < 10), J INTEGER)";
    fixture = sql(sql);
    fixture.ok("CREATE TABLE `X` (`I` INTEGER NOT NULL, "
        + "CONSTRAINT `C1` CHECK (`I` < 10), `J` INTEGER)");
    tester.accept(fixture, n -> n instanceof SqlCreateTable);

    sql = "CREATE FUNCTION F AS 'a.b'";
    fixture = sql(sql);
    fixture.ok("CREATE FUNCTION `F` AS 'a.b'");
    tester.accept(fixture, n -> n instanceof SqlCreateFunction);

    sql = "CREATE SCHEMA F";
    fixture = sql(sql);
    fixture.ok("CREATE SCHEMA `F`");
    tester.accept(fixture, n -> n instanceof SqlCreateSchema);

    sql = "DROP FUNCTION IF EXISTS F";
    fixture = sql(sql);
    fixture.ok("DROP FUNCTION IF EXISTS `F`");
    tester.accept(fixture, n -> n instanceof SqlDropFunction);

    sql = "DROP VIEW IF EXISTS V";
    fixture = sql(sql);
    fixture.ok("DROP VIEW IF EXISTS `V`");
    tester.accept(fixture, n -> n instanceof SqlDropView);

    sql = "DROP TABLE T";
    fixture = sql(sql);
    fixture.ok("DROP TABLE `T`");
    tester.accept(fixture, n -> n instanceof SqlDropTable);

    sql = "DROP SCHEMA IF EXISTS S";
    fixture = sql(sql);
    fixture.ok("DROP SCHEMA IF EXISTS `S`");
    tester.accept(fixture, n -> n instanceof SqlDropSchema);

    sql = "DROP TYPE IF EXISTS T";
    fixture = sql(sql);
    fixture.ok("DROP TYPE IF EXISTS `T`");
    tester.accept(fixture, n -> n instanceof SqlDropType);

    sql = "DROP MATERIALIZED VIEW IF EXISTS V";
    fixture = sql(sql);
    fixture.ok("DROP MATERIALIZED VIEW IF EXISTS `V`");
    tester.accept(fixture, n -> n instanceof SqlDropMaterializedView);

    sql = "TRUNCATE TABLE T CONTINUE IDENTITY";
    fixture = sql(sql);
    fixture.ok("TRUNCATE TABLE `T` CONTINUE IDENTITY");
    tester.accept(fixture, n -> n instanceof SqlTruncateTable);
  }

  @Test void testCreateForeignSchema2() {
    final String sql = "create or replace foreign schema x\n"
        + "library 'com.example.ExampleSchemaFactory'\n"
        + "options ()";
    final String expected = "CREATE OR REPLACE FOREIGN SCHEMA `X` "
        + "LIBRARY 'com.example.ExampleSchemaFactory' "
        + "OPTIONS ()";
    sql(sql).ok(expected);
  }

  @Test void testCreateTypeWithAttributeList() {
    sql("create type x.mytype1 as (i int not null, j varchar(5) null)")
        .ok("CREATE TYPE `X`.`MYTYPE1` AS (`I` INTEGER NOT NULL, `J` VARCHAR(5))");
  }

  @Test void testCreateTypeWithBaseType() {
    sql("create type mytype1 as varchar(5)")
        .ok("CREATE TYPE `MYTYPE1` AS VARCHAR(5)");
  }

  @Test void testCreateOrReplaceTypeWith() {
    sql("create or replace type mytype1 as varchar(5)")
        .ok("CREATE OR REPLACE TYPE `MYTYPE1` AS VARCHAR(5)");
  }

  @Test void testCreateTable() {
    sql("create table x (i int not null, j varchar(5) null)")
        .ok("CREATE TABLE `X` (`I` INTEGER NOT NULL, `J` VARCHAR(5))");
  }

  @Test void testCreateTableAsSelect() {
    final String expected = "CREATE TABLE `X` AS\n"
        + "SELECT *\n"
        + "FROM `EMP`";
    sql("create table x as select * from emp")
        .ok(expected);
  }

  @Test void testCreateTableIfNotExistsAsSelect() {
    final String expected = "CREATE TABLE IF NOT EXISTS `X`.`Y` AS\n"
        + "SELECT *\n"
        + "FROM `EMP`";
    sql("create table if not exists x.y as select * from emp")
        .ok(expected);
  }

  @Test void testCreateTableAsValues() {
    final String expected = "CREATE TABLE `X` AS\n"
        + "VALUES (ROW(1)),\n"
        + "(ROW(2))";
    sql("create table x as values 1, 2")
        .ok(expected);
  }

  @Test void testCreateTableAsSelectColumnList() {
    final String expected = "CREATE TABLE `X` (`A`, `B`) AS\n"
        + "SELECT *\n"
        + "FROM `EMP`";
    sql("create table x (a, b) as select * from emp")
        .ok(expected);
  }

  @Test void testCreateTableCheck() {
    final String expected = "CREATE TABLE `X` (`I` INTEGER NOT NULL,"
        + " CONSTRAINT `C1` CHECK (`I` < 10), `J` INTEGER)";
    sql("create table x (i int not null, constraint c1 check (i < 10), j int)")
        .ok(expected);
  }

  @Test void testCreateTableVirtualColumn() {
    final String sql = "create table if not exists x (\n"
        + " i int not null,\n"
        + " j int generated always as (i + 1) stored,\n"
        + " k int as (j + 1) virtual,\n"
        + " m int as (k + 1))";
    final String expected = "CREATE TABLE IF NOT EXISTS `X` "
        + "(`I` INTEGER NOT NULL,"
        + " `J` INTEGER AS (`I` + 1) STORED,"
        + " `K` INTEGER AS (`J` + 1) VIRTUAL,"
        + " `M` INTEGER AS (`K` + 1) VIRTUAL)";
    sql(sql).ok(expected);
  }

  @Test void testCreateTableWithUDT() {
    final String sql = "create table if not exists t (\n"
        + "  f0 MyType0 not null,\n"
        + "  f1 db_name.MyType1,\n"
        + "  f2 catalog_name.db_name.MyType2)";
    final String expected = "CREATE TABLE IF NOT EXISTS `T` ("
        + "`F0` `MYTYPE0` NOT NULL,"
        + " `F1` `DB_NAME`.`MYTYPE1`,"
        + " `F2` `CATALOG_NAME`.`DB_NAME`.`MYTYPE2`)";
    sql(sql).ok(expected);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6022">[CALCITE-6022]
   * Support "CREATE TABLE ... LIKE" DDL in server module</a>. */
  @Test void testCreateTableLike() {
    final String sql = "create table x like y";
    final String expected = "CREATE TABLE `X` LIKE `Y`";
    sql(sql).ok(expected);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6022">[CALCITE-6022]
   * Support "CREATE TABLE ... LIKE" DDL in server module</a>. */
  @Test void testCreateTableLikeWithOptions() {
    sql("create table x like y including all")
        .ok("CREATE TABLE `X` LIKE `Y`\n"
            + "INCLUDING ALL");

    sql("create table s.x like s.y excluding defaults including generated")
        .ok("CREATE TABLE `S`.`X` LIKE `S`.`Y`\n"
            + "INCLUDING GENERATED\n"
            + "EXCLUDING DEFAULTS");

    sql("create table x like y excluding defaults including all")
        .fails("ALL cannot be used with other options");

    sql("create table x like y including defaults excluding defaults")
        .fails("Cannot include and exclude option DEFAULTS at same time");

  }

  @Test void testCreateView() {
    final String sql = "create or replace view v as\n"
        + "select * from (values (1, '2'), (3, '45')) as t (x, y)";
    final String expected = "CREATE OR REPLACE VIEW `V` AS\n"
        + "SELECT *\n"
        + "FROM (VALUES (ROW(1, '2')),\n"
        + "(ROW(3, '45'))) AS `T` (`X`, `Y`)";
    sql(sql).ok(expected);
  }

  @Test void testCreateMaterializedView() {
    final String sql = "create materialized view mv (d, v) as\n"
        + "select deptno, count(*) from emp\n"
        + "group by deptno order by deptno desc";
    final String expected = "CREATE MATERIALIZED VIEW `MV` (`D`, `V`) AS\n"
        + "SELECT `DEPTNO`, COUNT(*)\n"
        + "FROM `EMP`\n"
        + "GROUP BY `DEPTNO`\n"
        + "ORDER BY `DEPTNO` DESC";
    sql(sql).ok(expected);
  }

  @Test void testCreateMaterializedView2() {
    final String sql = "create materialized view if not exists mv as\n"
        + "select deptno, count(*) from emp\n"
        + "group by deptno order by deptno desc";
    final String expected = "CREATE MATERIALIZED VIEW IF NOT EXISTS `MV` AS\n"
        + "SELECT `DEPTNO`, COUNT(*)\n"
        + "FROM `EMP`\n"
        + "GROUP BY `DEPTNO`\n"
        + "ORDER BY `DEPTNO` DESC";
    sql(sql).ok(expected);
  }

  // "OR REPLACE" is allowed by the parser, but the validator will give an
  // error later
  @Test void testCreateOrReplaceMaterializedView() {
    final String sql = "create or replace materialized view mv as\n"
        + "select * from emp";
    final String expected = "CREATE MATERIALIZED VIEW `MV` AS\n"
        + "SELECT *\n"
        + "FROM `EMP`";
    sql(sql).ok(expected);
  }

  @Test void testCreateOrReplaceFunction() {
    final String sql = "create or replace function if not exists x.udf\n"
        + " as 'org.apache.calcite.udf.TableFun.demoUdf'\n"
        + "using jar 'file:/path/udf/udf-0.0.1-SNAPSHOT.jar',\n"
        + " jar 'file:/path/udf/udf2-0.0.1-SNAPSHOT.jar',\n"
        + " file 'file:/path/udf/logback.xml'";
    final String expected = "CREATE OR REPLACE FUNCTION"
        + " IF NOT EXISTS `X`.`UDF`"
        + " AS 'org.apache.calcite.udf.TableFun.demoUdf'"
        + " USING JAR 'file:/path/udf/udf-0.0.1-SNAPSHOT.jar',"
        + " JAR 'file:/path/udf/udf2-0.0.1-SNAPSHOT.jar',"
        + " FILE 'file:/path/udf/logback.xml'";
    sql(sql).ok(expected);
  }

  @Test void testCreateOrReplaceFunction2() {
    final String sql = "create function \"my Udf\"\n"
        + " as 'org.apache.calcite.udf.TableFun.demoUdf'";
    final String expected = "CREATE FUNCTION `my Udf`"
        + " AS 'org.apache.calcite.udf.TableFun.demoUdf'";
    sql(sql).ok(expected);
  }

  @Test void testDropSchema() {
    sql("drop schema x")
        .ok("DROP SCHEMA `X`");
  }

  @Test void testDropSchemaIfExists() {
    sql("drop schema if exists x")
        .ok("DROP SCHEMA IF EXISTS `X`");
  }

  @Test void testDropForeignSchema() {
    sql("drop foreign schema x")
        .ok("DROP FOREIGN SCHEMA `X`");
  }

  @Test void testDropType() {
    sql("drop type X")
        .ok("DROP TYPE `X`");
  }

  @Test void testDropTypeIfExists() {
    sql("drop type if exists X")
        .ok("DROP TYPE IF EXISTS `X`");
  }

  @Test void testDropTypeTrailingIfExistsFails() {
    sql("drop type X ^if^ exists")
        .fails("(?s)Encountered \"if\" at.*");
  }

  @Test void testDropTable() {
    sql("drop table x")
        .ok("DROP TABLE `X`");
  }

  @Test void testDropTableComposite() {
    sql("drop table x.y")
        .ok("DROP TABLE `X`.`Y`");
  }

  @Test void testDropTableIfExists() {
    sql("drop table if exists x")
        .ok("DROP TABLE IF EXISTS `X`");
  }

  @Test void testTruncateTable() {
    sql("truncate table x")
        .ok("TRUNCATE TABLE `X` CONTINUE IDENTITY");

    sql("truncate table x continue identity")
        .ok("TRUNCATE TABLE `X` CONTINUE IDENTITY");

    sql("truncate table x restart identity")
        .ok("TRUNCATE TABLE `X` RESTART IDENTITY");
  }

  @Test void testDropView() {
    sql("drop view x")
        .ok("DROP VIEW `X`");
  }

  @Test void testDropMaterializedView() {
    sql("drop materialized view x")
        .ok("DROP MATERIALIZED VIEW `X`");
  }

  @Test void testDropMaterializedViewIfExists() {
    sql("drop materialized view if exists x")
        .ok("DROP MATERIALIZED VIEW IF EXISTS `X`");
  }

  @Test void testDropFunction() {
    final String sql = "drop function x.udf";
    final String expected = "DROP FUNCTION `X`.`UDF`";
    sql(sql).ok(expected);
  }

  @Test void testDropFunctionIfExists() {
    final String sql = "drop function if exists \"my udf\"";
    final String expected = "DROP FUNCTION IF EXISTS `my udf`";
    sql(sql).ok(expected);
  }

}
