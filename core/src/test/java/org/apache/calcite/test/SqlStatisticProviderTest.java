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

import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.materialize.SqlStatisticProvider;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.statistic.CachingSqlStatisticProvider;
import org.apache.calcite.statistic.MapSqlStatisticProvider;
import org.apache.calcite.statistic.QuerySqlStatisticProvider;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Util;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Unit test for {@link org.apache.calcite.materialize.SqlStatisticProvider}
 * and implementations of it.
 */
public class SqlStatisticProviderTest {
  /** Creates a config based on the "foodmart" schema. */
  public static Frameworks.ConfigBuilder config() {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(
            CalciteAssert.addSchema(rootSchema,
                CalciteAssert.SchemaSpec.JDBC_FOODMART))
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
  }

  @Test public void testMapProvider() {
    check(MapSqlStatisticProvider.INSTANCE);
  }

  @Test public void testQueryProvider() {
    final boolean debug = CalciteSystemProperty.DEBUG.value();
    final Consumer<String> sqlConsumer =
        debug ? System.out::println : Util::discard;
    check(new QuerySqlStatisticProvider(sqlConsumer));
  }

  @Test public void testQueryProviderWithCache() {
    Cache<List, Object> cache = CacheBuilder.newBuilder()
        .expireAfterAccess(5, TimeUnit.MINUTES)
        .build();
    final AtomicInteger counter = new AtomicInteger();
    QuerySqlStatisticProvider provider =
        new QuerySqlStatisticProvider(sql -> counter.incrementAndGet());
    final SqlStatisticProvider cachingProvider =
        new CachingSqlStatisticProvider(provider, cache);
    check(cachingProvider);
    final int expectedQueryCount = 6;
    assertThat(counter.get(), is(expectedQueryCount));
    check(cachingProvider);
    assertThat(counter.get(), is(expectedQueryCount)); // no more queries
  }

  private void check(SqlStatisticProvider provider) {
    final RelBuilder relBuilder = RelBuilder.create(config().build());
    final RelNode productScan = relBuilder.scan("product").build();
    final RelOptTable productTable = productScan.getTable();
    final RelNode salesScan = relBuilder.scan("sales_fact_1997").build();
    final RelOptTable salesTable = salesScan.getTable();
    final RelNode employeeScan = relBuilder.scan("employee").build();
    final RelOptTable employeeTable = employeeScan.getTable();
    assertThat(provider.tableCardinality(productTable), is(1_560.0d));
    assertThat(
        provider.isKey(productTable, columns(productTable, "product_id")),
        is(true));
    assertThat(
        provider.isKey(salesTable, columns(salesTable, "product_id")),
        is(false));
    assertThat(
        provider.isForeignKey(salesTable, columns(salesTable, "product_id"),
            productTable, columns(productTable, "product_id")),
        is(true));
    // Not a foreign key; product has some ids that are not referenced by any
    // sale
    assertThat(
        provider.isForeignKey(
            productTable, columns(productTable, "product_id"),
            salesTable, columns(salesTable, "product_id")),
        is(false));
    // There is one supervisor_id, 0, which is not an employee_id
    assertThat(
        provider.isForeignKey(
            employeeTable, columns(employeeTable, "supervisor_id"),
            employeeTable, columns(employeeTable, "employee_id")),
        is(false));
  }

  private List<Integer> columns(RelOptTable table, String... columnNames) {
    return Arrays.stream(columnNames)
        .map(columnName ->
            table.getRowType().getFieldNames().indexOf(columnName))
        .collect(Collectors.toList());
  }
}

// End SqlStatisticProviderTest.java
