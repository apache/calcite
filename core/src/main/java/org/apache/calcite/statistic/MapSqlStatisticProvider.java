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
package org.apache.calcite.statistic;

import org.apache.calcite.adapter.jdbc.JdbcTable;
import org.apache.calcite.materialize.SqlStatisticProvider;
import org.apache.calcite.plan.RelOptTable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Implementation of {@link SqlStatisticProvider} that looks up values in a
 * table.
 *
 * <p>Only for testing.
 */
public enum MapSqlStatisticProvider implements SqlStatisticProvider {
  INSTANCE;

  private final ImmutableMap<String, Double> cardinalityMap;

  private final ImmutableMultimap<String, ImmutableList<String>> keyMap;

  MapSqlStatisticProvider() {
    final Initializer initializer = new Initializer()
        .put("foodmart", "agg_c_14_sales_fact_1997", 86_805, "id")
        .put("foodmart", "account", 11, "account_id")
        .put("foodmart", "category", 4, "category_id")
        .put("foodmart", "currency", 10_281, "currency_id")
        .put("foodmart", "customer", 10_281, "customer_id")
        .put("foodmart", "days", 7, "day")
        .put("foodmart", "employee", 1_155, "employee_id")
        .put("foodmart", "employee_closure", 7_179)
        .put("foodmart", "department", 10_281, "department_id")
        .put("foodmart", "inventory_fact_1997", 4_070)
        .put("foodmart", "position", 18, "position_id")
        .put("foodmart", "product", 1560, "product_id")
        .put("foodmart", "product_class", 110, "product_class_id")
        .put("foodmart", "promotion", 1_864, "promotion_id")
        // region really has 110 rows; made it smaller than store to trick FK
        .put("foodmart", "region", 24, "region_id")
        .put("foodmart", "salary", 21_252)
        .put("foodmart", "sales_fact_1997", 86_837)
        .put("foodmart", "store", 25, "store_id")
        .put("foodmart", "store_ragged", 25, "store_id")
        .put("foodmart", "time_by_day", 730, "time_id", "the_date") // 2 keys
        .put("foodmart", "warehouse", 24, "warehouse_id")
        .put("foodmart", "warehouse_class", 6, "warehouse_class_id")
        .put("scott", "EMP", 10, "EMPNO")
        .put("scott", "DEPT", 4, "DEPTNO")
        .put("tpcds", "CALL_CENTER", 8, "id")
        .put("tpcds", "CATALOG_PAGE", 11_718, "id")
        .put("tpcds", "CATALOG_RETURNS", 144_067, "id")
        .put("tpcds", "CATALOG_SALES", 1_441_548, "id")
        .put("tpcds", "CUSTOMER", 100_000, "id")
        .put("tpcds", "CUSTOMER_ADDRESS", 50_000, "id")
        .put("tpcds", "CUSTOMER_DEMOGRAPHICS", 1_920_800, "id")
        .put("tpcds", "DATE_DIM", 73049, "id")
        .put("tpcds", "DBGEN_VERSION", 1, "id")
        .put("tpcds", "HOUSEHOLD_DEMOGRAPHICS", 7200, "id")
        .put("tpcds", "INCOME_BAND", 20, "id")
        .put("tpcds", "INVENTORY", 11_745_000, "id")
        .put("tpcds", "ITEM", 18_000, "id")
        .put("tpcds", "PROMOTION", 300, "id")
        .put("tpcds", "REASON", 35, "id")
        .put("tpcds", "SHIP_MODE", 20, "id")
        .put("tpcds", "STORE", 12, "id")
        .put("tpcds", "STORE_RETURNS", 287_514, "id")
        .put("tpcds", "STORE_SALES", 2_880_404, "id")
        .put("tpcds", "TIME_DIM", 86_400, "id")
        .put("tpcds", "WAREHOUSE", 5, "id")
        .put("tpcds", "WEB_PAGE", 60, "id")
        .put("tpcds", "WEB_RETURNS", 71_763, "id")
        .put("tpcds", "WEB_SALES", 719_384, "id")
        .put("tpcds", "WEB_SITE", 1, "id");
    cardinalityMap = initializer.cardinalityMapBuilder.build();
    keyMap = initializer.keyMapBuilder.build();
  }

  @Override public double tableCardinality(RelOptTable table) {
    final List<String> qualifiedName =
        table.maybeUnwrap(JdbcTable.class)
            .map(value ->
                Arrays.asList(value.jdbcSchemaName, value.jdbcTableName))
            .orElseGet(table::getQualifiedName);
    return requireNonNull(cardinalityMap.get(qualifiedName.toString()));
  }

  @Override public boolean isForeignKey(RelOptTable fromTable, List<Integer> fromColumns,
      RelOptTable toTable, List<Integer> toColumns) {
    // Assume that anything that references a primary key is a foreign key.
    // It's wrong but it's enough for our current test cases.
    return isKey(toTable, toColumns)
        // supervisor_id contains one 0 value, which does not match any
        // employee_id, therefore it is not a foreign key
        && !"[foodmart, employee].[supervisor_id]"
            .equals(fromTable.getQualifiedName() + "."
                + columnNames(fromTable, fromColumns));
  }

  @Override public boolean isKey(RelOptTable table, List<Integer> columns) {
    // In order to match, all column ordinals must be in range 0 .. columnCount
    return columns.stream().allMatch(columnOrdinal ->
        (columnOrdinal >= 0)
            && (columnOrdinal < table.getRowType().getFieldCount()))
        // ... and the column names match the name of the primary key
        && keyMap.get(table.getQualifiedName().toString())
            .contains(columnNames(table, columns));
  }

  private static List<String> columnNames(RelOptTable table, List<Integer> columns) {
    return columns.stream()
        .map(columnOrdinal -> table.getRowType().getFieldNames()
            .get(columnOrdinal))
        .collect(Collectors.toList());
  }

  /** Helper during construction. */
  private static class Initializer {
    final ImmutableMap.Builder<String, Double> cardinalityMapBuilder =
        ImmutableMap.builder();
    final ImmutableMultimap.Builder<String, ImmutableList<String>> keyMapBuilder =
        ImmutableMultimap.builder();

    Initializer put(String schema, String table, int count, Object... keys) {
      String qualifiedName = Arrays.asList(schema, table).toString();
      cardinalityMapBuilder.put(qualifiedName, (double) count);
      for (Object key : keys) {
        final ImmutableList<String> keyList;
        if (key instanceof String) {
          keyList = ImmutableList.of((String) key);
        } else if (key instanceof String[]) {
          keyList = ImmutableList.copyOf((String[]) key); // composite key
        } else {
          throw new AssertionError("unknown key " + key);
        }
        keyMapBuilder.put(qualifiedName, keyList);
      }
      return this;
    }
  }
}
