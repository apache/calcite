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
package org.apache.calcite.materialize;

import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link SqlStatisticProvider} that looks up values in a
 * table.
 *
 * <p>Only for testing.
 */
public enum MapSqlStatisticProvider implements SqlStatisticProvider {
  INSTANCE;

  private static final Map<String, Double> CARDINALITY_MAP =
      ImmutableMap.<String, Double>builder()
          .put("[foodmart, agg_c_14_sales_fact_1997]", 86_805d)
          .put("[foodmart, customer]", 10_281d)
          .put("[foodmart, employee]", 1_155d)
          .put("[foodmart, employee_closure]", 7_179d)
          .put("[foodmart, department]", 10_281d)
          .put("[foodmart, inventory_fact_1997]", 4_070d)
          .put("[foodmart, position]", 18d)
          .put("[foodmart, product]", 1560d)
          .put("[foodmart, product_class]", 110d)
          .put("[foodmart, promotion]", 1_864d)
          // region really has 110 rows; made it smaller than store to trick FK
          .put("[foodmart, region]", 24d)
          .put("[foodmart, salary]", 21_252d)
          .put("[foodmart, sales_fact_1997]", 86_837d)
          .put("[foodmart, store]", 25d)
          .put("[foodmart, store_ragged]", 25d)
          .put("[foodmart, time_by_day]", 730d)
          .put("[foodmart, warehouse]", 24d)
          .put("[foodmart, warehouse_class]", 6d)
          .put("[scott, EMP]", 10d)
          .put("[scott, DEPT]", 4d)
          .put("[tpcds, CALL_CENTER]", 8d)
          .put("[tpcds, CATALOG_PAGE]", 11_718d)
          .put("[tpcds, CATALOG_RETURNS]", 144_067d)
          .put("[tpcds, CATALOG_SALES]", 1_441_548d)
          .put("[tpcds, CUSTOMER]", 100_000d)
          .put("[tpcds, CUSTOMER_ADDRESS]", 50_000d)
          .put("[tpcds, CUSTOMER_DEMOGRAPHICS]", 1_920_800d)
          .put("[tpcds, DATE_DIM]", 73049d)
          .put("[tpcds, DBGEN_VERSION]", 1d)
          .put("[tpcds, HOUSEHOLD_DEMOGRAPHICS]", 7200d)
          .put("[tpcds, INCOME_BAND]", 20d)
          .put("[tpcds, INVENTORY]", 11_745_000d)
          .put("[tpcds, ITEM]", 18_000d)
          .put("[tpcds, PROMOTION]", 300d)
          .put("[tpcds, REASON]", 35d)
          .put("[tpcds, SHIP_MODE]", 20d)
          .put("[tpcds, STORE]", 12d)
          .put("[tpcds, STORE_RETURNS]", 287_514d)
          .put("[tpcds, STORE_SALES]", 2_880_404d)
          .put("[tpcds, TIME_DIM]", 86_400d)
          .put("[tpcds, WAREHOUSE]", 5d)
          .put("[tpcds, WEB_PAGE]", 60d)
          .put("[tpcds, WEB_RETURNS]", 71_763d)
          .put("[tpcds, WEB_SALES]", 719_384d)
          .put("[tpcds, WEB_SITE]", 1d)
          .build();

  public double tableCardinality(List<String> qualifiedTableName) {
    return CARDINALITY_MAP.get(qualifiedTableName.toString());
  }
}

// End MapSqlStatisticProvider.java
