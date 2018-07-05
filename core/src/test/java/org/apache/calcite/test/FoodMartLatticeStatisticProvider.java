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

import org.apache.calcite.materialize.DelegatingLatticeStatisticProvider;
import org.apache.calcite.materialize.Lattice;
import org.apache.calcite.materialize.LatticeStatisticProvider;
import org.apache.calcite.materialize.Lattices;

import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link LatticeStatisticProvider}
 * that has hard-coded values for various attributes in the FoodMart lattice.
 *
 * <p>This makes testing faster.
 */
public class FoodMartLatticeStatisticProvider
    extends DelegatingLatticeStatisticProvider {
  public static final FoodMartLatticeStatisticProvider.Factory FACTORY =
      lattice -> new FoodMartLatticeStatisticProvider(lattice,
          Lattices.CACHED_SQL.apply(lattice));

  private static final Map<String, Integer> CARDINALITY_MAP =
      ImmutableMap.<String, Integer>builder()
          .put("brand_name", 111)
          .put("cases_per_pallet", 10)
          .put("customer_id", 5581)
          .put("day_of_month", 30)
          .put("fiscal_period", 0)
          .put("gross_weight", 376)
          .put("low_fat", 2)
          .put("month_of_year", 12)
          .put("net_weight", 332)
          .put("product_category", 45)
          .put("product_class_id", 102)
          .put("product_department", 22)
          .put("product_family", 3)
          .put("product_id", 1559)
          .put("product_name", 1559)
          .put("product_subcategory", 102)
          .put("promotion_id", 149)
          .put("quarter", 4)
          .put("recyclable_package", 2)
          .put("shelf_depth", 488)
          .put("shelf_height", 524)
          .put("shelf_width", 534)
          .put("SKU", 1559)
          .put("SRP", 315)
          .put("store_cost", 10777)
          .put("store_id", 13)
          .put("store_sales", 1049)
          .put("the_date", 323)
          .put("the_day", 7)
          .put("the_month", 12)
          .put("the_year", 1)
          .put("time_id", 323)
          .put("units_per_case", 36)
          .put("unit_sales", 6)
          .put("week_of_year", 52)
          .build();

  private final Lattice lattice;

  private FoodMartLatticeStatisticProvider(Lattice lattice,
      LatticeStatisticProvider provider) {
    super(provider);
    this.lattice = lattice;
  }

  private int cardinality(Lattice.Column column) {
    final Integer integer = CARDINALITY_MAP.get(column.alias);
    if (integer != null && integer > 0) {
      return integer;
    }
    return column.alias.length();
  }

  @Override public double cardinality(List<Lattice.Column> columns) {
    final List<Double> cardinalityList = new ArrayList<>();
    for (Lattice.Column column : columns) {
      cardinalityList.add((double) cardinality(column));
    }
    return Lattice.getRowCount(lattice.getFactRowCount(), cardinalityList);
  }
}

// End FoodMartLatticeStatisticProvider.java
