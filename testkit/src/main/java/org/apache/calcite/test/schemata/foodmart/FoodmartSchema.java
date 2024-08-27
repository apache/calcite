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
package org.apache.calcite.test.schemata.foodmart;

import org.apache.calcite.test.CalciteAssert;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

/**
 * Foodmart schema.
 */
public class FoodmartSchema {
  public static final String FOODMART_SCHEMA = "     {\n"
      + "       type: 'jdbc',\n"
      + "       name: 'foodmart',\n"
      + "       jdbcDriver: " + q(CalciteAssert.DB.foodmart.driver) + ",\n"
      + "       jdbcUser: " + q(CalciteAssert.DB.foodmart.username) + ",\n"
      + "       jdbcPassword: " + q(CalciteAssert.DB.foodmart.password) + ",\n"
      + "       jdbcUrl: " + q(CalciteAssert.DB.foodmart.url) + ",\n"
      + "       jdbcCatalog: " + q(CalciteAssert.DB.foodmart.catalog) + ",\n"
      + "       jdbcSchema: " + q(CalciteAssert.DB.foodmart.schema) + "\n"
      + "     }\n";
  public static final String FOODMART_MODEL = "{\n"
      + "  version: '1.0',\n"
      + "  defaultSchema: 'foodmart',\n"
      + "   schemas: [\n"
      + FOODMART_SCHEMA
      + "   ]\n"
      + "}";

  private static String q(@Nullable String s) {
    return s == null ? "null" : "'" + s + "'";
  }

  public final SalesFact[] sales_fact_1997 = {
      new SalesFact(100, 10),
      new SalesFact(150, 20),
  };

  /**
   * Sales fact model.
   */
  public static class SalesFact {
    public final int cust_id;
    public final int prod_id;

    public SalesFact(int cust_id, int prod_id) {
      this.cust_id = cust_id;
      this.prod_id = prod_id;
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof SalesFact
          && cust_id == ((SalesFact) obj).cust_id
          && prod_id == ((SalesFact) obj).prod_id;
    }

    @Override public int hashCode() {
      return Objects.hash(cust_id, prod_id);
    }
  }
}
