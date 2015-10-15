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

/**
 * Common Foodmart schema for tests.
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


  private static String q(String s) {
    return s == null ? "null" : "'" + s + "'";
  }

  // CHECKSTYLE: OFF
  public final SalesFact[] sales_fact_1997 = {
    new SalesFact(100, 10),
    new SalesFact(150, 20),
  };
  // CHECKSTYLE: ON
}

// End FoodmartSchema.java

