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
 * A Schema representing per month sales figure
 *
 * <p>It contains a single table with information of sales.
 */

public class SalesSchema {

  public final SalesSchema.Sales[] sales = {
      new SalesSchema.Sales(123, 2022, 100, 200, 300,
          50, 100, 150),
      new SalesSchema.Sales(123, 2022, 200, 300, 400,
          100, 150, 200),
  };

  /**
   * Sales table.
   */
  public static class Sales {
    public final int id;
    public final int year;
    public final int jan_sales;
    public final int feb_sales;
    public final int mar_sales;
    public final int jan_expense;
    public final int feb_expense;
    public final int mar_expense;

    public Sales(
        int id, int year, int jan_sales, int feb_sales, int mar_sales,
        int jan_expense, int feb_expense, int mar_expense) {
      this.id = id;
      this.year = year;
      this.jan_sales = jan_sales;
      this.feb_sales = feb_sales;
      this.mar_sales = mar_sales;
      this.jan_expense = jan_expense;
      this.feb_expense = feb_expense;
      this.mar_expense = mar_expense;
    }
  }
}
