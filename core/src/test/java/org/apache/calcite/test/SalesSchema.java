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
    public final int jansales;
    public final int febsales;
    public final int marsales;
    public final int janexpense;
    public final int febexpense;
    public final int marexpense;

    public Sales(
        int id, int year, int jansales, int febsales, int marsales,
        int janexpense, int febexpense, int marexpense) {
      this.id = id;
      this.year = year;
      this.jansales = jansales;
      this.febsales = febsales;
      this.marsales = marsales;
      this.janexpense = janexpense;
      this.febexpense = febexpense;
      this.marexpense = marexpense;
    }
  }
}
