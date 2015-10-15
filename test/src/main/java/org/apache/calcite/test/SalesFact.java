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
 * A struct relating customer to product.
 */
public class SalesFact {
  // CHECKSTYLE: OFF
  public final int cust_id;
  public final int prod_id;

  public SalesFact(int cust_id, int prod_id) {
    this.cust_id = cust_id;
    this.prod_id = prod_id;
  }
  // CHECKSTYLE: ON
}

// End SalesFact.java
