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
package org.apache.calcite.test.schemata.orderstream;

import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.schema.StreamableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Table representing the ORDERS stream.
 */
public class OrdersTable extends BaseOrderStreamTable
    implements StreamableTable {
  private final ImmutableList<Object[]> rows;

  public OrdersTable(ImmutableList<Object[]> rows) {
    this.rows = rows;
  }

  @Override public Enumerable<@Nullable Object[]> scan(DataContext root) {
    return Linq4j.asEnumerable(rows);
  }

  @Override public Table stream() {
    return new OrdersTable(rows);
  }

  @Override public boolean isRolledUp(String column) {
    return false;
  }

  @Override public boolean rolledUpColumnValidInsideAgg(String column,
      SqlCall call, @Nullable SqlNode parent, @Nullable CalciteConnectionConfig config) {
    return false;
  }
}
