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
package org.apache.calcite.sql.util;

import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.runtime.GeoFunctions;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlGeoFunctions;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;

import java.util.function.Supplier;

/**
 * Utilities for {@link SqlOperatorTable}s.
 */
public class SqlOperatorTables extends ReflectiveSqlOperatorTable {

  private static final Supplier<SqlOperatorTable> SPATIAL =
      Suppliers.memoize(SqlOperatorTables::createSpatial)::get;

  private static SqlOperatorTable createSpatial() {
    return CalciteCatalogReader.operatorTable(
        GeoFunctions.class.getName(),
        SqlGeoFunctions.class.getName());
  }

  /** Returns the Spatial operator table, creating it if necessary. */
  public static SqlOperatorTable spatialInstance() {
    return SPATIAL.get();
  }

  /** Creates a composite operator table. */
  public static SqlOperatorTable chain(Iterable<SqlOperatorTable> tables) {
    final ImmutableList<SqlOperatorTable> list =
        ImmutableList.copyOf(tables);
    if (list.size() == 1) {
      return list.get(0);
    }
    return new ChainedSqlOperatorTable(list);
  }

  /** Creates a composite operator table from an array of tables. */
  public static SqlOperatorTable chain(SqlOperatorTable... tables) {
    return chain(ImmutableList.copyOf(tables));
  }
}
