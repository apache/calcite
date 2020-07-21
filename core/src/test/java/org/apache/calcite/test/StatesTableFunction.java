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

import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;

/** A table function that returns states and their boundaries; also national
 * parks.
 *
 * <p>Has same content as
 * <code>file/src/test/resources/geo/states.json</code>. */
public class StatesTableFunction {
  private StatesTableFunction() {}

  private static final Object[][] STATE_ROWS = {
      {"NV", "Polygon((-120 42, -114 42, -114 37, -114.75 35.1, -120 39,"
          + " -120 42))"},
      {"UT", "Polygon((-114 42, -111.05 42, -111.05 41, -109.05 41, -109.05 37,"
          + " -114 37, -114 42))"},
      {"CA", "Polygon((-124.25 42, -120 42, -120 39, -114.75 35.1,"
          + " -114.75 32.5, -117.15 32.5, -118.30 33.75, -120.5 34.5,"
          + " -122.4 37.2, -124.25 42))"},
      {"AZ", "Polygon((-114 37, -109.05 37, -109.05 31.33, -111.07 31.33,"
          + " -114.75 32.5, -114.75 35.1, -114 37))"},
      {"CO", "Polygon((-109.05 41, -102 41, -102 37, -109.05 37, -109.05 41))"},
      {"OR", "Polygon((-123.9 46.2, -122.7 45.7, -119 46, -117 46, -116.5 45.5,"
          + " -117.03 44.2, -117.03 42, -124.25 42, -124.6 42.8,"
          + " -123.9 46.2))"},
      {"WA", "Polygon((-124.80 48.4, -123.2 48.2, -123.2 49, -117 49, -117 46,"
          + " -119 46, -122.7 45.7, -123.9 46.2, -124.80 48.4))"},
      {"ID", "Polygon((-117 49, -116.05 49, -116.05 48, -114.4 46.6,"
          + " -112.9 44.45, -111.05 44.45, -111.05 42, -117.03 42,"
          + " -117.03 44.2, -116.5 45.5, -117 46, -117 49))"},
      {"MT", "Polygon((-116.05 49, -104.05 49, -104.05 45, -111.05 45,"
          + " -111.05 44.45, -112.9 44.45, -114.4 46.6, -116.05 48,"
          + " -116.05 49))"},
      {"WY", "Polygon((-111.05 45, -104.05 45, -104.05 41, -111.05 41,"
          + " -111.05 45))"},
      {"NM", "Polygon((-109.05 37, -103 37, -103 32, -106.65 32, -106.5 31.8,"
          + " -108.2 31.8, -108.2 31.33, -109.05 31.33, -109.05 37))"}
  };

  private static final Object[][] PARK_ROWS = {
      {"Yellowstone NP", "Polygon((-111.2 45.1, -109.30 45.1, -109.30 44.1,"
          + " -109 43.8, -110 43, -111.2 43.4, -111.2 45.1))"},
      {"Yosemite NP", "Polygon((-120.2 38, -119.30 38.2, -119 37.7,"
          + " -119.9 37.6, -120.2 38))"},
      {"Death Valley NP", "Polygon((-118.2 37.3, -117 37, -116.3 35.7,"
          + " -117 35.7, -117.2 36.2, -117.8 36.4, -118.2 37.3))"},
  };

  public static ScannableTable states(boolean b) {
    return eval(STATE_ROWS);
  };

  public static ScannableTable parks(boolean b) {
    return eval(PARK_ROWS);
  };

  private static ScannableTable eval(final Object[][] rows) {
    return new ScannableTable() {
      public Enumerable<Object[]> scan(DataContext root) {
        return Linq4j.asEnumerable(rows);
      }

      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
            .add("name", SqlTypeName.VARCHAR)
            .add("geom", SqlTypeName.VARCHAR)
            .build();
      }

      public Statistic getStatistic() {
        return Statistics.of(rows.length,
            ImmutableList.of(ImmutableBitSet.of(0)));
      }

      public Schema.TableType getJdbcTableType() {
        return Schema.TableType.TABLE;
      }

      public boolean isRolledUp(String column) {
        return false;
      }

      public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call,
          SqlNode parent, CalciteConnectionConfig config) {
        return false;
      }
    };
  }
}
