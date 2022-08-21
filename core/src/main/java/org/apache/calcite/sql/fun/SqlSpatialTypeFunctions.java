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
package org.apache.calcite.sql.fun;

import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.runtime.SpatialTypeFunctions;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.locationtech.jts.geom.Geometry;

import java.math.BigDecimal;

/**
 * Utilities for spatial type functions.
 *
 * <p>Includes some table functions, and may in future include other functions
 * that have dependencies beyond the {@code org.apache.calcite.runtime} package.
 */
public class SqlSpatialTypeFunctions {
  private SqlSpatialTypeFunctions() {}

  // Geometry table functions =================================================

  /** Calculates a regular grid of polygons based on {@code geom}.
   *
   * @see SpatialTypeFunctions ST_MakeGrid */
  @SuppressWarnings({"WeakerAccess", "unused"})
  public static ScannableTable ST_MakeGrid(final Geometry geom,
      final BigDecimal deltaX, final BigDecimal deltaY) {
    return new GridTable(geom, deltaX, deltaY, false);
  }

  /** Calculates a regular grid of points based on {@code geom}.
   *
   * @see SpatialTypeFunctions ST_MakeGridPoints */
  @SuppressWarnings({"WeakerAccess", "unused"})
  public static ScannableTable ST_MakeGridPoints(final Geometry geom,
      final BigDecimal deltaX, final BigDecimal deltaY) {
    return new GridTable(geom, deltaX, deltaY, true);
  }

  /** Returns the points or rectangles in a grid that covers a given
   * geometry. */
  public static class GridTable implements ScannableTable {
    private final Geometry geom;
    private final BigDecimal deltaX;
    private final BigDecimal deltaY;
    private boolean point;

    GridTable(Geometry geom, BigDecimal deltaX, BigDecimal deltaY,
        boolean point) {
      this.geom = geom;
      this.deltaX = deltaX;
      this.deltaY = deltaY;
      this.point = point;
    }

    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          // a point (for ST_MakeGridPoints) or a rectangle (for ST_MakeGrid)
          .add("THE_GEOM", SqlTypeName.GEOMETRY)
          // in [0, width * height)
          .add("ID", SqlTypeName.INTEGER)
          // in [1, width]
          .add("ID_COL", SqlTypeName.INTEGER)
          // in [1, height]
          .add("ID_ROW", SqlTypeName.INTEGER)
          // absolute column, with 0 somewhere near the origin; not standard
          .add("ABS_COL", SqlTypeName.INTEGER)
          // absolute row, with 0 somewhere near the origin; not standard
          .add("ABS_ROW", SqlTypeName.INTEGER)
          .build();
    }

    @Override public Enumerable<@Nullable Object[]> scan(DataContext root) {
      if (geom != null && deltaX != null && deltaY != null) {
        if (deltaX.compareTo(BigDecimal.ZERO) > 0
            && deltaY.compareTo(BigDecimal.ZERO) > 0) {
          return new SpatialTypeFunctions.GridEnumerable(geom.getEnvelopeInternal(), deltaX, deltaY,
              point);
        }
      }
      return Linq4j.emptyEnumerable();
    }

    @Override public Statistic getStatistic() {
      return Statistics.of(100d, ImmutableList.of(ImmutableBitSet.of(0, 1)));
    }

    @Override public Schema.TableType getJdbcTableType() {
      return Schema.TableType.OTHER;
    }

    @Override public boolean isRolledUp(String column) {
      return false;
    }

    @Override public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call,
        @Nullable SqlNode parent, @Nullable CalciteConnectionConfig config) {
      return false;
    }
  }
}
