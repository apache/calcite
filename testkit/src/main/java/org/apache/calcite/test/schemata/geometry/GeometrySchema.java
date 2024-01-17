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
package org.apache.calcite.test.schemata.geometry;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

/**
 * A simple schema that contains spatial data.
 */
public class GeometrySchema {

  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

  public final PointRow[] points = {
      new PointRow(1L, GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1))),
      new PointRow(1L, GEOMETRY_FACTORY.createPoint(new Coordinate(2, 2))),
      new PointRow(3L, GEOMETRY_FACTORY.createPoint(new Coordinate(3, 3))),
  };

  public final LineStringRow[] linestrings = {
      new LineStringRow(
          1L,
          GEOMETRY_FACTORY.createLineString(new Coordinate[] {
              new Coordinate(1, 1),
              new Coordinate(2, 2),
              new Coordinate(3, 3),
          })),
      new LineStringRow(
          2L,
          GEOMETRY_FACTORY.createLineString(new Coordinate[] {
              new Coordinate(4, 4),
              new Coordinate(5, 5),
              new Coordinate(6, 6),
          })),
  };

  public final PolygonRow[] polygons = {
      new PolygonRow(
          1L,
          GEOMETRY_FACTORY.createPolygon(new Coordinate[] {
              new Coordinate(1, 1),
              new Coordinate(2, 2),
              new Coordinate(3, 3),
              new Coordinate(1, 1),
          })),
      new PolygonRow(
          2L,
          GEOMETRY_FACTORY.createPolygon(new Coordinate[] {
              new Coordinate(4, 4),
              new Coordinate(5, 5),
              new Coordinate(6, 6),
              new Coordinate(4, 4),
          })),
  };

  /**
   * A point.
   */
  public static class PointRow {

    public final Long id;

    public final Geometry point;

    public PointRow(Long id, Point point) {
      this.id = id;
      this.point = point;
    }
  }

  /**
   * A linestring.
   */
  public static class LineStringRow {

    public final Long id;

    public final Geometry lineString;

    public LineStringRow(Long id, LineString lineString) {
      this.id = id;
      this.lineString = lineString;
    }
  }

  /**
   * A polygon.
   */
  public static class PolygonRow {

    public final Long id;

    public final Geometry polygon;

    public PolygonRow(Long id, Polygon polygon) {
      this.id = id;
      this.polygon = polygon;
    }
  }

}
