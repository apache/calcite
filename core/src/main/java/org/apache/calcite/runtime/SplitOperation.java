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
package org.apache.calcite.runtime;

import org.locationtech.jts.algorithm.LineIntersector;
import org.locationtech.jts.algorithm.RobustLineIntersector;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.util.LineStringExtracter;
import org.locationtech.jts.operation.polygonize.Polygonizer;

import java.util.ArrayList;
import java.util.List;

/**
 * Splits {@code geom} by {@code blade}.
 */
public class SplitOperation {

  private final Geometry geom;

  private final Geometry blade;

  public SplitOperation(Geometry geom, Geometry blade) {
    this.geom = geom;
    this.blade = blade;
  }

  public Geometry split() {
    if (geom instanceof LineString && blade instanceof Point) {
      return split((LineString) geom, (Point) blade);

    } else if (geom instanceof LineString && blade instanceof LineString) {
      return split((LineString) geom, (LineString) blade);

    } else if (geom instanceof MultiLineString && blade instanceof Point) {
      return split((MultiLineString) geom, (Point) blade);

    } else if (geom instanceof MultiLineString && blade instanceof LineString) {
      return split((MultiLineString) geom, (LineString) blade);

    } else if (geom instanceof Polygon && blade instanceof LineString) {
      return split((Polygon) geom, (LineString) blade);

    } else if (geom instanceof MultiPolygon && blade instanceof LineString) {
      return split((MultiPolygon) geom, (LineString) blade);

    } else {
      throw new UnsupportedOperationException(
          "Split operation not supported for "
              + geom.getGeometryType() + " and "
              + blade.getGeometryType());
    }
  }

  private static Geometry split(LineString geometry, Point blade) {
    GeometryFactory factory = geometry.getFactory();

    Coordinate[] coordinates = geometry.getCoordinates();
    LineIntersector intersector = new RobustLineIntersector();
    Coordinate p = blade.getCoordinate();

    List<Coordinate> accumulator = new ArrayList<>();
    List<LineString> lines = new ArrayList<>();

    for (int i = 1; i < coordinates.length; i++) {
      Coordinate p1 = coordinates[i - 1];
      Coordinate p2 = coordinates[i];

      accumulator.add(p1.copy());

      intersector.computeIntersection(p, p1, p2);
      if (intersector.hasIntersection()) {
        accumulator.add(p.copy());
        LineString line =
            factory.createLineString(accumulator.toArray(new Coordinate[0]));
        lines.add(line);

        accumulator.clear();
        accumulator.add(p.copy());
      }
    }

    accumulator.add(coordinates[coordinates.length - 1].copy());
    LineString line =
        factory.createLineString(accumulator.toArray(new Coordinate[0]));
    lines.add(line);

    if (lines.size() == 1) {
      return lines.get(0);
    } else {
      return factory.buildGeometry(lines);
    }
  }

  private static Geometry split(MultiLineString geometry, Point blade) {
    GeometryFactory factory = geometry.getFactory();
    List<Geometry> geometries = new ArrayList<>();
    for (int i = 0; i < geometry.getNumGeometries(); i++) {
      Geometry split = split((LineString) geometry.getGeometryN(i), blade);
      if (split instanceof GeometryCollection) {
        for (int j = 0; j < split.getNumGeometries(); j++) {
          geometries.add(split.getGeometryN(j));
        }
      } else {
        geometries.add(split);
      }
    }
    return factory.buildGeometry(geometries);
  }

  private static Geometry split(LineString geometry, LineString blade) {
    return geometry.difference(blade);
  }

  private static Geometry split(MultiLineString geometry, LineString blade) {
    return geometry.difference(blade);
  }

  private static Geometry split(Polygon geometry, LineString blade) {
    GeometryFactory factory = geometry.getFactory();
    List<Polygon> polygons = new ArrayList<>();
    Geometry union = geometry.getBoundary().union(blade);
    Polygonizer polygonizer = new Polygonizer();
    polygonizer.add(LineStringExtracter.getLines(union));
    for (Polygon p : GeometryFactory.toPolygonArray(polygonizer.getPolygons())) {
      if (geometry.contains(geometry.getInteriorPoint())) {
        p.normalize();
        polygons.add(p);
      }
    }
    return factory.buildGeometry(polygons);
  }

  private static Geometry split(MultiPolygon geometry, LineString blade) {
    GeometryFactory factory = geometry.getFactory();
    List<Geometry> geometries = new ArrayList<>();
    for (int i = 0; i < geometry.getNumGeometries(); i++) {
      Geometry split = split((Polygon) geometry.getGeometryN(i), blade);
      if (split instanceof GeometryCollection) {
        for (int j = 0; j < split.getNumGeometries(); j++) {
          geometries.add(split.getGeometryN(j));
        }
      } else {
        geometries.add(split);
      }
    }
    return factory.buildGeometry(geometries);
  }

}
