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

import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.function.Deterministic;
import org.apache.calcite.linq4j.function.Experimental;
import org.apache.calcite.linq4j.function.Hints;
import org.apache.calcite.linq4j.function.SemiStrict;
import org.apache.calcite.linq4j.function.Strict;
import org.apache.calcite.runtime.SpatialTypeUtils.SpatialType;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.locationtech.jts.algorithm.InteriorPoint;
import org.locationtech.jts.algorithm.MinimumBoundingCircle;
import org.locationtech.jts.algorithm.MinimumDiameter;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryComponentFilter;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.GeometryFilter;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.OctagonalEnvelope;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.geom.util.AffineTransformation;
import org.locationtech.jts.geom.util.GeometryFixer;
import org.locationtech.jts.operation.linemerge.LineMerger;
import org.locationtech.jts.operation.overlay.snap.GeometrySnapper;
import org.locationtech.jts.operation.polygonize.Polygonizer;
import org.locationtech.jts.precision.GeometryPrecisionReducer;
import org.locationtech.jts.simplify.DouglasPeuckerSimplifier;
import org.locationtech.jts.simplify.TopologyPreservingSimplifier;
import org.locationtech.jts.util.GeometricShapeFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.apache.calcite.runtime.SpatialTypeUtils.GEOMETRY_FACTORY;
import static org.apache.calcite.runtime.SpatialTypeUtils.NO_SRID;
import static org.apache.calcite.runtime.SpatialTypeUtils.asEwkt;
import static org.apache.calcite.runtime.SpatialTypeUtils.asGeoJson;
import static org.apache.calcite.runtime.SpatialTypeUtils.asGml;
import static org.apache.calcite.runtime.SpatialTypeUtils.asWkb;
import static org.apache.calcite.runtime.SpatialTypeUtils.asWkt;
import static org.apache.calcite.runtime.SpatialTypeUtils.fromEwkt;
import static org.apache.calcite.runtime.SpatialTypeUtils.fromGeoJson;
import static org.apache.calcite.runtime.SpatialTypeUtils.fromGml;
import static org.apache.calcite.runtime.SpatialTypeUtils.fromWkb;
import static org.apache.calcite.runtime.SpatialTypeUtils.fromWkt;

/**
 * Helper methods to implement spatial type (ST) functions in generated code.
 *
 * <p>Remaining tasks:
 *
 * <ul>
 *   <li>Determine type code for
 *   {@link org.apache.calcite.sql.type.ExtraSqlTypes#GEOMETRY}
 *   <li>Should we create aliases for functions in upper-case?
 *   Without ST_ prefix?
 *   <li>Consider adding spatial literals, e.g. `GEOMETRY 'POINT (30 10)'`
 *   <li>Integer arguments, e.g. SELECT ST_MakePoint(1, 2, 1.5),
 *     ST_MakePoint(1, 2)
 *   <li>Are GEOMETRY values comparable? If so add ORDER BY test
 *   <li>We have to add 'Z' to create 3D objects. This is inconsistent with
 *   PostGIS. Who is right? At least document the difference.
 *   <li>Should add GeometryEngine.intersects; similar to disjoint etc.
 *   <li>Make {@link #ST_MakeLine(Geometry, Geometry)} varargs</li>
 * </ul>
 */
@SuppressWarnings({"WeakerAccess", "unused"})
@Deterministic
@Strict
@Experimental
public class SpatialTypeFunctions {

  private SpatialTypeFunctions() {
  }

  // Geometry conversion functions (2D)

  public static @Nullable ByteString ST_AsBinary(Geometry geometry) {
    return ST_AsWKB(geometry);
  }

  public static @Nullable String ST_AsEWKT(Geometry geometry) {
    return asEwkt(geometry);
  }

  public static @Nullable String ST_AsGeoJSON(Geometry geometry) {
    return asGeoJson(geometry);
  }

  public static @Nullable String ST_AsGML(Geometry geometry) {
    return asGml(geometry);
  }

  public static @Nullable String ST_AsText(Geometry geometry) {
    return ST_AsWKT(geometry);
  }

  public static @Nullable ByteString ST_AsEWKB(Geometry geometry) {
    return ST_AsWKB(geometry);
  }

  public static @Nullable ByteString ST_AsWKB(Geometry geometry) {
    return asWkb(geometry);
  }

  public static @Nullable String ST_AsWKT(Geometry geometry) {
    return asWkt(geometry);
  }

  public static @Nullable Geometry ST_Force2D(Geometry geometry) {
    Function<Coordinate, Coordinate> transform =
        coordinate -> new Coordinate(coordinate.getX(), coordinate.getY());
    CoordinateTransformer transformer = new CoordinateTransformer(transform);
    return transformer.transform(geometry);
  }

  public static @Nullable Geometry ST_GeomFromEWKB(ByteString ewkb) {
    return ST_GeomFromWKB(ewkb);
  }

  public static @Nullable Geometry ST_GeomFromEWKT(String ewkt) {
    return fromEwkt(ewkt);
  }

  public static @Nullable Geometry ST_GeomFromGeoJSON(String geojson) {
    return fromGeoJson(geojson);
  }

  public static @Nullable Geometry ST_GeomFromGML(String gml) {
    return ST_GeomFromGML(gml, NO_SRID);
  }

  public static @Nullable Geometry ST_GeomFromGML(String gml, int srid) {
    Geometry geometry = fromGml(gml);
    geometry.setSRID(srid);
    return geometry;
  }

  public static @Nullable Geometry ST_GeomFromText(String wkt) {
    return ST_GeomFromWKT(wkt);
  }

  public static @Nullable Geometry ST_GeomFromText(String wkt, int srid) {
    return ST_GeomFromWKT(wkt, srid);
  }

  public static @Nullable Geometry ST_GeomFromWKB(ByteString wkb) {
    return fromWkb(wkb);
  }

  public static @Nullable Geometry ST_GeomFromWKB(ByteString wkb, int srid) {
    Geometry geometry = fromWkb(wkb);
    geometry.setSRID(srid);
    return geometry;
  }

  public static @Nullable Geometry ST_GeomFromWKT(String wkt) {
    return ST_GeomFromWKT(wkt, NO_SRID);
  }

  public static @Nullable Geometry ST_GeomFromWKT(String wkt, int srid) {
    Geometry geometry = fromWkt(wkt);
    geometry.setSRID(srid);
    return geometry;
  }

  public static @Nullable Geometry ST_LineFromText(String wkt) {
    Geometry geometry = ST_GeomFromWKT(wkt);
    return geometry instanceof LineString ? geometry : null;
  }

  public static @Nullable Geometry ST_LineFromText(String wkt, int srid) {
    Geometry geometry = ST_GeomFromWKT(wkt, srid);
    return geometry instanceof LineString ? geometry : null;
  }

  public static @Nullable Geometry ST_LineFromWKB(ByteString wkb) {
    Geometry geometry = ST_GeomFromWKB(wkb);
    return geometry instanceof LineString ? geometry : null;
  }

  public static @Nullable Geometry ST_LineFromWKB(ByteString wkt, int srid) {
    Geometry geometry = ST_GeomFromWKB(wkt, srid);
    return geometry instanceof LineString ? geometry : null;
  }

  public static @Nullable Geometry ST_MLineFromText(String wkt) {
    Geometry geometry = ST_GeomFromWKT(wkt);
    return geometry instanceof MultiLineString ? geometry : null;
  }

  public static @Nullable Geometry ST_MLineFromText(String wkt, int srid) {
    Geometry geometry = ST_GeomFromWKT(wkt, srid);
    return geometry instanceof MultiLineString ? geometry : null;
  }

  public static @Nullable Geometry ST_MPointFromText(String wkt) {
    Geometry geometry = ST_GeomFromWKT(wkt);
    return geometry instanceof MultiPoint ? geometry : null;
  }

  public static @Nullable Geometry ST_MPointFromText(String wkt, int srid) {
    Geometry geometry = ST_GeomFromWKT(wkt, srid);
    return geometry instanceof MultiPoint ? geometry : null;
  }

  public static @Nullable Geometry ST_MPolyFromText(String wkt) {
    Geometry geometry = ST_GeomFromWKT(wkt);
    return geometry instanceof MultiPolygon ? geometry : null;
  }

  public static @Nullable Geometry ST_MPolyFromText(String wkt, int srid) {
    Geometry geometry = ST_GeomFromWKT(wkt, srid);
    return geometry instanceof MultiPolygon ? geometry : null;
  }

  public static @Nullable Geometry ST_PointFromText(String wkt) {
    Geometry geometry = ST_GeomFromWKT(wkt);
    return geometry instanceof Point ? geometry : null;
  }

  public static @Nullable Geometry ST_PointFromText(String wkt, int srid) {
    Geometry geometry = ST_GeomFromWKT(wkt, srid);
    return geometry instanceof Point ? geometry : null;
  }

  public static @Nullable Geometry ST_PointFromWKB(ByteString wkb) {
    Geometry geometry = ST_GeomFromWKB(wkb);
    return geometry instanceof Point ? geometry : null;
  }

  public static @Nullable Geometry ST_PointFromWKB(ByteString wkb, int srid) {
    Geometry geometry = ST_GeomFromWKB(wkb, srid);
    return geometry instanceof Point ? geometry : null;
  }

  public static @Nullable Geometry ST_PolyFromText(String wkt) {
    Geometry geometry = ST_GeomFromWKT(wkt);
    return geometry instanceof Polygon ? geometry : null;
  }

  public static @Nullable Geometry ST_PolyFromText(String wkt, int srid) {
    Geometry geometry = ST_GeomFromWKT(wkt, srid);
    return geometry instanceof Polygon ? geometry : null;
  }

  public static @Nullable Geometry ST_PolyFromWKB(ByteString wkb) {
    Geometry geometry = ST_GeomFromWKB(wkb);
    return geometry instanceof Polygon ? geometry : null;
  }

  public static @Nullable Geometry ST_PolyFromWKB(ByteString wkb, int srid) {
    Geometry geometry = ST_GeomFromWKB(wkb, srid);
    return geometry instanceof Polygon ? geometry : null;
  }

  /**
   * Converts the coordinates of a {@code geom} into a MULTIPOINT.
   */
  public static Geometry ST_ToMultiPoint(Geometry geom) {
    CoordinateSequence coordinateSequence = GEOMETRY_FACTORY
        .getCoordinateSequenceFactory().create(geom.getCoordinates());
    return GEOMETRY_FACTORY.createMultiPoint(coordinateSequence);
  }

  /**
   * Converts the a {@code geom} into a MULTILINESTRING.
   */
  public static Geometry ST_ToMultiLine(Geometry geom) {
    GeometryFactory factory = geom.getFactory();
    ArrayList<LineString> lines = new ArrayList<>();
    geom.apply((GeometryComponentFilter) inputGeom -> {
      if (inputGeom instanceof LineString) {
        lines.add(factory.createLineString(inputGeom.getCoordinates()));
      }
    });
    if (lines.isEmpty()) {
      return factory.createMultiLineString();
    } else {
      return factory.createMultiLineString(lines.toArray(new LineString[lines.size()]));
    }
  }

  /**
   * Converts a {@code geom} into a set of distinct segments stored in a MULTILINESTRING.
   */
  public static Geometry ST_ToMultiSegments(Geometry geom) {
    GeometryFactory factory = geom.getFactory();
    ArrayList<LineString> lines = new ArrayList<>();
    geom.apply((GeometryComponentFilter) inputGeom -> {
      if (inputGeom instanceof LineString) {
        Coordinate[] coordinates = inputGeom.getCoordinates();
        for (int i = 1; i < coordinates.length; i++) {
          Coordinate[] pair = new Coordinate[]{coordinates[i - 1], coordinates[i]};
          lines.add(factory.createLineString(pair));
        }
      }
    });
    if (lines.isEmpty()) {
      return factory.createMultiLineString();
    } else {
      return factory.createMultiLineString(lines.toArray(new LineString[lines.size()]));
    }
  }

  // Geometry conversion functions (3D)

  public static @Nullable Geometry ST_Force3D(Geometry geometry) {
    Function<Coordinate, Coordinate> transform =
        coordinate -> new Coordinate(
            coordinate.getX(),
            coordinate.getY(),
            Double.isNaN(coordinate.getZ()) ? 0d : coordinate.getZ());
    CoordinateTransformer transformer = new CoordinateTransformer(transform);
    return transformer.transform(geometry);
  }

  // Geometry creation functions ==============================================

  /**
   * Calculates a regular grid of polygons based on {@code geom}.
   */
  private static void ST_MakeGrid(final Geometry geom,
      final BigDecimal deltaX, final BigDecimal deltaY) {
    // This is a dummy function. We cannot include table functions in this
    // package, because they have too many dependencies. See the real definition
    // in SqlSpatialTypeFunctions.
  }

  /**
   * Calculates a regular grid of points based on {@code geom}.
   */
  private static void ST_MakeGridPoints(final Geometry geom,
      final BigDecimal deltaX, final BigDecimal deltaY) {
    // This is a dummy function. We cannot include table functions in this
    // package, because they have too many dependencies. See the real definition
    // in SqlSpatialTypeFunctions.
  }

  // Geometry creation functions (2D)

  /**
   * Returns the minimum bounding circle of {@code geom}.
   */
  public static Geometry ST_BoundingCircle(Geometry geom) {
    return new MinimumBoundingCircle(geom).getCircle();
  }

  /**
   * Expands {@code geom}'s envelope.
   */
  public static Geometry ST_Expand(Geometry geom, BigDecimal distance) {
    Envelope envelope = geom.getEnvelopeInternal().copy();
    envelope.expandBy(distance.doubleValue());
    return geom.getFactory().toGeometry(envelope);
  }

  /**
   * Makes an ellipse.
   */
  public static @Nullable Geometry ST_MakeEllipse(Geometry point, BigDecimal width,
      BigDecimal height) {
    if (!(point instanceof Point)) {
      return null;
    }
    GeometricShapeFactory factory = new GeometricShapeFactory(point.getFactory());
    factory.setCentre(point.getCoordinate());
    factory.setWidth(width.doubleValue());
    factory.setHeight(height.doubleValue());
    return factory.createEllipse();
  }

  /**
   * Makes a polygon.
   */
  public static @Nullable Geometry ST_MakePolygon(Geometry shell) {
    return makePolygon(shell);
  }

  /**
   * Makes a polygon.
   */
  public static @Nullable Geometry ST_MakePolygon(Geometry shell, Geometry hole0) {
    return makePolygon(shell, hole0);
  }

  /**
   * Makes a polygon.
   */
  public static @Nullable Geometry ST_MakePolygon(Geometry shell,
      Geometry hole0, Geometry hole1) {
    return makePolygon(shell,
        hole0, hole1);
  }

  /**
   * Makes a polygon.
   */
  public static @Nullable Geometry ST_MakePolygon(Geometry shell,
      Geometry hole0, Geometry hole1, Geometry hole2) {
    return makePolygon(shell,
        hole0, hole1, hole2);
  }

  /**
   * Makes a polygon.
   */
  public static @Nullable Geometry ST_MakePolygon(Geometry shell,
      Geometry hole0, Geometry hole1, Geometry hole2, Geometry hole3) {
    return makePolygon(shell,
        hole0, hole1, hole2, hole3);
  }

  /**
   * Makes a polygon.
   */
  public static @Nullable Geometry ST_MakePolygon(Geometry shell,
      Geometry hole0, Geometry hole1, Geometry hole2, Geometry hole3, Geometry hole4) {
    return makePolygon(shell,
        hole0, hole1, hole2, hole3, hole4);
  }

  /**
   * Makes a polygon.
   */
  public static @Nullable Geometry ST_MakePolygon(Geometry shell,
      Geometry hole0, Geometry hole1, Geometry hole2, Geometry hole3, Geometry hole4,
      Geometry hole5) {
    return makePolygon(shell,
        hole0, hole1, hole2, hole3, hole4,
        hole5);
  }

  /**
   * Makes a polygon.
   */
  public static @Nullable Geometry ST_MakePolygon(Geometry shell,
      Geometry hole0, Geometry hole1, Geometry hole2, Geometry hole3, Geometry hole4,
      Geometry hole5, Geometry hole6) {
    return makePolygon(shell,
        hole0, hole1, hole2, hole3, hole4,
        hole5, hole6);
  }

  /**
   * Makes a polygon.
   */
  public static @Nullable Geometry ST_MakePolygon(Geometry shell,
      Geometry hole0, Geometry hole1, Geometry hole2, Geometry hole3, Geometry hole4,
      Geometry hole5, Geometry hole6, Geometry hole7) {
    return makePolygon(shell,
        hole0, hole1, hole2, hole3, hole4,
        hole5, hole6, hole7);
  }

  /**
   * Makes a polygon.
   */
  public static @Nullable Geometry ST_MakePolygon(Geometry shell,
      Geometry hole0, Geometry hole1, Geometry hole2, Geometry hole3, Geometry hole4,
      Geometry hole5, Geometry hole6, Geometry hole7, Geometry hole8) {
    return makePolygon(shell,
        hole0, hole1, hole2, hole3, hole4,
        hole5, hole6, hole7, hole8);
  }

  /**
   * Makes a polygon.
   */
  public static @Nullable Geometry ST_MakePolygon(Geometry shell,
      Geometry hole0, Geometry hole1, Geometry hole2, Geometry hole3, Geometry hole4,
      Geometry hole5, Geometry hole6, Geometry hole7, Geometry hole8, Geometry hole9) {
    return makePolygon(shell,
        hole0, hole1, hole2, hole3, hole4,
        hole5, hole6, hole7, hole8, hole9);
  }

  private static @Nullable Geometry makePolygon(Geometry shell, Geometry... holes) {
    if (!(shell instanceof LineString)) {
      throw new RuntimeException("Only supports LINESTRINGs.");
    }
    if (!((LineString) shell).isClosed()) {
      throw new RuntimeException("The LINESTRING must be closed.");
    }
    for (Geometry hole : holes) {
      if (!(hole instanceof LineString)) {
        throw new RuntimeException("Only supports LINESTRINGs.");
      }
      if (!((LineString) hole).isClosed()) {
        throw new RuntimeException("The LINESTRING must be closed.");
      }
    }
    LinearRing shellRing = shell.getFactory().createLinearRing(shell.getCoordinates());
    LinearRing[] holeRings = new LinearRing[holes.length];
    for (int i = 0; i < holes.length; i++) {
      holeRings[i] = holes[i].getFactory().createLinearRing(holes[i].getCoordinates());
    }
    return shell.getFactory().createPolygon(shellRing, holeRings);
  }

  /**
   * Returns the minimum diameter of {@code geom}.
   */
  public static @Nullable Geometry ST_MinimumDiameter(Geometry geom) {
    return new MinimumDiameter(geom).getDiameter();
  }

  /**
   * Returns the minimum rectangle enclosing {@code geom}.
   */
  public static @Nullable Geometry ST_MinimumRectangle(Geometry geom) {
    return new MinimumDiameter(geom).getMinimumRectangle();
  }

  /**
   * Returns the octagonal envelope of {@code geom}.
   */
  public static @Nullable Geometry ST_OctagonalEnvelope(Geometry geom) {
    return new OctagonalEnvelope(geom).toGeometry(geom.getFactory());
  }

  /**
   * Expands {@code geom}'s envelope.
   */
  public static Geometry ST_Expand(Geometry geom, BigDecimal deltaX, BigDecimal deltaY) {
    Envelope envelope = geom.getEnvelopeInternal().copy();
    envelope.expandBy(deltaX.doubleValue(), deltaY.doubleValue());
    return geom.getFactory().toGeometry(envelope);
  }

  /**
   * Creates a rectangular Polygon.
   */
  public static Geometry ST_MakeEnvelope(BigDecimal xMin, BigDecimal yMin,
      BigDecimal xMax, BigDecimal yMax, int srid) {
    Geometry geom = ST_GeomFromText("POLYGON(("
        + xMin + " " + yMin + ", "
        + xMin + " " + yMax + ", "
        + xMax + " " + yMax + ", "
        + xMax + " " + yMin + ", "
        + xMin + " " + yMin + "))", srid);
    return Objects.requireNonNull(geom, "geom");
  }

  /**
   * Creates a rectangular Polygon.
   */
  public static Geometry ST_MakeEnvelope(BigDecimal xMin, BigDecimal yMin,
      BigDecimal xMax, BigDecimal yMax) {
    return ST_MakeEnvelope(xMin, yMin, xMax, yMax, NO_SRID);
  }

  /**
   * Creates a line-string from the given POINTs (or MULTIPOINTs).
   */
  @Hints({"SqlKind:ST_MAKE_LINE"})
  public static Geometry ST_MakeLine(Geometry geom1, Geometry geom2) {
    return GEOMETRY_FACTORY.createLineString(new Coordinate[]{
        geom1.getCoordinate(),
        geom2.getCoordinate(),
    });
  }

  @Hints({"SqlKind:ST_MAKE_LINE"})
  public static Geometry ST_MakeLine(Geometry geom1, Geometry geom2, Geometry geom3) {
    return GEOMETRY_FACTORY.createLineString(new Coordinate[]{
        geom1.getCoordinate(),
        geom2.getCoordinate(),
        geom3.getCoordinate(),
    });
  }

  @Hints({"SqlKind:ST_MAKE_LINE"})
  public static Geometry ST_MakeLine(Geometry geom1, Geometry geom2, Geometry geom3,
      Geometry geom4) {
    return GEOMETRY_FACTORY.createLineString(new Coordinate[]{
        geom1.getCoordinate(),
        geom2.getCoordinate(),
        geom3.getCoordinate(),
        geom4.getCoordinate(),
    });
  }

  @Hints({"SqlKind:ST_MAKE_LINE"})
  public static Geometry ST_MakeLine(Geometry geom1, Geometry geom2, Geometry geom3,
      Geometry geom4, Geometry geom5) {
    return GEOMETRY_FACTORY.createLineString(new Coordinate[]{
        geom1.getCoordinate(),
        geom2.getCoordinate(),
        geom3.getCoordinate(),
        geom4.getCoordinate(),
        geom5.getCoordinate(),
    });
  }

  @Hints({"SqlKind:ST_MAKE_LINE"})
  public static Geometry ST_MakeLine(Geometry geom1, Geometry geom2, Geometry geom3,
      Geometry geom4, Geometry geom5, Geometry geom6) {
    return GEOMETRY_FACTORY.createLineString(new Coordinate[]{
        geom1.getCoordinate(),
        geom2.getCoordinate(),
        geom3.getCoordinate(),
        geom4.getCoordinate(),
        geom5.getCoordinate(),
        geom6.getCoordinate(),
    });
  }

  /**
   * Alias for {@link #ST_Point(BigDecimal, BigDecimal)}.
   */
  @Hints({"SqlKind:ST_POINT"})
  public static Geometry ST_MakePoint(BigDecimal x, BigDecimal y) {
    return ST_Point(x, y);
  }

  /**
   * Alias for {@link #ST_Point(BigDecimal, BigDecimal, BigDecimal)}.
   */
  @Hints({"SqlKind:ST_POINT3"})
  public static Geometry ST_MakePoint(BigDecimal x, BigDecimal y, BigDecimal z) {
    return ST_Point(x, y, z);
  }

  /**
   * Constructs a 2D point from coordinates.
   */
  @Hints({"SqlKind:ST_POINT"})
  public static Geometry ST_Point(BigDecimal x, BigDecimal y) {
    // NOTE: Combine the double and BigDecimal variants of this function
    return GEOMETRY_FACTORY.createPoint(new Coordinate(x.doubleValue(), y.doubleValue()));
  }

  /**
   * Constructs a 3D point from coordinates.
   */
  @Hints({"SqlKind:ST_POINT3"})
  public static Geometry ST_Point(BigDecimal x, BigDecimal y, BigDecimal z) {
    final Geometry g = GEOMETRY_FACTORY.createPoint(
        new Coordinate(x.doubleValue(), y.doubleValue(),
            z.doubleValue()));
    return g;
  }

  // Geometry properties (2D)

  /**
   * Returns the minimum bounding box that encloses geom as a Geometry.
   */
  public static @Nullable Geometry ST_Extent(Geometry geom) {
    // Note: check whether the extent and the envelope are the same.
    return geom.getEnvelope();
  }

  /**
   * Returns the nth geometry of a geometry collection.
   */
  public static @Nullable Geometry ST_GeometryN(Geometry geom, int n) {
    if (!(geom instanceof GeometryCollection)) {
      return null;
    }
    return geom.getGeometryN(n);
  }

  /**
   * Returns the exterior ring of {@code geom}, or null if {@code geom} is not a polygon.
   */
  public static @Nullable Geometry ST_ExteriorRing(Geometry geom) {
    if (geom instanceof Polygon) {
      return ((Polygon) geom).getExteriorRing();
    }
    return null;
  }

  /**
   * Returns the first point of {@code geom}.
   */
  public static Geometry ST_EndPoint(Geometry geom) {
    return ST_PointN(geom, -1);
  }

  /**
   * Returns the nth interior ring of {@code geom}, or null if {@code geom} is not a polygon.
   */
  public static @Nullable Geometry ST_InteriorRing(Geometry geom, int n) {
    if (geom instanceof Polygon) {
      return ((Polygon) geom).getInteriorRingN(n);
    }
    return null;
  }

  /**
   * Returns whether {@code geom} is a closed LINESTRING or MULTILINESTRING.
   */
  public static boolean ST_IsClosed(Geometry geom) {
    if (geom instanceof LineString) {
      return ((LineString) geom).isClosed();
    }
    if (geom instanceof MultiLineString) {
      return ((MultiLineString) geom).isClosed();
    }
    return false;
  }

  /**
   * Returns whether {@code geom} has at least one z-coordinate.
   */
  public static boolean ST_Is3D(Geometry geom) {
    return ST_CoordDim(geom) == 3;
  }

  /**
   * Returns true if geom is empty.
   */
  public static boolean ST_IsEmpty(Geometry geom) {
    return geom.isEmpty();
  }

  /**
   * Returns true if geom is rectangle.
   */
  public static boolean ST_IsRectangle(Geometry geom) {
    return geom.isRectangle();
  }

  /**
   * Returns whether {@code geom} is a closed and simple linestring or multi-linestring.
   */
  public static boolean ST_IsRing(Geometry geom) {
    if (geom instanceof LineString) {
      return ((LineString) geom).isClosed() && geom.isSimple();
    }
    if (geom instanceof MultiLineString) {
      return ((MultiLineString) geom).isClosed() && geom.isSimple();
    }
    return false;
  }

  /**
   * Returns true if geom is simple.
   */
  public static boolean ST_IsSimple(Geometry geom) {
    return geom.isSimple();
  }

  /**
   * Returns true if geom is valid.
   */
  public static boolean ST_IsValid(Geometry geom) {
    return geom.isValid();
  }

  /**
   * Returns the number of points in {@code geom}.
   */
  public static int ST_NPoints(Geometry geom) {
    return ST_NumPoints(geom);
  }

  /**
   * Returns the number of geometries in {@code geom} (1 if it is not a GEOMETRYCOLLECTION).
   */
  public static int ST_NumGeometries(Geometry geom) {
    return geom.getNumGeometries();
  }

  /**
   * Returns the number of interior rings of {@code geom}.
   */
  public static int ST_NumInteriorRing(Geometry geom) {
    return ST_NumInteriorRings(geom);
  }

  /**
   * Returns the number of interior rings of {@code geom}.
   */
  public static int ST_NumInteriorRings(Geometry geom) {
    int[] num = new int[]{0};
    geom.apply(new GeometryFilter() {
      @Override public void filter(Geometry geom) {
        if (geom instanceof Polygon) {
          num[0] += ((Polygon) geom).getNumInteriorRing();
        }
      }
    });
    return num[0];
  }

  /**
   * Returns the number of points in {@code geom}.
   */
  public static int ST_NumPoints(Geometry geom) {
    return geom.getCoordinates().length;
  }

  /**
   * Returns the nth point of a {@code geom}.
   */
  public static Geometry ST_PointN(Geometry geom, int n) {
    Coordinate[] coordinates = geom.getCoordinates();
    int i = (coordinates.length + (n % coordinates.length)) % coordinates.length;
    return geom.getFactory().createPoint(coordinates[i]);
  }

  /**
   * Returns an interior or boundary point of {@code geom}.
   */
  public static Geometry ST_PointOnSurface(Geometry geom) {
    return geom.getFactory().createPoint(InteriorPoint.getInteriorPoint(geom));
  }

  /**
   * Returns SRID value or 0 if input Geometry does not have one.
   */
  public static int ST_SRID(Geometry geom) {
    return geom.getSRID();
  }

  /**
   * Returns the first point of {@code geom}.
   */
  public static Geometry ST_StartPoint(Geometry geom) {
    return ST_PointN(geom, 0);
  }

  /**
   * Return the X coordinate of the point, or NULL if not available. Input must be a point..
   */
  public static @Nullable Double ST_X(Geometry geom) {
    return geom instanceof Point ? ((Point) geom).getX() : null;
  }

  /**
   * Returns the X maxima of a 2D or 3D bounding box or a geometry.
   */
  public static @Nullable Double ST_XMax(Geometry geom) {
    return geom.getEnvelopeInternal().getMaxX();
  }

  /**
   * Returns the X minima of a 2D or 3D bounding box or a geometry.
   */
  public static @Nullable Double ST_XMin(Geometry geom) {
    return geom.getEnvelopeInternal().getMinX();
  }

  /**
   * Returns the y-value of the first coordinate of {@code geom}.
   */
  public static @Nullable Double ST_Y(Geometry geom) {
    return geom instanceof Point ? ((Point) geom).getY() : null;
  }

  /**
   * Returns the Y maxima of a 2D or 3D bounding box or a geometry.
   */
  public static @Nullable Double ST_YMax(Geometry geom) {
    return geom.getEnvelopeInternal().getMaxY();
  }

  /**
   * Returns the Y minima of a 2D or 3D bounding box or a geometry.
   */
  public static @Nullable Double ST_YMin(Geometry geom) {
    return geom.getEnvelopeInternal().getMinY();
  }

  /**
   * Returns the z-value of the first coordinate of {@code geom}.
   */
  public static Double ST_Z(Geometry geom) {
    if (geom.getCoordinate() != null) {
      return geom.getCoordinate().getZ();
    } else {
      return Double.NaN;
    }

  }

  /**
   * Returns the maximum z-value of {@code geom}.
   */
  public static Double ST_ZMax(Geometry geom) {
    return Arrays.stream(geom.getCoordinates())
        .filter(c -> !Double.isNaN(c.getZ()))
        .map(c -> c.getZ())
        .max(Double::compareTo)
        .orElse(Double.NaN);
  }

  /**
   * Returns the minimum z-value of {@code geom}.
   */
  public static Double ST_ZMin(Geometry geom) {
    return Arrays.stream(geom.getCoordinates())
        .filter(c -> !Double.isNaN(c.getZ()))
        .map(c -> c.getZ())
        .min(Double::compareTo)
        .orElse(Double.NaN);
  }

  // Geometry properties (2D)

  /**
   * Returns the boundary of {@code geom}.
   */
  public static Geometry ST_Boundary(Geometry geom) {
    return geom.getBoundary();
  }

  /**
   * Returns the centroid of {@code geom}.
   */
  public static Geometry ST_Centroid(Geometry geom) {
    return geom.getCentroid();
  }

  /**
   * Returns the dimension of the coordinates of {@code geom}.
   */
  public static int ST_CoordDim(Geometry geom) {
    Coordinate coordinate = geom.getCoordinate();
    if (coordinate != null && !Double.isNaN(coordinate.getZ())) {
      return 3;
    }
    return 2;
  }

  /**
   * Returns the dimension of {@code geom}.
   */
  public static int ST_Dimension(Geometry geom) {
    return geom.getDimension();
  }

  /**
   * Returns the distance between {@code geom1} and {@code geom2}.
   */
  public static double ST_Distance(Geometry geom1, Geometry geom2) {
    return geom1.distance(geom2);
  }

  /**
   * Returns the type of {@code geom}.
   */
  public static String ST_GeometryType(Geometry geom) {
    return SpatialType.fromGeometry(geom).name();
  }

  /**
   * Returns the OGC SFS type code of {@code geom}.
   */
  public static int ST_GeometryTypeCode(Geometry geom) {
    return SpatialType.fromGeometry(geom).code();
  }

  /**
   * Returns the minimum bounding box of {@code geom} (which may be a GEOMETRYCOLLECTION).
   */
  public static Geometry ST_Envelope(Geometry geom) {
    return geom.getEnvelope();
  }

  // Geometry predicates ======================================================

  /**
   * Returns whether {@code geom1} contains {@code geom2}.
   */
  @Hints({"SqlKind:ST_CONTAINS"})
  public static boolean ST_Contains(Geometry geom1, Geometry geom2) {
    return geom1.contains(geom2);
  }

  /**
   * Returns whether {@code geom1} contains {@code geom2} but does not intersect its boundary.
   */
  public static boolean ST_ContainsProperly(Geometry geom1, Geometry geom2) {
    return geom1.contains(geom2)
        && !geom1.crosses(geom2);
  }

  /**
   * Returns whether no point in {@code geom1} is outside {@code geom2}.
   */
  public static boolean ST_CoveredBy(Geometry geom1, Geometry geom2) {
    return geom1.coveredBy(geom2);
  }

  /**
   * Returns whether no point in {@code geom2} is outside {@code geom1}.
   */
  public static boolean ST_Covers(Geometry geom1, Geometry geom2) {
    return geom1.covers(geom2);
  }

  /**
   * Returns whether {@code geom1} crosses {@code geom2}.
   */
  public static boolean ST_Crosses(Geometry geom1, Geometry geom2) {
    return geom1.crosses(geom2);
  }

  /**
   * Returns whether {@code geom1} and {@code geom2} are disjoint.
   */
  public static boolean ST_Disjoint(Geometry geom1, Geometry geom2) {
    return geom1.disjoint(geom2);
  }

  /**
   * Returns whether the envelope of {@code geom1} intersects the envelope of {@code geom2}.
   */
  public static boolean ST_EnvelopesIntersect(Geometry geom1, Geometry geom2) {
    final Geometry e1 = geom1.getEnvelope();
    final Geometry e2 = geom2.getEnvelope();
    return e1.intersects(e2);
  }

  /**
   * Returns whether {@code geom1} equals {@code geom2}.
   */
  public static boolean ST_Equals(Geometry geom1, Geometry geom2) {
    return geom1.equals(geom2);
  }

  /**
   * Returns whether {@code geom1} intersects {@code geom2}.
   */
  public static boolean ST_Intersects(Geometry geom1, Geometry geom2) {
    return geom1.intersects(geom2);
  }

  /**
   * Returns whether {@code geom1} equals {@code geom2} and their coordinates and component
   * Geometries are listed in the same order.
   */
  public static boolean ST_OrderingEquals(Geometry geom1, Geometry geom2) {
    return geom1.equals(geom2);
  }

  /**
   * Returns {@code geom1} overlaps {@code geom2}.
   */
  public static boolean ST_Overlaps(Geometry geom1, Geometry geom2) {
    return geom1.overlaps(geom2);
  }

  /**
   * Returns whether {@code geom1} touches {@code geom2}.
   */
  public static boolean ST_Touches(Geometry geom1, Geometry geom2) {
    return geom1.touches(geom2);
  }

  /**
   * Returns whether {@code geom1} is within {@code geom2}.
   */
  public static boolean ST_Within(Geometry geom1, Geometry geom2) {
    return geom1.within(geom2);
  }

  /**
   * Returns whether {@code geom1} and {@code geom2} are within {@code distance} of each other.
   */
  @Hints({"SqlKind:ST_DWITHIN"})
  public static boolean ST_DWithin(Geometry geom1, Geometry geom2, double distance) {
    final double distance1 = geom1.distance(geom2);
    return distance1 <= distance;
  }

  // Geometry operators (2D and 3D) ===========================================

  /**
   * Computes a buffer around {@code geom}.
   */
  public static Geometry ST_Buffer(Geometry geom, double distance) {
    return geom.buffer(distance);
  }

  /**
   * Computes a buffer around {@code geom}.
   */
  public static Geometry ST_Buffer(Geometry geom, double distance, int quadSegs) {
    return geom.buffer(distance, quadSegs);
  }

  /**
   * Computes a buffer around {@code geom}.
   */
  public static Geometry ST_Buffer(Geometry geom, double distance, int quadSegs, int endCapStyle) {
    return geom.buffer(distance, quadSegs, endCapStyle);
  }

  /**
   * Computes the smallest convex POLYGON that contains all the points of geom.
   */
  public static Geometry ST_ConvexHull(Geometry geom) {
    return geom.convexHull();
  }

  /**
   * Computes the difference between geom1 and geom2.
   */
  public static Geometry ST_Difference(Geometry geom1, Geometry geom2) {
    return geom1.difference(geom2);
  }

  /**
   * Computes the symmetric difference between geom1 and geom2.
   */
  public static Geometry ST_SymDifference(Geometry geom1, Geometry geom2) {
    return geom1.symDifference(geom2);
  }

  /**
   * Computes the intersection between geom1 and geom2.
   */
  public static Geometry ST_Intersection(Geometry geom1, Geometry geom2) {
    return geom1.intersection(geom2);
  }

  /**
   * Returns the DE-9IM intersection matrix for geom1 and geom2.
   */
  public static String ST_Relate(Geometry geom1, Geometry geom2) {
    return geom1.relate(geom2).toString();
  }

  /**
   * Returns true if geom1 and geom2 are related by the intersection matrix specified by iMatrix.
   */
  public static boolean ST_Relate(Geometry geom1, Geometry geom2, String iMatrix) {
    return geom1.relate(geom2, iMatrix);
  }

  /**
   * Computes the union of {@code geom1} and {@code geom2}.
   */
  public static Geometry ST_Union(Geometry geom1, Geometry geom2) {
    return geom1.union(geom2);
  }

  /**
   * Computes the union of the geometries in {@code geomCollection}.
   */
  @SemiStrict public static Geometry ST_Union(Geometry geomCollection) {
    return geomCollection.union();
  }

  // Geometry projection functions ============================================

  /**
   * Transforms {@code geom} from one coordinate reference system (CRS) to the CRS specified by
   * {@code srid}.
   */
  public static Geometry ST_Transform(Geometry geom, int srid) {
    ProjectionTransformer projectionTransformer =
        new ProjectionTransformer(geom.getSRID(), srid);
    return projectionTransformer.transform(geom);
  }

  /**
   * Returns a copy of {@code geom} with a new SRID.
   */
  public static Geometry ST_SetSRID(Geometry geom, int srid) {
    geom.setSRID(srid);
    return geom;
  }

  // Process Geometries

  /**
   * Merges a collection of linear components to form a line-string of maximal length.
   */
  public static Geometry ST_LineMerge(Geometry geom) {
    LineMerger merger = new LineMerger();
    merger.add(geom);
    LineString[] geometries = ((Stream<Object>) merger.getMergedLineStrings().stream())
        .map(LineString.class::cast)
        .toArray(size -> new LineString[size]);
    return GEOMETRY_FACTORY.createMultiLineString(geometries);
  }

  /**
   * Makes a valid geometry of a given invalid geometry.
   */
  public static Geometry ST_MakeValid(Geometry geometry) {
    return new GeometryFixer(geometry).getResult();
  }

  /**
   * Creates a multipolygon from the geometry.
   */
  public static Geometry ST_Polygonize(Geometry geometry) {
    Polygonizer polygonizer = new Polygonizer(true);
    polygonizer.add(geometry);
    return polygonizer.getGeometry();
  }

  /**
   * Reduces the geometry's precision to n decimal places.
   */
  public static Geometry ST_PrecisionReducer(Geometry geometry, BigDecimal decimal) {
    double scale = Math.pow(10, decimal.doubleValue());
    PrecisionModel precisionModel = new PrecisionModel(scale);
    GeometryPrecisionReducer precisionReducer = new GeometryPrecisionReducer(precisionModel);
    return precisionReducer.reduce(geometry);
  }

  /**
   * Simplifies geom a geometry using the Douglas-Peuker algorithm.
   */
  public static Geometry ST_Simplify(Geometry geom, BigDecimal distance) {
    DouglasPeuckerSimplifier simplifier = new DouglasPeuckerSimplifier(geom);
    simplifier.setDistanceTolerance(distance.doubleValue());
    return simplifier.getResultGeometry();
  }

  /**
   * Simplifies a geometry and preserves its topology.
   */
  public static Geometry ST_SimplifyPreserveTopology(Geometry geom, BigDecimal distance) {
    TopologyPreservingSimplifier simplifier = new TopologyPreservingSimplifier(geom);
    simplifier.setDistanceTolerance(distance.doubleValue());
    return simplifier.getResultGeometry();
  }

  /**
   * Snaps geom1 and geom2 together with the given snapTolerance.
   */
  public static Geometry ST_Snap(Geometry geom1, Geometry geom2, BigDecimal snapTolerance) {
    GeometrySnapper snapper = new GeometrySnapper(geom1);
    return snapper.snapTo(geom2, snapTolerance.doubleValue());
  }

  // Affine transformation functions (3D and 2D)

  /**
   * Rotates geom counter-clockwise by angle (in radians) about the point origin.
   */
  public static Geometry ST_Rotate(Geometry geom, BigDecimal angle) {
    AffineTransformation transformation = new AffineTransformation();
    transformation.rotate(angle.doubleValue());
    return transformation.transform(geom);
  }

  /**
   * Rotates geom counter-clockwise by angle (in radians) about the point origin.
   */
  public static Geometry ST_Rotate(Geometry geom, BigDecimal angle, Geometry origin) {
    // Note: check whether we can add support for the Point type.
    if (!(origin instanceof Point)) {
      throw new RuntimeException("The origin must be a point");
    }
    Point point = (Point) origin;
    AffineTransformation transformation = new AffineTransformation();
    transformation.rotate(angle.doubleValue(), point.getX(), point.getY());
    return transformation.transform(geom);
  }

  /**
   * Rotates geom counter-clockwise by angle (in radians) about the point origin.
   */
  public static Geometry ST_Rotate(Geometry geom, BigDecimal angle, BigDecimal x, BigDecimal y) {
    AffineTransformation transformation = new AffineTransformation();
    transformation.rotate(angle.doubleValue(), x.doubleValue(), y.doubleValue());
    return transformation.transform(geom);
  }

  /**
   * Scales geom Geometry by multiplying the ordinates by the indicated scale factors.
   */
  public static Geometry ST_Scale(Geometry geom, BigDecimal xFactor, BigDecimal yFactor) {
    AffineTransformation transformation = new AffineTransformation();
    transformation.scale(xFactor.doubleValue(), yFactor.doubleValue());
    return transformation.transform(geom);
  }

  /**
   * Translates geom by the vector (x, y).
   */
  public static Geometry ST_Translate(Geometry geom, BigDecimal x, BigDecimal y) {
    AffineTransformation transformation = new AffineTransformation();
    transformation.translate(x.doubleValue(), y.doubleValue());
    return transformation.transform(geom);
  }

  // Space-filling curves

  /**
   * Returns the position of a point on the Hilbert curve, or null if it is not a 2-dimensional
   * point.
   */
  @Hints({"SqlKind:HILBERT"})
  public static @Nullable Long hilbert(Geometry geom) {
    if (geom instanceof Point) {
      final double x = ((Point) geom).getX();
      final double y = ((Point) geom).getY();
      return new HilbertCurve2D(8).toIndex(x, y);
    }
    return null;
  }

  /**
   * Returns the position of a point on the Hilbert curve.
   */
  @Hints({"SqlKind:HILBERT"})
  public static long hilbert(BigDecimal x, BigDecimal y) {
    return new HilbertCurve2D(8).toIndex(x.doubleValue(), y.doubleValue());
  }


  // Inner classes ============================================================


  /**
   * Used at run time by the {@link #ST_MakeGrid} and {@link #ST_MakeGridPoints} functions.
   */
  public static class GridEnumerable extends AbstractEnumerable<Object[]> {
    private final Envelope envelope;
    private final boolean point;
    private final double deltaX;
    private final double deltaY;
    private final double minX;
    private final double minY;
    private final int baseX;
    private final int baseY;
    private final int spanX;
    private final int spanY;
    private final int area;

    public GridEnumerable(Envelope envelope, BigDecimal deltaX,
        BigDecimal deltaY, boolean point) {
      this.envelope = envelope;
      this.deltaX = deltaX.doubleValue();
      this.deltaY = deltaY.doubleValue();
      this.point = point;
      this.spanX = (int) Math.floor((envelope.getMaxX() - envelope.getMinX())
          / this.deltaX) + 1;
      this.baseX = (int) Math.floor(envelope.getMinX() / this.deltaX);
      this.minX = this.deltaX * baseX;
      this.spanY = (int) Math.floor((envelope.getMaxY() - envelope.getMinY())
          / this.deltaY) + 1;
      this.baseY = (int) Math.floor(envelope.getMinY() / this.deltaY);
      this.minY = this.deltaY * baseY;
      this.area = this.spanX * this.spanY;
    }

    @Override public Enumerator<Object[]> enumerator() {
      return new Enumerator<Object[]>() {
        int id = -1;

        @Override public Object[] current() {
          final Geometry geom;
          final int x = id % spanX;
          final int y = id / spanX;
          if (point) {
            final double xCurrent = minX + (x + 0.5D) * deltaX;
            final double yCurrent = minY + (y + 0.5D) * deltaY;
            geom = ST_MakePoint(BigDecimal.valueOf(xCurrent),
                BigDecimal.valueOf(yCurrent));
          } else {
            final double left = minX + x * deltaX;
            final double right = left + deltaX;
            final double bottom = minY + y * deltaY;
            final double top = bottom + deltaY;

            Coordinate[] coordinates = new Coordinate[]{
                new Coordinate(left, bottom),
                new Coordinate(left, top),
                new Coordinate(right, top),
                new Coordinate(right, bottom),
                new Coordinate(left, bottom)
            };

            LinearRing linearRing = GEOMETRY_FACTORY.createLinearRing(coordinates);
            Polygon polygon = GEOMETRY_FACTORY.createPolygon(linearRing);

            geom = polygon;
          }
          return new Object[]{geom, id, x + 1, y + 1, baseX + x, baseY + y};
        }

        @Override public boolean moveNext() {
          return ++id < area;
        }

        @Override public void reset() {
          id = -1;
        }

        @Override public void close() {
        }
      };
    }
  }

}
