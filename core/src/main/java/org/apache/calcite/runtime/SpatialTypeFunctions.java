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

import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.function.Deterministic;
import org.apache.calcite.linq4j.function.Experimental;
import org.apache.calcite.linq4j.function.Hints;
import org.apache.calcite.linq4j.function.SemiStrict;
import org.apache.calcite.linq4j.function.Strict;
import org.apache.calcite.runtime.SpatialTypeUtils.SpatialType;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import java.math.BigDecimal;
import java.util.Objects;

import static org.apache.calcite.runtime.SpatialTypeUtils.GEOMETRY_FACTORY;
import static org.apache.calcite.runtime.SpatialTypeUtils.NO_SRID;
import static org.apache.calcite.runtime.SpatialTypeUtils.asEwkt;
import static org.apache.calcite.runtime.SpatialTypeUtils.asWkt;
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

  private SpatialTypeFunctions() {}

  // Geometry conversion functions (2D and 3D) ================================

  public static @Nullable String ST_AsEWKT(Geometry g) {
    return asEwkt(g);
  }

  public static @Nullable String ST_AsText(Geometry g) {
    return asWkt(g);
  }

  public static @Nullable String ST_AsWKT(Geometry g) {
    return asWkt(g);
  }

  public static @Nullable Geometry ST_GeomFromText(String s) {
    return fromWkt(s);
  }

  public static @Nullable Geometry ST_GeomFromText(String s, int srid) {
    final Geometry g = fromWkt(s);
    g.setSRID(srid);
    return g;
  }

  public static @Nullable Geometry ST_LineFromText(String s) {
    return ST_GeomFromText(s, NO_SRID);
  }

  public static @Nullable Geometry ST_LineFromText(String wkt, int srid) {
    final Geometry g = fromWkt(wkt);
    g.setSRID(srid);
    return g == null ? null : g;
  }

  public static @Nullable Geometry ST_MPointFromText(String s) {
    return ST_GeomFromText(s, NO_SRID);
  }

  public static @Nullable Geometry ST_MPointFromText(String wkt, int srid) {
    final Geometry g = fromWkt(wkt);
    g.setSRID(srid);
    return g == null ? null : g;
  }

  public static @Nullable Geometry ST_PointFromText(String s) {
    return ST_GeomFromText(s, NO_SRID);
  }

  public static @Nullable Geometry ST_PointFromText(String wkt, int srid) {
    final Geometry g = fromWkt(wkt);
    g.setSRID(srid);
    return g == null ? null : g;
  }

  public static @Nullable Geometry ST_PolyFromText(String s) {
    return ST_GeomFromText(s, NO_SRID);
  }

  public static @Nullable Geometry ST_PolyFromText(String wkt, int srid) {
    final Geometry g = fromWkt(wkt);
    g.setSRID(srid);
    return g == null ? null : g;
  }

  public static @Nullable Geometry ST_MLineFromText(String s) {
    return ST_GeomFromText(s, NO_SRID);
  }

  public static @Nullable Geometry ST_MLineFromText(String wkt, int srid) {
    final Geometry g = fromWkt(wkt);
    g.setSRID(srid);
    return g == null ? null : g;
  }

  public static @Nullable Geometry ST_MPolyFromText(String s) {
    return ST_GeomFromText(s, NO_SRID);
  }

  public static @Nullable Geometry ST_MPolyFromText(String wkt, int srid) {
    final Geometry g = fromWkt(wkt);
    g.setSRID(srid);
    return g == null ? null : g;
  }

  // Geometry creation functions ==============================================

  /** Calculates a regular grid of polygons based on {@code geom}. */
  private static void ST_MakeGrid(final Geometry geom,
      final BigDecimal deltaX, final BigDecimal deltaY) {
    // This is a dummy function. We cannot include table functions in this
    // package, because they have too many dependencies. See the real definition
    // in SqlSpatialTypeFunctions.
  }

  /** Calculates a regular grid of points based on {@code geom}. */
  private static void ST_MakeGridPoints(final Geometry geom,
      final BigDecimal deltaX, final BigDecimal deltaY) {
    // This is a dummy function. We cannot include table functions in this
    // package, because they have too many dependencies. See the real definition
    // in SqlSpatialTypeFunctions.
  }

  /** Creates a rectangular Polygon. */
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

  /** Creates a rectangular Polygon. */
  public static Geometry ST_MakeEnvelope(BigDecimal xMin, BigDecimal yMin,
      BigDecimal xMax, BigDecimal yMax) {
    return ST_MakeEnvelope(xMin, yMin, xMax, yMax, NO_SRID);
  }

  /** Creates a line-string from the given POINTs (or MULTIPOINTs). */
  @Hints({"SqlKind:ST_MAKE_LINE"})
  public static Geometry ST_MakeLine(Geometry geom1, Geometry geom2) {
    return GEOMETRY_FACTORY.createLineString(new Coordinate[] {
        geom1.getCoordinate(),
        geom2.getCoordinate(),
    });
  }

  @Hints({"SqlKind:ST_MAKE_LINE"})
  public static Geometry ST_MakeLine(Geometry geom1, Geometry geom2, Geometry geom3) {
    return GEOMETRY_FACTORY.createLineString(new Coordinate[] {
        geom1.getCoordinate(),
        geom2.getCoordinate(),
        geom3.getCoordinate(),
    });
  }

  @Hints({"SqlKind:ST_MAKE_LINE"})
  public static Geometry ST_MakeLine(Geometry geom1, Geometry geom2, Geometry geom3,
      Geometry geom4) {
    return GEOMETRY_FACTORY.createLineString(new Coordinate[] {
        geom1.getCoordinate(),
        geom2.getCoordinate(),
        geom3.getCoordinate(),
        geom4.getCoordinate(),
    });
  }

  @Hints({"SqlKind:ST_MAKE_LINE"})
  public static Geometry ST_MakeLine(Geometry geom1, Geometry geom2, Geometry geom3,
      Geometry geom4, Geometry geom5) {
    return GEOMETRY_FACTORY.createLineString(new Coordinate[] {
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
    return GEOMETRY_FACTORY.createLineString(new Coordinate[] {
        geom1.getCoordinate(),
        geom2.getCoordinate(),
        geom3.getCoordinate(),
        geom4.getCoordinate(),
        geom5.getCoordinate(),
        geom6.getCoordinate(),
    });
  }

  /** Alias for {@link #ST_Point(BigDecimal, BigDecimal)}. */
  @Hints({"SqlKind:ST_POINT"})
  public static Geometry ST_MakePoint(BigDecimal x, BigDecimal y) {
    return ST_Point(x, y);
  }

  /** Alias for {@link #ST_Point(BigDecimal, BigDecimal, BigDecimal)}. */
  @Hints({"SqlKind:ST_POINT3"})
  public static Geometry ST_MakePoint(BigDecimal x, BigDecimal y, BigDecimal z) {
    return ST_Point(x, y, z);
  }

  /** Constructs a 2D point from coordinates. */
  @Hints({"SqlKind:ST_POINT"})
  public static Geometry ST_Point(BigDecimal x, BigDecimal y) {
    // NOTE: Combine the double and BigDecimal variants of this function
    return GEOMETRY_FACTORY.createPoint(new Coordinate(x.doubleValue(), y.doubleValue()));
  }

  /** Constructs a 3D point from coordinates. */
  @Hints({"SqlKind:ST_POINT3"})
  public static Geometry ST_Point(BigDecimal x, BigDecimal y, BigDecimal z) {
    final Geometry g = GEOMETRY_FACTORY.createPoint(
        new Coordinate(x.doubleValue(), y.doubleValue(),
        z.doubleValue()));
    return g;
  }

  // Geometry properties (2D and 3D) ==========================================

  /** Returns whether {@code geom} has at least one z-coordinate. */
  public static boolean ST_Is3D(Geometry geom) {
    for (Coordinate coordinate : geom.getCoordinates()) {
      if (!Double.isNaN(coordinate.getZ())) {
        return true;
      }
    }
    return false;
  }

  /** Returns the x-value of the first coordinate of {@code geom}. */
  public static @Nullable Double ST_X(Geometry geom) {
    return geom instanceof Point ? ((Point) geom).getX() : null;
  }

  /** Returns the y-value of the first coordinate of {@code geom}. */
  public static @Nullable Double ST_Y(Geometry geom) {
    return geom instanceof Point ? ((Point) geom).getY() : null;
  }

  /** Returns the z-value of the first coordinate of {@code geom}. */
  public static @Nullable Double ST_Z(Geometry geom) {
    return geom instanceof Point
        && !Double.isNaN(geom.getCoordinate().getZ())
        ? geom.getCoordinate().getZ() : null;
  }

  /** Returns the boundary of {@code geom}. */
  public static Geometry ST_Boundary(Geometry geom) {
    return geom.getBoundary();
  }

  /** Returns the distance between {@code geom1} and {@code geom2}. */
  public static double ST_Distance(Geometry geom1, Geometry geom2) {
    return geom1.distance(geom2);
  }

  /** Returns the type of {@code geom}. */
  public static String ST_GeometryType(Geometry geom) {
    return SpatialType.fromGeometry(geom).name();
  }

  /** Returns the OGC SFS type code of {@code geom}. */
  public static int ST_GeometryTypeCode(Geometry geom) {
    return SpatialType.fromGeometry(geom).code();
  }

  /** Returns the minimum bounding box of {@code geom} (which may be a
   *  GEOMETRYCOLLECTION). */
  public static Geometry ST_Envelope(Geometry geom) {
    return geom.getEnvelope();
  }

  // Geometry predicates ======================================================

  /** Returns whether {@code geom1} contains {@code geom2}. */
  @Hints({"SqlKind:ST_CONTAINS"})
  public static boolean ST_Contains(Geometry geom1, Geometry geom2) {
    return geom1.contains(geom2);
  }

  /** Returns whether {@code geom1} contains {@code geom2} but does not
   * intersect its boundary. */
  public static boolean ST_ContainsProperly(Geometry geom1, Geometry geom2) {
    return geom1.contains(geom2)
        && !geom1.crosses(geom2);
  }

  /** Returns whether no point in {@code geom2} is outside {@code geom1}. */
  private static boolean ST_Covers(Geometry geom1, Geometry geom2)  {
    return geom1.covers(geom2);
  }

  /** Returns whether {@code geom1} crosses {@code geom2}. */
  public static boolean ST_Crosses(Geometry geom1, Geometry geom2)  {
    return geom1.crosses(geom2);
  }

  /** Returns whether {@code geom1} and {@code geom2} are disjoint. */
  public static boolean ST_Disjoint(Geometry geom1, Geometry geom2)  {
    return geom1.disjoint(geom2);
  }

  /** Returns whether the envelope of {@code geom1} intersects the envelope of
   *  {@code geom2}. */
  public static boolean ST_EnvelopesIntersect(Geometry geom1, Geometry geom2)  {
    final Geometry e1 = geom1.getEnvelope();
    final Geometry e2 = geom2.getEnvelope();
    return e1.intersects(e2);
  }

  /** Returns whether {@code geom1} equals {@code geom2}. */
  public static boolean ST_Equals(Geometry geom1, Geometry geom2)  {
    return geom1.equals(geom2);
  }

  /** Returns whether {@code geom1} intersects {@code geom2}. */
  public static boolean ST_Intersects(Geometry geom1, Geometry geom2)  {
    return geom1.intersects(geom2);
  }

  /** Returns whether {@code geom1} equals {@code geom2} and their coordinates
   * and component Geometries are listed in the same order. */
  public static boolean ST_OrderingEquals(Geometry geom1, Geometry geom2)  {
    return geom1.equals(geom2);
  }

  /** Returns {@code geom1} overlaps {@code geom2}. */
  public static boolean ST_Overlaps(Geometry geom1, Geometry geom2)  {
    return geom1.overlaps(geom2);
  }

  /** Returns whether {@code geom1} touches {@code geom2}. */
  public static boolean ST_Touches(Geometry geom1, Geometry geom2)  {
    return geom1.touches(geom2);
  }

  /** Returns whether {@code geom1} is within {@code geom2}. */
  public static boolean ST_Within(Geometry geom1, Geometry geom2)  {
    return geom1.within(geom2);
  }

  /** Returns whether {@code geom1} and {@code geom2} are within
   * {@code distance} of each other. */
  @Hints({"SqlKind:ST_DWITHIN"})
  public static boolean ST_DWithin(Geometry geom1, Geometry geom2, double distance) {
    final double distance1 = geom1.distance(geom2);
    return distance1 <= distance;
  }

  // Geometry operators (2D and 3D) ===========================================

  /** Computes a buffer around {@code geom}. */
  public static Geometry ST_Buffer(Geometry geom, double distance) {
    return geom.buffer(distance);
  }

  /** Computes a buffer around {@code geom}. */
  public static Geometry ST_Buffer(Geometry geom, double distance, int quadSegs) {
    return geom.buffer(distance, quadSegs);
  }

  /** Computes a buffer around {@code geom}. */
  public static Geometry ST_Buffer(Geometry geom, double distance, int quadSegs, int endCapStyle) {
    return geom.buffer(distance, quadSegs, endCapStyle);
  }

  /** Computes the union of {@code geom1} and {@code geom2}. */
  public static Geometry ST_Union(Geometry geom1, Geometry geom2) {
    return geom1.union(geom2);
  }

  /** Computes the union of the geometries in {@code geomCollection}. */
  @SemiStrict public static Geometry ST_Union(Geometry geomCollection) {
    return geomCollection.union();
  }

  // Geometry projection functions ============================================

  /** Transforms {@code geom} from one coordinate reference
   * system (CRS) to the CRS specified by {@code srid}. */
  public static Geometry ST_Transform(Geometry geom, int srid) {
    ProjectionTransformer projectionTransformer =
        new ProjectionTransformer(geom.getSRID(), srid);
    return projectionTransformer.transform(geom);
  }

  /** Returns a copy of {@code geom} with a new SRID. */
  public static Geometry ST_SetSRID(Geometry geom, int srid) {
    geom.setSRID(srid);
    return geom;
  }

  // Space-filling curves

  /** Returns the position of a point on the Hilbert curve,
   * or null if it is not a 2-dimensional point. */
  @Hints({"SqlKind:HILBERT"})
  public static @Nullable Long hilbert(Geometry geom) {
    if (geom instanceof Point) {
      final double x = ((Point) geom).getX();
      final double y = ((Point) geom).getY();
      return new HilbertCurve2D(8).toIndex(x, y);
    }
    return null;
  }

  /** Returns the position of a point on the Hilbert curve. */
  @Hints({"SqlKind:HILBERT"})
  public static long hilbert(BigDecimal x, BigDecimal y) {
    return new HilbertCurve2D(8).toIndex(x.doubleValue(), y.doubleValue());
  }

  // Inner classes ============================================================

  /** Used at run time by the {@link #ST_MakeGrid} and
   * {@link #ST_MakeGridPoints} functions. */
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

            Coordinate[] coordinates = new Coordinate[] {
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
          return new Object[] {geom, id, x + 1, y + 1, baseX + x, baseY + y};
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
