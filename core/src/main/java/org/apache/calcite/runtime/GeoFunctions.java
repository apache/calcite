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
import org.apache.calcite.runtime.Geometries.CapStyle;
import org.apache.calcite.runtime.Geometries.Geom;
import org.apache.calcite.runtime.Geometries.JoinStyle;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.Line;
import com.esri.core.geometry.OperatorBoundary;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.Polyline;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.WktExportFlags;
import com.esri.core.geometry.WktImportFlags;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;

import static org.apache.calcite.runtime.Geometries.NO_SRID;
import static org.apache.calcite.runtime.Geometries.bind;
import static org.apache.calcite.runtime.Geometries.buffer;
import static org.apache.calcite.runtime.Geometries.envelope;
import static org.apache.calcite.runtime.Geometries.intersects;
import static org.apache.calcite.runtime.Geometries.makeLine;
import static org.apache.calcite.runtime.Geometries.point;
import static org.apache.calcite.runtime.Geometries.todo;

/**
 * Helper methods to implement Geo-spatial functions in generated code.
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
 *   <li>Make {@link #ST_MakeLine(Geom, Geom)} varargs</li>
 * </ul>
 */
@SuppressWarnings({"UnnecessaryUnboxing", "WeakerAccess", "unused"})
@Deterministic
@Strict
@Experimental
public class GeoFunctions {

  private GeoFunctions() {}

  // Geometry conversion functions (2D and 3D) ================================

  public static String ST_AsText(Geom g) {
    return ST_AsWKT(g);
  }

  public static String ST_AsWKT(Geom g) {
    return GeometryEngine.geometryToWkt(g.g(),
        WktExportFlags.wktExportDefaults);
  }

  public static Geom ST_GeomFromText(String s) {
    return ST_GeomFromText(s, NO_SRID);
  }

  public static Geom ST_GeomFromText(String s, int srid) {
    final Geometry g = GeometryEngine.geometryFromWkt(s,
        WktImportFlags.wktImportDefaults, Geometry.Type.Unknown);
    return bind(g, srid);
  }

  public static Geom ST_LineFromText(String s) {
    return ST_GeomFromText(s, NO_SRID);
  }

  public static Geom ST_LineFromText(String wkt, int srid) {
    final Geometry g = GeometryEngine.geometryFromWkt(wkt,
        WktImportFlags.wktImportDefaults,
        Geometry.Type.Line);
    return bind(g, srid);
  }

  public static Geom ST_MPointFromText(String s) {
    return ST_GeomFromText(s, NO_SRID);
  }

  public static Geom ST_MPointFromText(String wkt, int srid) {
    final Geometry g = GeometryEngine.geometryFromWkt(wkt,
        WktImportFlags.wktImportDefaults,
        Geometry.Type.MultiPoint);
    return bind(g, srid);
  }

  public static Geom ST_PointFromText(String s) {
    return ST_GeomFromText(s, NO_SRID);
  }

  public static Geom ST_PointFromText(String wkt, int srid) {
    final Geometry g = GeometryEngine.geometryFromWkt(wkt,
        WktImportFlags.wktImportDefaults,
        Geometry.Type.Point);
    return bind(g, srid);
  }

  public static Geom ST_PolyFromText(String s) {
    return ST_GeomFromText(s, NO_SRID);
  }

  public static Geom ST_PolyFromText(String wkt, int srid) {
    final Geometry g = GeometryEngine.geometryFromWkt(wkt,
        WktImportFlags.wktImportDefaults,
        Geometry.Type.Polygon);
    return bind(g, srid);
  }

  public static Geom ST_MLineFromText(String s) {
    return ST_GeomFromText(s, NO_SRID);
  }

  public static Geom ST_MLineFromText(String wkt, int srid) {
    final Geometry g = GeometryEngine.geometryFromWkt(wkt,
        WktImportFlags.wktImportDefaults,
        Geometry.Type.Unknown); // NOTE: there is no Geometry.Type.MultiLine
    return bind(g, srid);
  }

  public static Geom ST_MPolyFromText(String s) {
    return ST_GeomFromText(s, NO_SRID);
  }

  public static Geom ST_MPolyFromText(String wkt, int srid) {
    final Geometry g = GeometryEngine.geometryFromWkt(wkt,
        WktImportFlags.wktImportDefaults,
        Geometry.Type.Unknown); // NOTE: there is no Geometry.Type.MultiPolygon
    return bind(g, srid);
  }

  // Geometry creation functions ==============================================

  /** Calculates a regular grid of polygons based on {@code geom}. */
  private static void ST_MakeGrid(final Geom geom,
      final BigDecimal deltaX, final BigDecimal deltaY) {
    // This is a dummy function. We cannot include table functions in this
    // package, because they have too many dependencies. See the real definition
    // in SqlGeoFunctions.
  }

  /** Calculates a regular grid of points based on {@code geom}. */
  private static void ST_MakeGridPoints(final Geom geom,
      final BigDecimal deltaX, final BigDecimal deltaY) {
    // This is a dummy function. We cannot include table functions in this
    // package, because they have too many dependencies. See the real definition
    // in SqlGeoFunctions.
  }

  /** Creates a rectangular Polygon. */
  public static Geom ST_MakeEnvelope(BigDecimal xMin, BigDecimal yMin,
      BigDecimal xMax, BigDecimal yMax, int srid) {
    return ST_GeomFromText("POLYGON(("
        + xMin + " " + yMin + ", "
        + xMin + " " + yMax + ", "
        + xMax + " " + yMax + ", "
        + xMax + " " + yMin + ", "
        + xMin + " " + yMin + "))", srid);
  }

  /** Creates a rectangular Polygon. */
  public static Geom ST_MakeEnvelope(BigDecimal xMin, BigDecimal yMin,
      BigDecimal xMax, BigDecimal yMax) {
    return ST_MakeEnvelope(xMin, yMin, xMax, yMax, NO_SRID);
  }

  /** Creates a line-string from the given POINTs (or MULTIPOINTs). */
  @Hints({"SqlKind:ST_MAKE_LINE"})
  public static Geom ST_MakeLine(Geom geom1, Geom geom2) {
    return makeLine(geom1, geom2);
  }

  @Hints({"SqlKind:ST_MAKE_LINE"})
  public static Geom ST_MakeLine(Geom geom1, Geom geom2, Geom geom3) {
    return makeLine(geom1, geom2, geom3);
  }

  @Hints({"SqlKind:ST_MAKE_LINE"})
  public static Geom ST_MakeLine(Geom geom1, Geom geom2, Geom geom3,
      Geom geom4) {
    return makeLine(geom1, geom2, geom3, geom4);
  }

  @Hints({"SqlKind:ST_MAKE_LINE"})
  public static Geom ST_MakeLine(Geom geom1, Geom geom2, Geom geom3,
      Geom geom4, Geom geom5) {
    return makeLine(geom1, geom2, geom3, geom4, geom5);
  }

  @Hints({"SqlKind:ST_MAKE_LINE"})
  public static Geom ST_MakeLine(Geom geom1, Geom geom2, Geom geom3,
      Geom geom4, Geom geom5, Geom geom6) {
    return makeLine(geom1, geom2, geom3, geom4, geom5, geom6);
  }

  /** Alias for {@link #ST_Point(BigDecimal, BigDecimal)}. */
  @Hints({"SqlKind:ST_POINT"})
  public static Geom ST_MakePoint(BigDecimal x, BigDecimal y) {
    return ST_Point(x, y);
  }

  /** Alias for {@link #ST_Point(BigDecimal, BigDecimal, BigDecimal)}. */
  @Hints({"SqlKind:ST_POINT3"})
  public static Geom ST_MakePoint(BigDecimal x, BigDecimal y, BigDecimal z) {
    return ST_Point(x, y, z);
  }

  /** Constructs a 2D point from coordinates. */
  @Hints({"SqlKind:ST_POINT"})
  public static Geom ST_Point(BigDecimal x, BigDecimal y) {
    // NOTE: Combine the double and BigDecimal variants of this function
    return point(x.doubleValue(), y.doubleValue());
  }

  /** Constructs a 3D point from coordinates. */
  @Hints({"SqlKind:ST_POINT3"})
  public static Geom ST_Point(BigDecimal x, BigDecimal y, BigDecimal z) {
    final Geometry g = new Point(x.doubleValue(), y.doubleValue(),
        z.doubleValue());
    return new Geometries.SimpleGeom(g);
  }

  // Geometry properties (2D and 3D) ==========================================

  /** Returns whether {@code geom} has at least one z-coordinate. */
  public static boolean ST_Is3D(Geom geom) {
    return geom.g().hasZ();
  }

  /** Returns the x-value of the first coordinate of {@code geom}. */
  public static @Nullable Double ST_X(Geom geom) {
    return geom.g() instanceof Point ? ((Point) geom.g()).getX() : null;
  }

  /** Returns the y-value of the first coordinate of {@code geom}. */
  public static @Nullable Double ST_Y(Geom geom) {
    return geom.g() instanceof Point ? ((Point) geom.g()).getY() : null;
  }

  /** Returns the z-value of the first coordinate of {@code geom}. */
  public static @Nullable Double ST_Z(Geom geom) {
    return geom.g().getDescription().hasZ() && geom.g() instanceof Point
        ? ((Point) geom.g()).getZ() : null;
  }

  /** Returns the boundary of {@code geom}. */
  public static Geom ST_Boundary(Geom geom) {
    OperatorBoundary op = OperatorBoundary.local();
    Geometry result = op.execute(geom.g(), null);
    return geom.wrap(result);
  }

  /** Returns the distance between {@code geom1} and {@code geom2}. */
  public static double ST_Distance(Geom geom1, Geom geom2) {
    return GeometryEngine.distance(geom1.g(), geom2.g(), geom1.sr());
  }

  /** Returns the type of {@code geom}. */
  public static String ST_GeometryType(Geom geom) {
    return Geometries.type(geom.g()).name();
  }

  /** Returns the OGC SFS type code of {@code geom}. */
  public static int ST_GeometryTypeCode(Geom geom) {
    return Geometries.type(geom.g()).code;
  }

  /** Returns the minimum bounding box of {@code geom} (which may be a
   *  GEOMETRYCOLLECTION). */
  public static Geom ST_Envelope(Geom geom) {
    final Envelope env = envelope(geom.g());
    return geom.wrap(env);
  }

  // Geometry predicates ======================================================

  /** Returns whether {@code geom1} contains {@code geom2}. */
  @Hints({"SqlKind:ST_CONTAINS"})
  public static boolean ST_Contains(Geom geom1, Geom geom2) {
    return GeometryEngine.contains(geom1.g(), geom2.g(), geom1.sr());
  }

  /** Returns whether {@code geom1} contains {@code geom2} but does not
   * intersect its boundary. */
  public static boolean ST_ContainsProperly(Geom geom1, Geom geom2) {
    return GeometryEngine.contains(geom1.g(), geom2.g(), geom1.sr())
        && !GeometryEngine.crosses(geom1.g(), geom2.g(), geom1.sr());
  }

  /** Returns whether no point in {@code geom2} is outside {@code geom1}. */
  private static boolean ST_Covers(Geom geom1, Geom geom2)  {
    throw todo();
  }

  /** Returns whether {@code geom1} crosses {@code geom2}. */
  public static boolean ST_Crosses(Geom geom1, Geom geom2)  {
    return GeometryEngine.crosses(geom1.g(), geom2.g(), geom1.sr());
  }

  /** Returns whether {@code geom1} and {@code geom2} are disjoint. */
  public static boolean ST_Disjoint(Geom geom1, Geom geom2)  {
    return GeometryEngine.disjoint(geom1.g(), geom2.g(), geom1.sr());
  }

  /** Returns whether the envelope of {@code geom1} intersects the envelope of
   *  {@code geom2}. */
  public static boolean ST_EnvelopesIntersect(Geom geom1, Geom geom2)  {
    final Geometry e1 = envelope(geom1.g());
    final Geometry e2 = envelope(geom2.g());
    return intersects(e1, e2, geom1.sr());
  }

  /** Returns whether {@code geom1} equals {@code geom2}. */
  public static boolean ST_Equals(Geom geom1, Geom geom2)  {
    return GeometryEngine.equals(geom1.g(), geom2.g(), geom1.sr());
  }

  /** Returns whether {@code geom1} intersects {@code geom2}. */
  public static boolean ST_Intersects(Geom geom1, Geom geom2)  {
    final Geometry g1 = geom1.g();
    final Geometry g2 = geom2.g();
    final SpatialReference sr = geom1.sr();
    return intersects(g1, g2, sr);
  }

  /** Returns whether {@code geom1} equals {@code geom2} and their coordinates
   * and component Geometries are listed in the same order. */
  public static boolean ST_OrderingEquals(Geom geom1, Geom geom2)  {
    return GeometryEngine.equals(geom1.g(), geom2.g(), geom1.sr());
  }

  /** Returns {@code geom1} overlaps {@code geom2}. */
  public static boolean ST_Overlaps(Geom geom1, Geom geom2)  {
    return GeometryEngine.overlaps(geom1.g(), geom2.g(), geom1.sr());
  }

  /** Returns whether {@code geom1} touches {@code geom2}. */
  public static boolean ST_Touches(Geom geom1, Geom geom2)  {
    return GeometryEngine.touches(geom1.g(), geom2.g(), geom1.sr());
  }

  /** Returns whether {@code geom1} is within {@code geom2}. */
  public static boolean ST_Within(Geom geom1, Geom geom2)  {
    return GeometryEngine.within(geom1.g(), geom2.g(), geom1.sr());
  }

  /** Returns whether {@code geom1} and {@code geom2} are within
   * {@code distance} of each other. */
  @Hints({"SqlKind:ST_DWITHIN"})
  public static boolean ST_DWithin(Geom geom1, Geom geom2, double distance) {
    final double distance1 =
        GeometryEngine.distance(geom1.g(), geom2.g(), geom1.sr());
    return distance1 <= distance;
  }

  // Geometry operators (2D and 3D) ===========================================

  /** Computes a buffer around {@code geom}. */
  public static Geom ST_Buffer(Geom geom, double distance) {
    final Polygon g = GeometryEngine.buffer(geom.g(), geom.sr(), distance);
    return geom.wrap(g);
  }

  /** Computes a buffer around {@code geom} with . */
  public static Geom ST_Buffer(Geom geom, double distance, int quadSegs) {
    throw todo();
  }

  /** Computes a buffer around {@code geom}. */
  public static Geom ST_Buffer(Geom geom, double bufferSize, String style) {
    int quadSegCount = 8;
    CapStyle endCapStyle = CapStyle.ROUND;
    JoinStyle joinStyle = JoinStyle.ROUND;
    float mitreLimit = 5f;
    int i = 0;
    parse:
    for (;;) {
      int equals = style.indexOf('=', i);
      if (equals < 0) {
        break;
      }
      int space = style.indexOf(' ', equals);
      if (space < 0) {
        space = style.length();
      }
      String name = style.substring(i, equals);
      String value = style.substring(equals + 1, space);
      switch (name) {
      case "quad_segs":
        quadSegCount = Integer.valueOf(value);
        break;
      case "endcap":
        endCapStyle = CapStyle.of(value);
        break;
      case "join":
        joinStyle = JoinStyle.of(value);
        break;
      case "mitre_limit":
      case "miter_limit":
        mitreLimit = Float.parseFloat(value);
        break;
      default:
        // ignore the value
      }
      i = space;
      for (;;) {
        if (i >= style.length()) {
          break parse;
        }
        if (style.charAt(i) != ' ') {
          break;
        }
        ++i;
      }
    }
    return buffer(geom, bufferSize, quadSegCount, endCapStyle, joinStyle,
        mitreLimit);
  }

  /** Computes the union of {@code geom1} and {@code geom2}. */
  public static Geom ST_Union(Geom geom1, Geom geom2) {
    SpatialReference sr = geom1.sr();
    final Geometry g =
        GeometryEngine.union(new Geometry[]{geom1.g(), geom2.g()}, sr);
    return bind(g, sr);
  }

  /** Computes the union of the geometries in {@code geomCollection}. */
  @SemiStrict public static Geom ST_Union(Geom geomCollection) {
    SpatialReference sr = geomCollection.sr();
    final Geometry g =
        GeometryEngine.union(new Geometry[] {geomCollection.g()}, sr);
    return bind(g, sr);
  }

  // Geometry projection functions ============================================

  /** Transforms {@code geom} from one coordinate reference
   * system (CRS) to the CRS specified by {@code srid}. */
  public static Geom ST_Transform(Geom geom, int srid) {
    return geom.transform(srid);
  }

  /** Returns a copy of {@code geom} with a new SRID. */
  public static Geom ST_SetSRID(Geom geom, int srid) {
    return geom.transform(srid);
  }

  // Space-filling curves

  /** Returns the position of a point on the Hilbert curve,
   * or null if it is not a 2-dimensional point. */
  @Hints({"SqlKind:HILBERT"})
  public static @Nullable Long hilbert(Geom geom) {
    final Geometry g = geom.g();
    if (g instanceof Point) {
      final double x = ((Point) g).getX();
      final double y = ((Point) g).getY();
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
      this.spanX = (int) Math.floor((envelope.getXMax() - envelope.getXMin())
          / this.deltaX) + 1;
      this.baseX = (int) Math.floor(envelope.getXMin() / this.deltaX);
      this.minX = this.deltaX * baseX;
      this.spanY = (int) Math.floor((envelope.getYMax() - envelope.getYMin())
          / this.deltaY) + 1;
      this.baseY = (int) Math.floor(envelope.getYMin() / this.deltaY);
      this.minY = this.deltaY * baseY;
      this.area = this.spanX * this.spanY;
    }

    @Override public Enumerator<Object[]> enumerator() {
      return new Enumerator<Object[]>() {
        int id = -1;

        @Override public Object[] current() {
          final Geom geom;
          final int x = id % spanX;
          final int y = id / spanX;
          if (point) {
            final double xCurrent = minX + (x + 0.5D) * deltaX;
            final double yCurrent = minY + (y + 0.5D) * deltaY;
            geom = ST_MakePoint(BigDecimal.valueOf(xCurrent),
                BigDecimal.valueOf(yCurrent));
          } else {
            final Polygon polygon = new Polygon();
            final double left = minX + x * deltaX;
            final double right = left + deltaX;
            final double bottom = minY + y * deltaY;
            final double top = bottom + deltaY;

            final Polyline polyline = new Polyline();
            polyline.addSegment(new Line(left, bottom, right, bottom), true);
            polyline.addSegment(new Line(right, bottom, right, top), false);
            polyline.addSegment(new Line(right, top, left, top), false);
            polyline.addSegment(new Line(left, top, left, bottom), false);
            polygon.add(polyline, false);
            geom = new Geometries.SimpleGeom(polygon);
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
