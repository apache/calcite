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

import org.apache.calcite.linq4j.function.Deterministic;
import org.apache.calcite.linq4j.function.Experimental;
import org.apache.calcite.linq4j.function.SemiStrict;
import org.apache.calcite.linq4j.function.Strict;
import org.apache.calcite.util.Util;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.Line;
import com.esri.core.geometry.MapGeometry;
import com.esri.core.geometry.Operator;
import com.esri.core.geometry.OperatorBoundary;
import com.esri.core.geometry.OperatorFactoryLocal;
import com.esri.core.geometry.OperatorIntersects;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.Polyline;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.WktExportFlags;
import com.esri.core.geometry.WktImportFlags;

import java.math.BigDecimal;
import java.util.Objects;

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
  private static final int NO_SRID = 0;
  private static final SpatialReference SPATIAL_REFERENCE =
      SpatialReference.create(4326);

  private GeoFunctions() {}

  private static UnsupportedOperationException todo() {
    return new UnsupportedOperationException();
  }

  protected static Geom bind(Geometry geometry, int srid) {
    if (geometry == null) {
      return null;
    }
    if (srid == NO_SRID) {
      return new SimpleGeom(geometry);
    }
    return bind(geometry, SpatialReference.create(srid));
  }

  private static MapGeom bind(Geometry geometry, SpatialReference sr) {
    return new MapGeom(new MapGeometry(geometry, sr));
  }

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

  /**  Creates a line-string from the given POINTs (or MULTIPOINTs). */
  public static Geom ST_MakeLine(Geom geom1, Geom geom2) {
    return makeLine(geom1, geom2);
  }

  public static Geom ST_MakeLine(Geom geom1, Geom geom2, Geom geom3) {
    return makeLine(geom1, geom2, geom3);
  }

  public static Geom ST_MakeLine(Geom geom1, Geom geom2, Geom geom3,
      Geom geom4) {
    return makeLine(geom1, geom2, geom3, geom4);
  }

  public static Geom ST_MakeLine(Geom geom1, Geom geom2, Geom geom3,
      Geom geom4, Geom geom5) {
    return makeLine(geom1, geom2, geom3, geom4, geom5);
  }

  public static Geom ST_MakeLine(Geom geom1, Geom geom2, Geom geom3,
      Geom geom4, Geom geom5, Geom geom6) {
    return makeLine(geom1, geom2, geom3, geom4, geom5, geom6);
  }

  private static Geom makeLine(Geom... geoms) {
    final Polyline g = new Polyline();
    Point p = null;
    for (Geom geom : geoms) {
      if (geom.g() instanceof Point) {
        final Point prev = p;
        p = (Point) geom.g();
        if (prev != null) {
          final Line line = new Line();
          line.setStart(prev);
          line.setEnd(p);
          g.addSegment(line, false);
        }
      }
    }
    return new SimpleGeom(g);
  }

  /**  Alias for {@link #ST_Point(BigDecimal, BigDecimal)}. */
  public static Geom ST_MakePoint(BigDecimal x, BigDecimal y) {
    return ST_Point(x, y);
  }

  /**  Alias for {@link #ST_Point(BigDecimal, BigDecimal, BigDecimal)}. */
  public static Geom ST_MakePoint(BigDecimal x, BigDecimal y, BigDecimal z) {
    return ST_Point(x, y, z);
  }

  /**  Constructs a 2D point from coordinates. */
  public static Geom ST_Point(BigDecimal x, BigDecimal y) {
    // NOTE: Combine the double and BigDecimal variants of this function
    return point(x.doubleValue(), y.doubleValue());
  }

  /**  Constructs a 3D point from coordinates. */
  public static Geom ST_Point(BigDecimal x, BigDecimal y, BigDecimal z) {
    final Geometry g = new Point(x.doubleValue(), y.doubleValue(),
        z.doubleValue());
    return new SimpleGeom(g);
  }

  private static Geom point(double x, double y) {
    final Geometry g = new Point(x, y);
    return new SimpleGeom(g);
  }

  // Geometry properties (2D and 3D) ==========================================

  /** Returns whether {@code geom} has at least one z-coordinate. */
  public static boolean ST_Is3D(Geom geom) {
    return geom.g().hasZ();
  }

  /** Returns the x-value of the first coordinate of {@code geom}. */
  public static Double ST_X(Geom geom) {
    return geom.g() instanceof Point ? ((Point) geom.g()).getX() : null;
  }

  /** Returns the y-value of the first coordinate of {@code geom}. */
  public static Double ST_Y(Geom geom) {
    return geom.g() instanceof Point ? ((Point) geom.g()).getY() : null;
  }

  /** Returns the z-value of the first coordinate of {@code geom}. */
  public static Double ST_Z(Geom geom) {
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
    return type(geom.g()).name();
  }

  /** Returns the OGC SFS type code of {@code geom}. */
  public static int ST_GeometryTypeCode(Geom geom) {
    return type(geom.g()).code;
  }

  /** Returns the OGC type of a geometry. */
  private static Type type(Geometry g) {
    switch (g.getType()) {
    case Point:
      return Type.POINT;
    case Polyline:
      return Type.LINESTRING;
    case Polygon:
      return Type.POLYGON;
    case MultiPoint:
      return Type.MULTIPOINT;
    case Envelope:
      return Type.POLYGON;
    case Line:
      return Type.LINESTRING;
    case Unknown:
      return Type.Geometry;
    default:
      throw new AssertionError(g);
    }
  }

  /** Returns the minimum bounding box of {@code geom} (which may be a
   *  GEOMETRYCOLLECTION). */
  public static Geom ST_Envelope(Geom geom) {
    final Envelope env = envelope(geom.g());
    return geom.wrap(env);
  }

  private static Envelope envelope(Geometry g) {
    final Envelope env = new Envelope();
    g.queryEnvelope(env);
    return env;
  }

  // Geometry predicates ======================================================

  /** Returns whether {@code geom1} contains {@code geom2}. */
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

  private static boolean intersects(Geometry g1, Geometry g2,
      SpatialReference sr) {
    final OperatorIntersects op = (OperatorIntersects) OperatorFactoryLocal
        .getInstance().getOperator(Operator.Type.Intersects);
    return op.execute(g1, g2, sr, null);
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

  private static Geom buffer(Geom geom, double bufferSize,
      int quadSegCount, CapStyle endCapStyle, JoinStyle joinStyle,
      float mitreLimit) {
    Util.discard(endCapStyle + ":" + joinStyle + ":" + mitreLimit
        + ":" + quadSegCount);
    throw todo();
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

  // Inner classes ============================================================

  /** How the "buffer" command terminates the end of a line. */
  enum CapStyle {
    ROUND, FLAT, SQUARE;

    static CapStyle of(String value) {
      switch (value) {
      case "round":
        return ROUND;
      case "flat":
      case "butt":
        return FLAT;
      case "square":
        return SQUARE;
      default:
        throw new IllegalArgumentException("unknown endcap value: " + value);
      }
    }
  }

  /** How the "buffer" command decorates junctions between line segments. */
  enum JoinStyle {
    ROUND, MITRE, BEVEL;

    static JoinStyle of(String value) {
      switch (value) {
      case "round":
        return ROUND;
      case "mitre":
      case "miter":
        return MITRE;
      case "bevel":
        return BEVEL;
      default:
        throw new IllegalArgumentException("unknown join value: " + value);
      }
    }
  }

  /** Geometry. It may or may not have a spatial reference
   * associated with it. */
  public interface Geom {
    Geometry g();

    SpatialReference sr();

    Geom transform(int srid);

    Geom wrap(Geometry g);
  }

  /** Sub-class of geometry that has no spatial reference. */
  static class SimpleGeom implements Geom {
    final Geometry g;

    SimpleGeom(Geometry g) {
      this.g = Objects.requireNonNull(g);
    }

    @Override public String toString() {
      return g.toString();
    }

    public Geometry g() {
      return g;
    }

    public SpatialReference sr() {
      return SPATIAL_REFERENCE;
    }

    public Geom transform(int srid) {
      if (srid == SPATIAL_REFERENCE.getID()) {
        return this;
      }
      return bind(g, srid);
    }

    public Geom wrap(Geometry g) {
      return new SimpleGeom(g);
    }
  }

  /** Sub-class of geometry that has a spatial reference. */
  static class MapGeom implements Geom {
    final MapGeometry mg;

    MapGeom(MapGeometry mg) {
      this.mg = Objects.requireNonNull(mg);
    }

    @Override public String toString() {
      return mg.toString();
    }

    public Geometry g() {
      return mg.getGeometry();
    }

    public SpatialReference sr() {
      return mg.getSpatialReference();
    }

    public Geom transform(int srid) {
      if (srid == NO_SRID) {
        return new SimpleGeom(mg.getGeometry());
      }
      if (srid == mg.getSpatialReference().getID()) {
        return this;
      }
      return bind(mg.getGeometry(), srid);
    }

    public Geom wrap(Geometry g) {
      return bind(g, this.mg.getSpatialReference());
    }
  }

  /** Geometry types, with the names and codes assigned by OGC. */
  enum Type {
    Geometry(0),
    POINT(1),
    LINESTRING(2),
    POLYGON(3),
    MULTIPOINT(4),
    MULTILINESTRING(5),
    MULTIPOLYGON(6),
    GEOMCOLLECTION(7),
    CURVE(13),
    SURFACE(14),
    POLYHEDRALSURFACE(15);

    final int code;

    Type(int code) {
      this.code = code;
    }
  }
}

// End GeoFunctions.java
