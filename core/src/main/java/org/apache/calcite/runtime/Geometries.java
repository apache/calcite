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
import org.apache.calcite.linq4j.function.Strict;
import org.apache.calcite.util.Util;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.Line;
import com.esri.core.geometry.MapGeometry;
import com.esri.core.geometry.Operator;
import com.esri.core.geometry.OperatorFactoryLocal;
import com.esri.core.geometry.OperatorIntersects;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polyline;
import com.esri.core.geometry.SpatialReference;
import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.PolyNull;

import java.util.Objects;

/**
 * Utilities for geometry.
 */
@SuppressWarnings({"UnnecessaryUnboxing", "WeakerAccess", "unused"})
@Deterministic
@Strict
@Experimental
public class Geometries {
  static final int NO_SRID = 0;
  private static final SpatialReference SPATIAL_REFERENCE =
      SpatialReference.create(4326);

  private Geometries() {}

  static UnsupportedOperationException todo() {
    return new UnsupportedOperationException();
  }

  protected static @PolyNull Geom bind(@PolyNull Geometry geometry, int srid) {
    if (geometry == null) {
      return null;
    }
    if (srid == NO_SRID) {
      return new SimpleGeom(geometry);
    }
    return bind(geometry, SpatialReference.create(srid));
  }

  static MapGeom bind(Geometry geometry, SpatialReference sr) {
    return new MapGeom(new MapGeometry(geometry, sr));
  }

  static Geom makeLine(Geom... geoms) {
    return makeLine(ImmutableList.copyOf(geoms));
  }

  public static Geom makeLine(Iterable<? extends Geom> geoms) {
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

  static Geom point(double x, double y) {
    final Geometry g = new Point(x, y);
    return new SimpleGeom(g);
  }

  /** Returns the OGC type of a geometry. */
  public static Type type(Geometry g) {
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

  static Envelope envelope(Geometry g) {
    final Envelope env = new Envelope();
    g.queryEnvelope(env);
    return env;
  }

  static boolean intersects(Geometry g1, Geometry g2,
      SpatialReference sr) {
    final OperatorIntersects op = (OperatorIntersects) OperatorFactoryLocal
        .getInstance().getOperator(Operator.Type.Intersects);
    return op.execute(g1, g2, sr, null);
  }

  static Geom buffer(Geom geom, double bufferSize,
      int quadSegCount, CapStyle endCapStyle, JoinStyle joinStyle,
      float mitreLimit) {
    Util.discard(endCapStyle + ":" + joinStyle + ":" + mitreLimit
        + ":" + quadSegCount);
    throw todo();
  }

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

  /** Geometry types, with the names and codes assigned by OGC. */
  public enum Type {
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

  /** Geometry. It may or may not have a spatial reference
   * associated with it. */
  public interface Geom extends Comparable<Geom> {
    Geometry g();

    Type type();

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

    @Override public int compareTo(Geom o) {
      return toString().compareTo(o.toString());
    }

    @Override public Geometry g() {
      return g;
    }

    @Override public Type type() {
      return Geometries.type(g);
    }

    @Override public SpatialReference sr() {
      return SPATIAL_REFERENCE;
    }

    @Override public Geom transform(int srid) {
      if (srid == SPATIAL_REFERENCE.getID()) {
        return this;
      }
      return bind(g, srid);
    }

    @Override public Geom wrap(Geometry g) {
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

    @Override public int compareTo(Geom o) {
      return toString().compareTo(o.toString());
    }

    @Override public Geometry g() {
      return mg.getGeometry();
    }

    @Override public Type type() {
      return Geometries.type(mg.getGeometry());
    }

    @Override public SpatialReference sr() {
      return mg.getSpatialReference();
    }

    @Override public Geom transform(int srid) {
      if (srid == NO_SRID) {
        return new SimpleGeom(mg.getGeometry());
      }
      if (srid == mg.getSpatialReference().getID()) {
        return this;
      }
      return bind(mg.getGeometry(), srid);
    }

    @Override public Geom wrap(Geometry g) {
      return bind(g, this.mg.getSpatialReference());
    }
  }
}
