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
import org.apache.calcite.linq4j.function.Deterministic;
import org.apache.calcite.linq4j.function.Experimental;
import org.apache.calcite.linq4j.function.Strict;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;
import org.locationtech.jts.io.geojson.GeoJsonReader;
import org.locationtech.jts.io.geojson.GeoJsonWriter;
import org.locationtech.jts.io.gml2.GMLReader;
import org.locationtech.jts.io.gml2.GMLWriter;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.util.Locale;
import java.util.regex.Pattern;
import javax.xml.parsers.ParserConfigurationException;

/**
 * Utilities for spatial types.
 */
@Deterministic
@Strict
@Experimental
public class SpatialTypeUtils {
  static final int NO_SRID = 0;

  public static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

  private SpatialTypeUtils() {}

  /** Geometry types, with the names and codes assigned by OGC. */
  public enum SpatialType {
    GEOMETRY(0),
    POINT(1),
    LINESTRING(2),
    POLYGON(3),
    MULTIPOINT(4),
    MULTILINESTRING(5),
    MULTIPOLYGON(6),
    GEOMETRYCOLLECTION(7);

    private final int code;

    SpatialType(int code) {
      this.code = code;
    }

    public int code() {
      return code;
    }

    /** Returns the OGC type of a geometry. */
    public static SpatialType fromGeometry(Geometry g) {
      switch (g.getGeometryType()) {
      case "Geometry":
        return SpatialType.GEOMETRY;
      case "Point":
        return SpatialType.POINT;
      case "LineString":
        return SpatialType.LINESTRING;
      case "Polygon":
        return SpatialType.POLYGON;
      case "MultiPoint":
        return SpatialType.MULTIPOINT;
      case "MultiLineString":
        return SpatialType.MULTILINESTRING;
      case "MultiPolygon":
        return SpatialType.MULTIPOLYGON;
      case "GeometryCollection":
        return SpatialType.GEOMETRYCOLLECTION;
      default:
        throw new AssertionError(g);
      }
    }
  }

  private static int dimension(Geometry geometry) {
    int dimension = 3;
    for (Coordinate coordinate : geometry.getCoordinates()) {
      if (Double.isNaN(coordinate.getZ())) {
        dimension = 2;
        break;
      }
    }
    return dimension;
  }

  /**
   * Constructs a geometry from a GeoJson representation.
   *
   * @param geoJson a GeoJson
   * @return a geometry
   */
  public static Geometry fromGeoJson(String geoJson) {
    try {
      GeoJsonReader reader = new GeoJsonReader();
      return reader.read(geoJson);
    } catch (ParseException e) {
      throw new RuntimeException("Unable to parse GeoJSON");
    }
  }

  /**
   * Constructs a geometry from a GML representation.
   *
   * @param gml a GML
   * @return a geometry
   */
  public static Geometry fromGml(String gml) {
    try {
      GMLReader reader = new GMLReader();
      return reader.read(gml, GEOMETRY_FACTORY);
    } catch (SAXException | IOException | ParserConfigurationException e) {
      throw new RuntimeException("Unable to parse GML");
    }
  }

  /**
   * Constructs a geometry from a Well-Known binary (WKB) representation.
   *
   * @param wkb a WKB
   * @return a geometry
   */
  public static Geometry fromWkb(ByteString wkb) {
    try {
      WKBReader reader = new WKBReader();
      return reader.read(wkb.getBytes());
    } catch (ParseException e) {
      throw new RuntimeException("Unable to parse WKB");
    }
  }

  /**
   * Constructs a geometry from an Extended Well-Known text (EWKT) representation.
   * EWKT representations are prefixed with the SRID.
   *
   * @param ewkt an EWKT
   * @return a geometry
   */
  public static Geometry fromEwkt(String ewkt) {
    Pattern pattern = Pattern.compile("^(?:srid:(\\d*);)?(.*)$");
    java.util.regex.Matcher matcher = pattern.matcher(ewkt);
    if (!matcher.matches()) {
      throw new RuntimeException("Unable to parse EWKT");
    }

    String wkt = matcher.group(2);
    if (wkt == null) {
      throw new RuntimeException("Unable to parse EWKT");
    }

    Geometry geometry = fromWkt(wkt);
    String srid = matcher.group(1);
    if (srid != null) {
      geometry.setSRID(Integer.parseInt(srid));
    }

    return geometry;
  }

  /**
   * Constructs a geometry from a Well-Known text (WKT) representation.
   *
   * @param wkt a WKT
   * @return a geometry
   */
  public static Geometry fromWkt(String wkt) {
    try {
      WKTReader reader = new WKTReader();
      return reader.read(wkt);
    } catch (ParseException e) {
      throw new RuntimeException("Unable to parse WKT");
    }
  }

  /**
   * Returns the GeoJson representation of the geometry.
   *
   * @param geometry a geometry
   * @return a GeoJson
   */
  public static String asGeoJson(Geometry geometry) {
    GeoJsonWriter geoJsonWriter = new GeoJsonWriter();
    return geoJsonWriter.write(geometry);
  }

  /**
   * Returns the GML representation of the geometry.
   *
   * @param geometry a geometry
   * @return a GML
   */
  public static String asGml(Geometry geometry) {
    GMLWriter gmlWriter = new GMLWriter();
    // remove line breaks and indentation
    String minified = gmlWriter.write(geometry)
        .replace("\n", "")
        .replace("  ", "");
    return minified;
  }

  /**
   * Returns the Extended Well-Known binary (WKB) representation of the geometry.
   *
   * @param geometry a geometry
   * @return an WKB
   */
  public static ByteString asWkb(Geometry geometry) {
    int outputDimension = dimension(geometry);
    WKBWriter wkbWriter = new WKBWriter(outputDimension);
    return new ByteString(wkbWriter.write(geometry));
  }

  /**
   * Returns the Extended Well-Known text (EWKT) representation of the geometry.
   * EWKT representations are prefixed with the SRID.
   *
   * @param geometry a geometry
   * @return an EWKT
   */
  public static String asEwkt(Geometry geometry) {
    return String.format(Locale.ROOT, "srid:%s;%s", geometry.getSRID(), asWkt(geometry));
  }

  /**
   * Returns the Well-Known text (WKT) representation of the geometry.
   *
   * @param geometry a geometry
   * @return a WKT
   */
  public static String asWkt(Geometry geometry) {
    int outputDimension = dimension(geometry);
    WKTWriter wktWriter = new WKTWriter(outputDimension);
    return wktWriter.write(geometry);
  }
}
