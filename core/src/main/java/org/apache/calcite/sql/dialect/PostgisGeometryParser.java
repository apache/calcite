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
package org.apache.calcite.sql.dialect;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * A parser for the PostGIS geometry format.
 */
public class PostgisGeometryParser {

  private static final GeometryFactory FACTORY = new GeometryFactory();

  private PostgisGeometryParser() {
  }

  public static byte[] hexStringToByteArray(String s) {
    int len = s.length();
    byte[] data = new byte[len / 2];
    for (int i = 0; i < len; i += 2) {
      data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
          + Character.digit(s.charAt(i + 1), 16));
    }
    return data;
  }

  public static Geometry parse(String string) {
    byte[] bytes = hexStringToByteArray(string);
    return parse(bytes);
  }

  public static Geometry parse(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    return parse(buffer);
  }

  public static Geometry parse(ByteBuffer buffer) {
    byte endianFlag = buffer.get();
    if (endianFlag == 0) {
      buffer.order(ByteOrder.BIG_ENDIAN);
    } else if (endianFlag == 1) {
      buffer.order(ByteOrder.LITTLE_ENDIAN);
    } else {
      throw new IllegalArgumentException("Unknown endian flag: " + endianFlag);
    }

    int typeInt = buffer.getInt();
    int geometryType = typeInt & 0x1FFFFFFF;

    boolean hasZ = (typeInt & 0x80000000) != 0;
    boolean hasM = (typeInt & 0x40000000) != 0;
    boolean hasSRID = (typeInt & 0x20000000) != 0;

    int srid = hasSRID ? Math.max(buffer.getInt(), 0) : 0;

    Geometry geometry = parseGeometry(buffer, geometryType, hasZ, hasM);
    if (hasSRID) {
      geometry.setSRID(srid);
    }

    return geometry;
  }

  private static Geometry parseGeometry(ByteBuffer buffer, int geometryType, boolean hasZ,
          boolean hasM) {
    switch (geometryType) {
    case 1:
      return parsePoint(buffer, hasZ, hasM);
    case 2:
      return parseLineString(buffer, hasZ, hasM);
    case 3:
      return parsePolygon(buffer, hasZ, hasM);
    case 4:
      return parseMultiPoint(buffer);
    case 5:
      return parseMultiLineString(buffer);
    case 6:
      return parseMultiPolygon(buffer);
    case 7:
      return parseGeometryCollection(buffer);
    default:
      throw new IllegalArgumentException("Unknown geometry type: " + geometryType);
    }
  }

  private static Point parsePoint(ByteBuffer buffer, boolean hasZ, boolean hasM) {
    double x = buffer.getDouble();
    double y = buffer.getDouble();
    double z = hasZ ? buffer.getDouble() : Double.NaN;
    double m = hasM ? buffer.getDouble() : Double.NaN;
    return FACTORY.createPoint(new Coordinate(x, y, z));
  }

  private static Coordinate[] parseCoordinates(ByteBuffer buffer, boolean hasZ, boolean hasM) {
    int numPoints = buffer.getInt();
    Coordinate[] coordinates = new Coordinate[numPoints];
    for (int i = 0; i < numPoints; i++) {
      double x = buffer.getDouble();
      double y = buffer.getDouble();
      double z = hasZ ? buffer.getDouble() : Double.NaN;
      double m = hasM ? buffer.getDouble() : Double.NaN;
      coordinates[i] = new Coordinate(x, y, z);

    }
    return coordinates;
  }

  private static LineString parseLineString(ByteBuffer buffer, boolean hasZ, boolean hasM) {
    return FACTORY.createLineString(parseCoordinates(buffer, hasZ, hasM));
  }

  private static LinearRing parseLinearRing(ByteBuffer buffer, boolean hasZ, boolean hasM) {
    return FACTORY.createLinearRing(parseCoordinates(buffer, hasZ, hasM));
  }

  private static Polygon parsePolygon(ByteBuffer buffer, boolean hasZ, boolean hasM) {
    int numRings = buffer.getInt();
    LinearRing[] rings = new LinearRing[numRings];
    for (int i = 0; i < numRings; i++) {
      rings[i] = parseLinearRing(buffer, hasZ, hasM);
    }
    return FACTORY.createPolygon(rings[0], Arrays.copyOfRange(rings, 1, rings.length));
  }

  private static <T extends Geometry> T[] parseGeometries(ByteBuffer buffer, Class<T> type) {
    int size = buffer.getInt();
    @SuppressWarnings("unchecked")
    T[] geometries = (T[]) Array.newInstance(type, size);
    for (int i = 0; i < size; i++) {
      geometries[i] = (T) parse(buffer);
    }
    return geometries;
  }

  private static MultiPoint parseMultiPoint(ByteBuffer buffer) {
    return FACTORY.createMultiPoint(parseGeometries(buffer, Point.class));
  }

  private static MultiLineString parseMultiLineString(ByteBuffer buffer) {
    return FACTORY.createMultiLineString(parseGeometries(buffer, LineString.class));
  }

  private static MultiPolygon parseMultiPolygon(ByteBuffer buffer) {
    return FACTORY.createMultiPolygon(parseGeometries(buffer, Polygon.class));
  }

  private static GeometryCollection parseGeometryCollection(ByteBuffer buffer) {
    return FACTORY.createGeometryCollection(parseGeometries(buffer, Geometry.class));
  }
}
