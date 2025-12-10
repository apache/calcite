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
package org.apache.calcite.util;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import org.checkerframework.checker.nullness.qual.Nullable;
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
 * A decoder for the PostGIS geometry format.
 */
public class PostgisGeometryDecoder {

  private static final GeometryFactory FACTORY = new GeometryFactory();

  private PostgisGeometryDecoder() {
  }

  public static @Nullable Geometry decode(@Nullable String string) {
    if (string == null) {
      return null;
    }
    try {
      byte[] bytes = Hex.decodeHex(string);
      return decode(bytes);
    } catch (DecoderException e) {
      throw new IllegalArgumentException("Invalid HEX string: " + string, e);
    }
  }

  public static @Nullable Geometry decode(byte[] bytes) {
    if (bytes == null) {
      return null;
    }
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    return decode(buffer);
  }

  public static @Nullable Geometry decode(@Nullable ByteBuffer buffer) {
    if (buffer == null) {
      return null;
    }

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

    Geometry geometry = decodeGeometry(buffer, geometryType, hasZ, hasM);
    if (hasSRID) {
      geometry.setSRID(srid);
    }

    return geometry;
  }

  private static Geometry decodeGeometry(ByteBuffer buffer, int geometryType, boolean hasZ,
      boolean hasM) {
    switch (geometryType) {
    case 1:
      return decodePoint(buffer, hasZ, hasM);
    case 2:
      return decodeLineString(buffer, hasZ, hasM);
    case 3:
      return decodePolygon(buffer, hasZ, hasM);
    case 4:
      return decodeMultiPoint(buffer);
    case 5:
      return decodeMultiLineString(buffer);
    case 6:
      return decodeMultiPolygon(buffer);
    case 7:
      return decodeGeometryCollection(buffer);
    default:
      throw new IllegalArgumentException("Unknown geometry type: " + geometryType);
    }
  }

  private static Point decodePoint(ByteBuffer buffer, boolean hasZ, boolean hasM) {
    double x = buffer.getDouble();
    double y = buffer.getDouble();
    Coordinate coordinate = new Coordinate(x, y);
    if (hasZ) {
      double z = buffer.getDouble();
      coordinate.setZ(z);
    }
    if (hasM) {
      double m = buffer.getDouble();
      coordinate.setM(m);
    }
    return FACTORY.createPoint(coordinate);
  }

  private static Coordinate[] decodeCoordinates(ByteBuffer buffer, boolean hasZ, boolean hasM) {
    int numPoints = buffer.getInt();
    Coordinate[] coordinates = new Coordinate[numPoints];
    for (int i = 0; i < numPoints; i++) {
      double x = buffer.getDouble();
      double y = buffer.getDouble();
      Coordinate coordinate = new Coordinate(x, y);
      if (hasZ) {
        double z = buffer.getDouble();
        coordinate.setZ(z);
      }
      if (hasM) {
        double m = buffer.getDouble();
        coordinate.setM(m);
      }
      coordinates[i] = coordinate;
    }
    return coordinates;
  }

  private static LineString decodeLineString(ByteBuffer buffer, boolean hasZ, boolean hasM) {
    return FACTORY.createLineString(decodeCoordinates(buffer, hasZ, hasM));
  }

  private static LinearRing decodeLinearRing(ByteBuffer buffer, boolean hasZ, boolean hasM) {
    return FACTORY.createLinearRing(decodeCoordinates(buffer, hasZ, hasM));
  }

  private static Polygon decodePolygon(ByteBuffer buffer, boolean hasZ, boolean hasM) {
    int numRings = buffer.getInt();
    LinearRing[] rings = new LinearRing[numRings];
    for (int i = 0; i < numRings; i++) {
      rings[i] = decodeLinearRing(buffer, hasZ, hasM);
    }
    LinearRing shell = rings[0];
    LinearRing[] holes = (LinearRing[]) Arrays.copyOfRange(rings, 1, rings.length);
    return FACTORY.createPolygon(shell, holes);
  }

  private static <T extends Geometry> T[] decodeGeometries(ByteBuffer buffer, Class<T> type) {
    int size = buffer.getInt();
    T[] geometries = (T[]) Array.newInstance(type, size);
    for (int i = 0; i < size; i++) {
      Object decoded = decode(buffer);
      if (type.isInstance(decoded)) {
        geometries[i] = type.cast(decoded);
      }
    }
    return geometries;
  }

  private static MultiPoint decodeMultiPoint(ByteBuffer buffer) {
    return FACTORY.createMultiPoint(decodeGeometries(buffer, Point.class));
  }

  private static MultiLineString decodeMultiLineString(ByteBuffer buffer) {
    return FACTORY.createMultiLineString(decodeGeometries(buffer, LineString.class));
  }

  private static MultiPolygon decodeMultiPolygon(ByteBuffer buffer) {
    return FACTORY.createMultiPolygon(decodeGeometries(buffer, Polygon.class));
  }

  private static GeometryCollection decodeGeometryCollection(ByteBuffer buffer) {
    return FACTORY.createGeometryCollection(decodeGeometries(buffer, Geometry.class));
  }
}
