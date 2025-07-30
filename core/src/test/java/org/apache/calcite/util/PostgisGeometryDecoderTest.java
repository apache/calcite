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

import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the {@link PostgisGeometryDecoder} class. These tests are based
 * on the values retrieved from PostGIS using the JDBC driver for PostgreSQL.
 */
class PostgisGeometryDecoderTest {

  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

  private static final String POINT = "0101000020E6100000000000000000F03F000000"
      + "0000000040";

  private static final String LINESTRING = "0102000020E610000003000000000000000"
      + "00000000000000000000000000000000000F03F000000000000F03F000000000000004"
      + "00000000000000040";

  private static final String POLYGON = "0103000020E610000002000000050000000000"
      + "0000000000000000000000000000000000000000F03F00000000000000000000000000"
      + "00F03F000000000000F03F0000000000000000000000000000F03F0000000000000000"
      + "000000000000000005000000000000000000E03F000000000000E03F000000000000E0"
      + "3F333333333333E33F333333333333E33F333333333333E33F333333333333E33F0000"
      + "00000000E03F000000000000E03F000000000000E03F";

  private static final String MULTIPOINT = "0104000020E610000002000000010100000"
      + "0000000000000000000000000000000000101000000000000000000F03F00000000000"
      + "0F03F";

  private static final String MULTILINESTRING = "0105000020E6100000020000000102"
      + "0000000200000000000000000000000000000000000000000000000000F03F00000000"
      + "0000F03F01020000000200000000000000000000400000000000000040000000000000"
      + "08400000000000000840";

  private static final String MULTIPOLYGON = "0106000020E6100000020000000103000"
      + "000010000000500000000000000000000000000000000000000000000000000F03F000"
      + "0000000000000000000000000F03F000000000000F03F0000000000000000000000000"
      + "000F03F000000000000000000000000000000000103000000010000000500000000000"
      + "0000000004000000000000000400000000000000840000000000000004000000000000"
      + "0084000000000000008400000000000000040000000000000084000000000000000400"
      + "000000000000040";

  private static final String GEOMETRYCOLLECTION = "0107000020E6100000020000000"
      + "1010000000000000000000000000000000000000001020000000200000000000000000"
      + "0F03F000000000000F03F00000000000000400000000000000040";

  @Test
  void decodeNull() {
    Geometry geometry = PostgisGeometryDecoder.decode((String) null);
    assertNull(geometry);
  }

  @Test
  void decodePoint() {
    Geometry geometry = PostgisGeometryDecoder.decode(POINT);
    Geometry expected = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 2));
    assertTrue(expected.equalsExact(geometry));
  }

  @Test
  void decodeLineString() {
    Geometry geometry = PostgisGeometryDecoder.decode(LINESTRING);
    Geometry expected = GEOMETRY_FACTORY.createLineString(new Coordinate[]{
        new Coordinate(0, 0),
        new Coordinate(1, 1),
        new Coordinate(2, 2)
    });
    assertTrue(expected.equalsExact(geometry));
  }

  @Test
  void decodePolygon() {
    Geometry geometry = PostgisGeometryDecoder.decode(POLYGON);
    org.locationtech.jts.geom.LinearRing shell = GEOMETRY_FACTORY.createLinearRing(new Coordinate[]{
        new Coordinate(0, 0),
        new Coordinate(1, 0),
        new Coordinate(1, 1),
        new Coordinate(0, 1),
        new Coordinate(0, 0)
    });
    org.locationtech.jts.geom.LinearRing hole = GEOMETRY_FACTORY.createLinearRing(new Coordinate[]{
        new Coordinate(0.5, 0.5),
        new Coordinate(0.5, 0.6),
        new Coordinate(0.6, 0.6),
        new Coordinate(0.6, 0.5),
        new Coordinate(0.5, 0.5)
    });
    org.locationtech.jts.geom.Polygon expected = GEOMETRY_FACTORY.createPolygon(shell, new org.locationtech.jts.geom.LinearRing[]{hole});
    assertTrue(expected.equalsExact(geometry));
  }

  @Test
  void decodeMultiPoint() {
    Geometry geometry = PostgisGeometryDecoder.decode(MULTIPOINT);
    Point[] points = new Point[]{
        GEOMETRY_FACTORY.createPoint(new Coordinate(0, 0)),
        GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1))
    };
    Geometry expected = GEOMETRY_FACTORY.createMultiPoint(points);
    assertTrue(expected.equalsExact(geometry));
  }

  @Test
  void decodeMultiLineString() {
    Geometry geometry = PostgisGeometryDecoder.decode(MULTILINESTRING);
    Geometry expected = GEOMETRY_FACTORY.createMultiLineString(new org.locationtech.jts.geom.LineString[]{
        GEOMETRY_FACTORY.createLineString(new Coordinate[]{
            new Coordinate(0, 0),
            new Coordinate(1, 1)
        }),
        GEOMETRY_FACTORY.createLineString(new Coordinate[]{
            new Coordinate(2, 2),
            new Coordinate(3, 3)
        })
    });
    assertTrue(expected.equalsExact(geometry));
  }

  @Test
  void decodeMultiPolygon() {
    Geometry geometry = PostgisGeometryDecoder.decode(MULTIPOLYGON);
    org.locationtech.jts.geom.Polygon[] polygons = new org.locationtech.jts.geom.Polygon[]{
        GEOMETRY_FACTORY.createPolygon(GEOMETRY_FACTORY.createLinearRing(new Coordinate[]{
            new Coordinate(0, 0),
            new Coordinate(1, 0),
            new Coordinate(1, 1),
            new Coordinate(0, 1),
            new Coordinate(0, 0)
        }), null),
        GEOMETRY_FACTORY.createPolygon(GEOMETRY_FACTORY.createLinearRing(new Coordinate[]{
            new Coordinate(2, 2),
            new Coordinate(3, 2),
            new Coordinate(3, 3),
            new Coordinate(2, 3),
            new Coordinate(2, 2)
        }), null)
    };
    Geometry expected = GEOMETRY_FACTORY.createMultiPolygon(polygons);
    assertTrue(expected.equalsExact(geometry));
  }

  @Test
  void decodeGeometryCollection() {
    Geometry geometry = PostgisGeometryDecoder.decode(GEOMETRYCOLLECTION);
    Geometry[] geometries = new Geometry[]{
        GEOMETRY_FACTORY.createPoint(new Coordinate(0, 0)),
        GEOMETRY_FACTORY.createLineString(new Coordinate[]{
            new Coordinate(1, 1),
            new Coordinate(2, 2)
        })
    };
    Geometry expected = GEOMETRY_FACTORY.createGeometryCollection(geometries);
    assertTrue(expected.equalsExact(geometry));
  }

  @Test
  void decodeInvalidHex() {
    String invalidHex = "XYZ";
    assertThrows(IllegalArgumentException.class, () -> PostgisGeometryDecoder.decode(invalidHex));
  }

  @Test
  void decodeUnknownEndianFlag() {
    byte[] bytes = new byte[]{(byte) 2, 0, 0, 0, 0};
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    assertThrows(IllegalArgumentException.class, () -> PostgisGeometryDecoder.decode(buffer));
  }

  @Test
  void decodeUnknownGeometryType() {
    ByteBuffer buffer = ByteBuffer.allocate(9);
    buffer.put((byte) 1); // little endian
    buffer.putInt(999); // unknown geometry type
    buffer.putInt(0); // dummy SRID
    buffer.rewind();
    assertThrows(IllegalArgumentException.class, () -> PostgisGeometryDecoder.decode(buffer));
  }
}
