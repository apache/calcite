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
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.WKTWriter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the {@link PostgisGeometryDecoder} class. These tests are based
 * on the values retrieved from PostGIS using the JDBC driver for PostgreSQL.
 */
class PostgisGeometryDecoderTest {

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

  @Test void decodeNull() {
    Geometry geometry = PostgisGeometryDecoder.decode((String) null);
    assertEquals(null, geometry);
  }

  @Test void decodePoint() {
    Geometry geometry = PostgisGeometryDecoder.decode(POINT);
    assertTrue(geometry instanceof Point);
    assertEquals("POINT (1 2)", new WKTWriter().write(geometry));
  }

  @Test void decodeLineString() {
    Geometry geometry = PostgisGeometryDecoder.decode(LINESTRING);
    assertTrue(geometry instanceof org.locationtech.jts.geom.LineString);
    assertEquals("LINESTRING (0 0, 1 1, 2 2)", new WKTWriter().write(geometry));
  }

  @Test void decodePolygon() {
    Geometry geometry = PostgisGeometryDecoder.decode(POLYGON);
    assertTrue(geometry instanceof org.locationtech.jts.geom.Polygon);
    assertEquals(
        "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0), (0.5 0.5, 0.5 0.6, 0.6 0.6, 0.6 0.5, 0.5 0.5))",
        new WKTWriter().write(geometry));
  }

  @Test void decodeMultiPoint() {
    Geometry geometry = PostgisGeometryDecoder.decode(MULTIPOINT);
    assertTrue(geometry instanceof org.locationtech.jts.geom.MultiPoint);
    assertEquals(
        "MULTIPOINT ((0 0), (1 1))",
        new WKTWriter().write(geometry));
  }

  @Test void decodeMultiLineString() {
    Geometry geometry = PostgisGeometryDecoder.decode(MULTILINESTRING);
    assertTrue(geometry instanceof org.locationtech.jts.geom.MultiLineString);
    assertEquals(
        "MULTILINESTRING ((0 0, 1 1), (2 2, 3 3))",
        new WKTWriter().write(geometry));
  }

  @Test void decodeMultiPolygon() {
    Geometry geometry = PostgisGeometryDecoder.decode(MULTIPOLYGON);
    assertTrue(geometry instanceof org.locationtech.jts.geom.MultiPolygon);
    assertEquals(
        "MULTIPOLYGON (((0 0, 1 0, 1 1, 0 1, 0 0)), ((2 2, 3 2, 3 3, 2 3, 2 2)))",
        new WKTWriter().write(geometry));
  }

  @Test void decodeGeometryCollection() {
    Geometry geometry = PostgisGeometryDecoder.decode(GEOMETRYCOLLECTION);
    assertTrue(geometry instanceof org.locationtech.jts.geom.GeometryCollection);
    assertEquals(
        "GEOMETRYCOLLECTION (POINT (0 0), LINESTRING (1 1, 2 2))",
        new WKTWriter().write(geometry));
  }
}
