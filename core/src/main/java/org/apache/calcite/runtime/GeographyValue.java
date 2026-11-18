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

import org.apache.calcite.linq4j.function.Strict;

import org.locationtech.jts.geom.Geometry;

import static java.util.Objects.requireNonNull;

/**
 * Java value supplied by adapters for the structural GEOGRAPHY accessors.
 *
 * <p>The adapter supplies a valid geography value. This wrapper and its
 * accessors do not validate CRS, transform coordinates, or perform geodetic
 * measurements.
 *
 * <p>The geometry is retained by reference. The adapter owns it and must not
 * mutate it after publishing this value to a query, including through aliases
 * or coordinate sequences.
 */
@Strict
public final class GeographyValue {
  private final Geometry geometry;

  /** Wraps an adapter-supplied geometry by reference, without copying or validation. */
  public GeographyValue(Geometry geometry) {
    this.geometry = requireNonNull(geometry, "geometry");
  }

  /** Returns whether the geography has no points. */
  public static boolean isEmpty(GeographyValue value) {
    return SpatialTypeFunctions.ST_IsEmpty(value.geometry);
  }

  /** Returns the concrete spatial type name. */
  public static String geometryType(GeographyValue value) {
    return SpatialTypeFunctions.ST_GeometryType(value.geometry);
  }
}
