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
 * JTS-backed representation of a SQL {@code GEOGRAPHY} value for local structural accessors.
 *
 * @see <a href="https://calcite.apache.org/docs/adapter.html#geography-values">
 *     Adapter ownership and validity requirements</a>
 */
@Strict
public final class GeographyValue {
  private final Geometry geometry;

  /**
   * Retains the supplied JTS geometry by reference, without a defensive copy or validation.
   * The caller must keep the geometry and its coordinate sequences stable during query use.
   */
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
