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

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;
import org.locationtech.jts.geom.util.GeometryTransformer;

import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Transforms the coordinates of a geometry.
 */
public class CoordinateTransformer extends GeometryTransformer {

  private final Function<Coordinate, Coordinate> transform;

  /**
   * Creates a transformer that applies the {@code transform} to all coordinates.
   */
  public CoordinateTransformer(Function<Coordinate, Coordinate> transform) {
    this.transform = transform;
  }

  @Override protected CoordinateSequence transformCoordinates(
      CoordinateSequence coordinateSequence, Geometry parent) {
    Coordinate[] coordinateArray =
        Stream.of(coordinateSequence.toCoordinateArray())
            .map(transform)
            .toArray(Coordinate[]::new);
    return new CoordinateArraySequence(coordinateArray);
  }

}
