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

/**
 * Operation that adds a z value to a geometry.
 */
public class AddZTransformer extends GeometryTransformer {

  private final double zToAdd;

  public AddZTransformer(double zToAdd) {
    this.zToAdd = zToAdd;
  }

  @Override protected CoordinateSequence transformCoordinates(
      CoordinateSequence coordinates, Geometry parent) {
    Coordinate[] newCoordinates = new Coordinate[coordinates.size()];
    for (int i = 0; i < coordinates.size(); i++) {
      Coordinate current = coordinates.getCoordinate(i);
      if (!Double.isNaN(current.z)) {
        newCoordinates[i] = new Coordinate(current.x, current.y, current.z + zToAdd);
      }
    }
    return new CoordinateArraySequence(newCoordinates);
  }

}
