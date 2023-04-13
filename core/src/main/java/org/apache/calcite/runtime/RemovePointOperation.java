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
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.util.GeometryEditor;

/**
 * Geometry editor operation that removes a point to a geometry.
 */
public class RemovePointOperation extends GeometryEditor.CoordinateOperation {

  private final int index;

  public RemovePointOperation(int index) {
    this.index = index;
  }

  @Override public Coordinate[] edit(Coordinate[] coordinates, Geometry geometry) {
    if (index < 0 || index > coordinates.length - 1) {
      throw new IllegalArgumentException("Invalid index: " + index);
    }
    Coordinate[] newCoordinates = new Coordinate[coordinates.length - 1];
    for (int i = 0; i < index; i++) {
      newCoordinates[i] = (Coordinate) coordinates[i].clone();
    }
    for (int i = index + 1; i < coordinates.length; i++) {
      newCoordinates[i - 1] = (Coordinate) coordinates[i].clone();
    }
    return newCoordinates;
  }
}
