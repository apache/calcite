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

import java.util.ArrayList;
import java.util.List;

/**
 * Removes repeated points from a geometry.
 */
public class RemoveRepeatedPointsTransformer extends GeometryTransformer {

  private double tolerance = 0;

  public RemoveRepeatedPointsTransformer() {
    super();
  }

  public RemoveRepeatedPointsTransformer(double tolerance) {
    super();
    this.tolerance = tolerance;
  }

  @Override protected CoordinateSequence transformCoordinates(
      CoordinateSequence coordinates, Geometry parent) {
    List<Coordinate> list = new ArrayList<>();
    Coordinate previous = coordinates.getCoordinate(0);
    list.add(previous);
    for (int i = 1; i < coordinates.size(); i++) {
      Coordinate current = coordinates.getCoordinate(i);
      double distance = current.distance(previous);
      if (distance > tolerance) {
        list.add(current);
        previous = current;
      }
    }
    Coordinate last = coordinates.getCoordinate(coordinates.size() - 1);
    double distance = last.distance(previous);
    if (distance <= tolerance) {
      list.set(list.size() - 1, last);
    }
    Coordinate[] array = list.toArray(new Coordinate[list.size()]);
    return new CoordinateArraySequence(array);
  }

}
