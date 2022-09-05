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
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;
import org.locationtech.jts.geom.util.GeometryTransformer;
import org.locationtech.proj4j.CRSFactory;
import org.locationtech.proj4j.CoordinateReferenceSystem;
import org.locationtech.proj4j.CoordinateTransform;
import org.locationtech.proj4j.CoordinateTransformFactory;
import org.locationtech.proj4j.ProjCoordinate;

import java.util.Locale;
import java.util.stream.Stream;

/**
 * Transforms the projection of a geometry.
 */
public class ProjectionTransformer extends GeometryTransformer {

  private final CoordinateTransform coordinateTransform;

  /**
   * Creates a transformer that reprojects geometries with the provided SRIDs.
   *
   * @param sourceSrid the source SRID
   * @param targetSrid the target SRID
   */
  public ProjectionTransformer(int sourceSrid, int targetSrid) {
    CRSFactory crsFactory = new CRSFactory();
    CoordinateReferenceSystem source = crsFactory
        .createFromName(String.format(Locale.ROOT, "epsg:%s", sourceSrid));
    CoordinateReferenceSystem target = crsFactory
        .createFromName(String.format(Locale.ROOT, "epsg:%s", targetSrid));
    CoordinateTransformFactory ctFactory = new CoordinateTransformFactory();
    coordinateTransform = ctFactory.createTransform(source, target);
  }

  @Override protected CoordinateSequence transformCoordinates(
      CoordinateSequence coordinateSequence, Geometry parent) {
    Coordinate[] coordinateArray =
        Stream.of(coordinateSequence.toCoordinateArray())
            .map(this::transformCoordinate)
            .toArray(Coordinate[]::new);
    return new CoordinateArraySequence(coordinateArray);
  }

  private Coordinate transformCoordinate(Coordinate coordinate) {
    ProjCoordinate c1 = new ProjCoordinate(coordinate.x, coordinate.y);
    ProjCoordinate c2 = coordinateTransform.transform(c1, new ProjCoordinate());
    return new Coordinate(c2.x, c2.y);
  }

  @Override protected Geometry transformPoint(Point geom, Geometry parent) {
    try {
      Geometry geometry = super.transformPoint(geom, parent);
      return withTargetSRID(geometry);
    } catch (Exception e) {
      return parent.getFactory().createEmpty(0);
    }
  }

  @Override protected Geometry transformMultiPoint(MultiPoint geom, Geometry parent) {
    try {
      Geometry geometry = super.transformMultiPoint(geom, parent);
      if (geometry instanceof Point) {
        geometry = factory.createMultiPoint(new Point[]{(Point) geometry});
      }
      return withTargetSRID(geometry);
    } catch (Exception e) {
      return parent.getFactory().createEmpty(0);
    }
  }

  @Override protected Geometry transformLinearRing(LinearRing geom, Geometry parent) {
    try {
      Geometry geometry = super.transformLinearRing(geom, parent);
      return withTargetSRID(geometry);
    } catch (Exception e) {
      return parent.getFactory().createEmpty(0);
    }
  }

  @Override protected Geometry transformLineString(LineString geom, Geometry parent) {
    try {
      Geometry geometry = super.transformLineString(geom, parent);
      return withTargetSRID(geometry);
    } catch (Exception e) {
      return parent.getFactory().createEmpty(0);
    }
  }

  @Override protected Geometry transformMultiLineString(MultiLineString geom, Geometry parent) {
    try {
      Geometry geometry = super.transformMultiLineString(geom, parent);
      if (geometry instanceof LineString) {
        geometry = factory.createMultiLineString(new LineString[]{(LineString) geometry});
      }
      return withTargetSRID(geometry);
    } catch (Exception e) {
      return parent.getFactory().createEmpty(0);
    }
  }

  @Override protected Geometry transformPolygon(Polygon geom, Geometry parent) {
    try {
      Geometry geometry = super.transformPolygon(geom, parent);
      return withTargetSRID(geometry);
    } catch (Exception e) {
      return parent.getFactory().createEmpty(0);
    }
  }

  @Override protected Geometry transformMultiPolygon(MultiPolygon geom, Geometry parent) {
    try {
      Geometry geometry = super.transformMultiPolygon(geom, parent);
      if (geometry instanceof Polygon) {
        geometry = factory.createMultiPolygon(new Polygon[]{(Polygon) geometry});
      }
      return withTargetSRID(geometry);
    } catch (Exception e) {
      return parent.getFactory().createEmpty(0);
    }
  }

  @Override protected Geometry transformGeometryCollection(
      GeometryCollection geom, Geometry parent) {
    try {
      Geometry geometry = super.transformGeometryCollection(geom, parent);
      return withTargetSRID(geometry);
    } catch (Exception e) {
      return parent.getFactory().createEmpty(0);
    }
  }

  private Geometry withTargetSRID(Geometry outputGeom) {
    int srid = coordinateTransform.getTargetCRS().getProjection().getEPSGCode();
    outputGeom.setSRID(srid);
    return outputGeom;
  }
}
