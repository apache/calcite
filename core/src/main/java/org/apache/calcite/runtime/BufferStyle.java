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

import org.locationtech.jts.operation.buffer.BufferParameters;

import java.util.Locale;

import static java.lang.Integer.parseInt;

/**
 * A parser for buffer styles as defined by PostGIS.
 */
public class BufferStyle {

  private int quadrantSegments = BufferParameters.DEFAULT_QUADRANT_SEGMENTS;

  private int endCapStyle = BufferParameters.CAP_ROUND;

  private int joinStyle = BufferParameters.JOIN_ROUND;

  private int side = 0;

  public BufferStyle(String style) {
    String[] parameters = style.toLowerCase(Locale.ROOT).split(" ");
    for (String parameter : parameters) {
      if (parameter == null || parameter.isEmpty()) {
        continue;
      }
      String[] keyValue = parameter.split("=");
      if (keyValue.length != 2) {
        throw new IllegalArgumentException("Invalid buffer style: " + style);
      }
      String key = keyValue[0];
      String value = keyValue[1];
      switch (key) {
      case "quad_segs":
        try {
          quadrantSegments = parseInt(value);
          break;
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException("Invalid buffer style: " + style);
        }
      case "endcap":
        switch (value) {
        case "round":
          endCapStyle = BufferParameters.CAP_ROUND;
          break;
        case "flat":
          endCapStyle = BufferParameters.CAP_FLAT;
          break;
        case "square":
          endCapStyle = BufferParameters.CAP_SQUARE;
          break;
        default:
          throw new IllegalArgumentException("Invalid buffer style: " + style);
        }
        break;
      case "join":
        switch (value) {
        case "round":
          joinStyle = BufferParameters.JOIN_ROUND;
          break;
        case "mitre":
          joinStyle = BufferParameters.JOIN_MITRE;
          break;
        case "bevel":
          joinStyle = BufferParameters.JOIN_BEVEL;
          break;
        default:
          throw new IllegalArgumentException("Invalid buffer style: " + style);
        }
        break;
      case "side":
        switch (value) {
        case "left":
          side += 1;
          break;
        case "right":
          side -= 1;
          break;
        case "both":
          side = 0;
          break;
        default:
          throw new IllegalArgumentException("Invalid buffer style: " + style);
        }
        break;
      }
    }
  }

  /**
   * Returns a sided distance.
   */
  public double asSidedDistance(double distance) {
    return side != 0 ? distance * side : distance;
  }

  /**
   * Returns buffer parameters.
   */
  public BufferParameters asBufferParameters() {
    BufferParameters params = new BufferParameters();
    params.setQuadrantSegments(quadrantSegments);
    params.setEndCapStyle(endCapStyle);
    params.setJoinStyle(joinStyle);
    params.setSingleSided(side != 0);
    return params;
  }
}
