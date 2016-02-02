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
package org.apache.calcite.avatica;

import org.apache.calcite.avatica.proto.Common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Metadata for a parameter.
 */
public class AvaticaParameter {
  public final boolean signed;
  public final int precision;
  public final int scale;
  public final int parameterType;
  public final String typeName;
  public final String className;
  public final String name;

  @JsonCreator
  public AvaticaParameter(
      @JsonProperty("signed") boolean signed,
      @JsonProperty("precision") int precision,
      @JsonProperty("scale") int scale,
      @JsonProperty("parameterType") int parameterType,
      @JsonProperty("typeName") String typeName,
      @JsonProperty("className") String className,
      @JsonProperty("name") String name) {
    this.signed = signed;
    this.precision = precision;
    this.scale = scale;
    this.parameterType = parameterType;
    this.typeName = typeName;
    this.className = className;
    this.name = name;
  }

  public Common.AvaticaParameter toProto() {
    Common.AvaticaParameter.Builder builder = Common.AvaticaParameter.newBuilder();

    builder.setSigned(signed);
    builder.setPrecision(precision);
    builder.setScale(scale);
    builder.setParameterType(parameterType);
    builder.setTypeName(typeName);
    builder.setClassName(className);
    builder.setName(name);

    return builder.build();
  }

  public static AvaticaParameter fromProto(Common.AvaticaParameter proto) {
    return new AvaticaParameter(proto.getSigned(), proto.getPrecision(),
        proto.getScale(), proto.getParameterType(), proto.getTypeName(),
        proto.getClassName(), proto.getName());
  }

  @Override public int hashCode() {
    return Objects.hash(className, name, parameterType, precision, scale,
        signed, typeName);
  }

  @Override public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj instanceof AvaticaParameter) {
      AvaticaParameter other = (AvaticaParameter) obj;

      if (null == className) {
        if (null != other.className) {
          return false;
        }
      } else if (!className.equals(other.className)) {
        return false;
      }

      if (null == name) {
        if (null != other.name) {
          return false;
        }
      } else if (!name.equals(other.name)) {
        return false;
      }

      if (parameterType != other.parameterType) {
        return false;
      }

      if (precision != other.precision) {
        return false;
      }

      if (scale != other.scale) {
        return false;
      }

      if (signed != other.signed) {
        return false;
      }

      if (null == typeName) {
        if (null != other.typeName) {
          return false;
        }
      } else if (!typeName.equals(other.typeName)) {
        return false;
      }

      return true;
    }

    return false;
  }
}

// End AvaticaParameter.java
