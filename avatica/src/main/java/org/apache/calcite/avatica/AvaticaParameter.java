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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

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

}

// End AvaticaParameter.java
