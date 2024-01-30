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
package org.apache.calcite.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * JSON schema element that represents a custom schema.
 *
 * <p>Like the base class {@link JsonSchema},
 * occurs within {@link JsonRoot#schemas}.
 *
 * @see org.apache.calcite.model.JsonRoot Description of schema elements
 */
public class JsonCustomSchema extends JsonMapSchema {
  /** Name of the factory class for this schema.
   *
   * <p>Required. Must implement interface
   * {@link org.apache.calcite.schema.SchemaFactory} and have a public default
   * constructor.
   */
  public final String factory;

  /** Contains attributes to be passed to the factory.
   *
   * <p>May be a JSON object (represented as Map) or null.
   */
  public final @Nullable Map<String, Object> operand;

  @JsonCreator
  public JsonCustomSchema(
      @JsonProperty(value = "name", required = true) String name,
      @JsonProperty("path") @Nullable List<Object> path,
      @JsonProperty("cache") @Nullable Boolean cache,
      @JsonProperty("autoLattice") @Nullable Boolean autoLattice,
      @JsonProperty(value = "factory", required = true) String factory,
      @JsonProperty("operand") @Nullable Map<String, Object> operand) {
    super(name, path, cache, autoLattice);
    this.factory = requireNonNull(factory, "factory");
    this.operand = operand;
  }

  @Override public void accept(ModelHandler handler) {
    handler.visit(this);
  }

  @Override public String toString() {
    return "JsonCustomSchema(name=" + name + ")";
  }
}
