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

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Custom table schema element.
 *
 * <p>Like base class {@link JsonTable},
 * occurs within {@link JsonMapSchema#tables}.
 *
 * @see JsonRoot Description of schema elements
 */
public class JsonCustomTable extends JsonTable {
  /** Name of the factory class for this table.
   *
   * <p>Required. Must implement interface
   * {@link org.apache.calcite.schema.TableFactory} and have a public default
   * constructor.
   */
  public final String factory;

  /** Contains attributes to be passed to the factory.
   *
   * <p>May be a JSON object (represented as Map) or null.
   */
  public final @Nullable Map<String, Object> operand;

  @JsonCreator
  public JsonCustomTable(
      @JsonProperty(value = "name", required = true) String name,
      @JsonProperty("stream") JsonStream stream,
      @JsonProperty(value = "factory", required = true) String factory,
      @JsonProperty("operand") @Nullable Map<String, Object> operand) {
    super(name, stream);
    this.factory = requireNonNull(factory, "factory");
    this.operand = operand;
  }


  @Override public void accept(ModelHandler handler) {
    handler.visit(this);
  }
}
