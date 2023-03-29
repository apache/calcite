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

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Root schema element.
 *
 * <p>A POJO with fields of {@link Boolean}, {@link String}, {@link ArrayList},
 * {@link java.util.LinkedHashMap LinkedHashMap}, per Jackson simple data
 * binding.</p>
 *
 * <p>Schema structure is as follows:</p>
 *
 * <!-- CHECKSTYLE: OFF -->
 * <pre>{@code Root}
 *   {@link JsonSchema} (in collection {@link JsonRoot#schemas schemas})
 *     {@link JsonType} (in collection {@link JsonMapSchema#types types})
 *     {@link JsonTable} (in collection {@link JsonMapSchema#tables tables})
 *       {@link JsonColumn} (in collection {@link JsonTable#columns columns})
 *       {@link JsonStream} (in field {@link JsonTable#stream stream})
 *     {@link JsonView}
 *     {@link JsonFunction} (in collection {@link JsonMapSchema#functions functions})
 *     {@link JsonLattice} (in collection {@link JsonSchema#lattices lattices})
 *       {@link JsonMeasure} (in collection {@link JsonLattice#defaultMeasures defaultMeasures})
 *       {@link JsonTile} (in collection {@link JsonLattice#tiles tiles})
 *         {@link JsonMeasure} (in collection {@link JsonTile#measures measures})
 *     {@link JsonMaterialization} (in collection {@link JsonSchema#materializations materializations})
 * </pre>
 * <!-- CHECKSTYLE: ON -->
 *
 * <p>See the <a href="http://calcite.apache.org/docs/model.html">JSON
 * model reference</a>.
 */
public class JsonRoot {
  /** Schema model version number. Required, must have value "1.0". */
  public final String version;

  /** Name of the schema that will become the default schema for connections
   * to Calcite that use this model.
   *
   * <p>Optional, case-sensitive. If specified, there must be a schema in this
   * model with this name.
   */
  public final @Nullable String defaultSchema;

  /** List of schema elements.
   *
   * <p>The list may be empty.
   */
  public final List<JsonSchema> schemas = new ArrayList<>();

  /** Types in the root schema. Shared by all schemas in the model.
   *
   * <p>The list may be empty.
   */
  public final List<JsonType> types = new ArrayList<>();

  @JsonCreator
  public JsonRoot(
      @JsonProperty(value = "version", required = true) String version,
      @JsonProperty("defaultSchema") @Nullable String defaultSchema) {
    this.version = requireNonNull(version, "version");
    this.defaultSchema = defaultSchema;
  }
}
