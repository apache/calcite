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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * Schema schema element.
 *
 * <p>Occurs within {@link JsonRoot#schemas}.
 *
 * @see JsonRoot Description of schema elements
 */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    property = "type",
    defaultImpl = JsonMapSchema.class)
@JsonSubTypes({
    @JsonSubTypes.Type(value = JsonMapSchema.class, name = "map"),
    @JsonSubTypes.Type(value = JsonJdbcSchema.class, name = "jdbc"),
    @JsonSubTypes.Type(value = JsonCustomSchema.class, name = "custom") })
public abstract class JsonSchema {
  /** Name of the schema.
   *
   * <p>Required.
   *
   * @see JsonRoot#defaultSchema
   */
  public String name;

  /** SQL path that is used to resolve functions used in this schema.
   *
   * <p>May be null, or a list, each element of which is a string or a
   * string-list.
   *
   * <p>For example,
   *
   * <blockquote><pre>path: [ ['usr', 'lib'], 'lib' ]</pre></blockquote>
   *
   * <p>declares a path with two elements: the schema '/usr/lib' and the schema
   * '/lib'. Most schemas are at the top level, and for these you can use a
   * string.
   */
  public List<Object> path;

  /**
   * List of tables in this schema that are materializations of queries.
   *
   * <p>The list may be empty.
   */
  public final List<JsonMaterialization> materializations = new ArrayList<>();

  public final List<JsonLattice> lattices = new ArrayList<>();

  /** Whether to cache metadata (tables, functions and sub-schemas) generated
   * by this schema. Default value is {@code true}.
   *
   * <p>If {@code false}, Calcite will go back to the schema each time it needs
   * metadata, for example, each time it needs a list of tables in order to
   * validate a query against the schema.</p>
   *
   * <p>If {@code true}, Calcite will cache the metadata the first time it reads
   * it. This can lead to better performance, especially if name-matching is
   * case-insensitive
   * (see {@link org.apache.calcite.config.Lex#caseSensitive}).</p>
   *
   * <p>Tables, functions and sub-schemas explicitly created in a schema are
   * not affected by this caching mechanism. They always appear in the schema
   * immediately, and are never flushed.</p>
   */
  public Boolean cache;

  /** Whether to create lattices in this schema based on queries occurring in
   * other schemas. Default value is {@code false}. */
  public Boolean autoLattice;

  public abstract void accept(ModelHandler handler);

  public void visitChildren(ModelHandler modelHandler) {
    for (JsonLattice jsonLattice : lattices) {
      jsonLattice.accept(modelHandler);
    }
    for (JsonMaterialization jsonMaterialization : materializations) {
      jsonMaterialization.accept(modelHandler);
    }
  }

  /** Built-in schema types. */
  public enum Type {
    MAP,
    JDBC,
    CUSTOM
  }
}

// End JsonSchema.java
