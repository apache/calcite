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
package net.hydromatic.optiq.model;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Schema schema element.
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
  public String name;

  /** SQL-path. May be null, or a list, each element of which is a string or a
   * string-list. */
  public List<Object> path;

  public final List<JsonMaterialization> materializations =
      Lists.newArrayList();

  public final List<JsonLattice> lattices = Lists.newArrayList();

  /** Whether to cache metadata (tables, functions and sub-schemas) generated
   * by this schema. Default value is {@code true}.
   *
   * <p>If {@code false}, Optiq will go back to the schema each time it needs
   * metadata, for example, each time it needs a list of tables in order to
   * validate a query against the schema.</p>
   *
   * <p>If {@code true}, Optiq will cache the metadata the first time it reads
   * it. This can lead to better performance, especially if name-matching is
   * case-insensitive
   * (see {@link net.hydromatic.optiq.config.Lex#caseSensitive}).
   * However, it also leads to the problem of cache staleness.
   * A particular schema implementation can override the
   * {@link net.hydromatic.optiq.Schema#contentsHaveChangedSince(long, long)}
   * method to tell Optiq when it should consider its cache to be out of
   * date.</p>
   *
   * <p>Tables, functions and sub-schemas explicitly created in a schema are
   * not affected by this caching mechanism. They always appear in the schema
   * immediately, and are never flushed.</p>
   */
  public Boolean cache;

  public abstract void accept(ModelHandler handler);

  public void visitChildren(ModelHandler modelHandler) {
    for (JsonLattice jsonLattice : lattices) {
      jsonLattice.accept(modelHandler);
    }
    for (JsonMaterialization jsonMaterialization : materializations) {
      jsonMaterialization.accept(modelHandler);
    }
  }
}

// End JsonSchema.java
