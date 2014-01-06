/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.optiq.impl;

import net.hydromatic.optiq.*;
import net.hydromatic.optiq.Table;

import com.google.common.collect.*;

import java.util.*;

/**
 * Implementation of {@link Schema} backed by a {@link HashMap}.
 */
public class MapSchema extends AbstractSchema {
  protected final Map<String, Table> tableMap;
  protected final Multimap<String, TableFunction> membersMap;
  protected final Map<String, Schema> subSchemaMap;

  /**
   * Creates a MapSchema.
   *
   * @param parentSchema Parent schema (may be null)
   * @param name Name of schema
   */
  public MapSchema(SchemaPlus parentSchema, String name) {
    this(
        parentSchema,
        name,
        new HashMap<String, Table>(),
        LinkedListMultimap.<String, TableFunction>create(),
        new HashMap<String, Schema>());
  }

  /**
   * Creates a MapSchema, with explicit map objects for tables, members and
   * sub-schemas.
   *
   * @param parentSchema Parent schema (may be null)
   * @param name Name of schema
   * @param tableMap Table map
   * @param membersMap Members map
   * @param subSchemaMap Sub-schema map
   */
  public MapSchema(SchemaPlus parentSchema, String name,
      Map<String, Table> tableMap,
      ListMultimap<String, TableFunction> membersMap,
      Map<String, Schema> subSchemaMap) {
    super(parentSchema, name);
    this.tableMap = tableMap;
    this.membersMap = membersMap;
    this.subSchemaMap = subSchemaMap;

    assert name != null;
    assert tableMap != null;
    assert membersMap != null;
    assert subSchemaMap != null;
  }

  /** Called by Optiq after creation, before loading tables explicitly defined
   * in a JSON model. */
  public void initialize() {
    for (Map.Entry<String, Table> entry : initialTables().entrySet()) {
      tableMap.put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  protected Map<String, Table> getTableMap() {
    return tableMap;
  }

  @Override
  protected Multimap<String, TableFunction> getTableFunctionMultimap() {
    return membersMap;
  }

  @Override
  protected Map<String, Schema> getSubSchemaMap() {
    return subSchemaMap;
  }

  /** Returns the initial set of tables.
   *
   * <p>The default implementation returns an empty list. Derived classes
   * may override this method to create tables based on their schema type. For
   * example, a CSV provider might scan for all ".csv" files in a particular
   * directory and return a table for each.</p>
   */
  protected Map<String, Table> initialTables() {
    return ImmutableMap.of();
  }
}

// End MapSchema.java
