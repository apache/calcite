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
package net.hydromatic.optiq.jdbc;

import net.hydromatic.optiq.Function;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.Table;

import org.eigenbase.util.Pair;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An {@link net.hydromatic.optiq.jdbc.OptiqSchema} implementation that
 * maintains minimal state.
 */
public class SimpleOptiqSchema extends OptiqSchema {

  private Map<String, SimpleOptiqSchema> subSchemas = Maps.newHashMap();
  private Map<String, TableEntry> tables = Maps.newHashMap();

  public SimpleOptiqSchema(OptiqSchema parent, Schema schema, String name) {
    super(parent, schema, name);
  }

  @Override
  public TableEntry add(String tableName, Table table) {
    TableEntry e = new TableEntryImpl(this, tableName, table);
    tables.put(tableName, e);
    return e;
  }

  @Override
  public OptiqSchema getSubSchema(String schemaName, boolean caseSensitive) {
    Schema s = schema.getSubSchema(schemaName);
    if (s != null) {
      return new SimpleOptiqSchema(this, s, schemaName);
    }
    return subSchemas.get(schemaName);
  }

  @Override
  public OptiqSchema add(String name, Schema schema) {
    SimpleOptiqSchema s = new SimpleOptiqSchema(this, schema, name);
    subSchemas.put(name, s);
    return s;
  }

  @Override
  public Pair<String, Table> getTable(String tableName, boolean caseSensitive) {
    Table t = schema.getTable(tableName);
    if (t == null) {
      TableEntry e = tables.get(tableName);
      if (e != null) {
        t = e.getTable();
      }
    }
    if (t != null) {
      return new Pair<String, Table>(tableName, t);
    }

    return null;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Collection<TableEntry> getTableEntries() {
    return Collections.EMPTY_LIST;
  }

  @Override
  public Set<String> getSubSchemaNames() {
    return Sets.union(schema.getSubSchemaNames(), subSchemas.keySet());
  }

  @Override
  public Collection<OptiqSchema> getSubSchemas() {
    List<OptiqSchema> schemas = Lists.newLinkedList();
    schemas.addAll(subSchemas.values());
    for (String name : schema.getSubSchemaNames()) {
      schemas.add(getSubSchema(name, true));
    }
    return schemas;
  }

  @Override
  public Set<String> getTableNames() {
    return Sets.union(schema.getTableNames(), tables.keySet());
  }

  @SuppressWarnings("unchecked")
  @Override
  public Collection<Function> getFunctions(String name, boolean caseSensitive) {
    return Collections.EMPTY_LIST;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Set<String> getFunctionNames() {
    return Collections.EMPTY_SET;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Table> getTablesBasedOnNullaryFunctions() {
    return Collections.EMPTY_MAP;
  }

  @Override
  public Pair<String, Table> getTableBasedOnNullaryFunction(String tableName,
      boolean caseSensitive) {
    return null;
  }

  @Override
  protected FunctionEntry add(String name, Function function) {
    throw new UnsupportedOperationException();
  }

  public static SchemaPlus createRootSchema(boolean addMetadataSchema) {
    SimpleOptiqRootSchema rootSchema =
        new SimpleOptiqRootSchema(new OptiqConnectionImpl.RootSchema());
    if (addMetadataSchema) {
      rootSchema.add("metadata", MetadataSchema.INSTANCE);
    }
    return rootSchema.plus();
  }
}
