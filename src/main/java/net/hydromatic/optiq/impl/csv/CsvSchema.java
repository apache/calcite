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
package net.hydromatic.optiq.impl.csv;

import net.hydromatic.linq4j.*;
import net.hydromatic.linq4j.expressions.Expression;

import net.hydromatic.optiq.*;
import net.hydromatic.optiq.impl.TableInSchemaImpl;

import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.jdbc.OptiqConnection;

import java.io.*;
import java.util.*;

/**
 * Schema mapped onto a directory of CSV files. Each table in the schema
 * is a CSV file in that directory.
 */
public class CsvSchema implements Schema {
  private final Schema parentSchema;
  final File directoryFile;
  private final Expression expression;
  final JavaTypeFactory typeFactory;
  private Map<String, TableInSchema> map;

  public CsvSchema(
      Schema parentSchema,
      File directoryFile,
      Expression expression)
  {
    this.parentSchema = parentSchema;
    this.directoryFile = directoryFile;
    this.expression = expression;
    this.typeFactory = ((OptiqConnection) getQueryProvider()).getTypeFactory();
  }

  public Expression getExpression() {
    return expression;
  }

  public List<TableFunction> getTableFunctions(String name) {
    return Collections.emptyList();
  }

  public QueryProvider getQueryProvider() {
    return parentSchema.getQueryProvider();
  }

  public Collection<TableInSchema> getTables() {
    return computeMap().values();
  }

  public <T> Table<T> getTable(String name, Class<T> elementType) {
    final TableInSchema tableInSchema = computeMap().get(name);
    //noinspection unchecked
    return tableInSchema == null
        ? null
        : (Table) tableInSchema.getTable(elementType);
  }

  public Map<String, List<TableFunction>> getTableFunctions() {
    // this kind of schema does not have table functions
    return Collections.emptyMap();
  }

  public Schema getSubSchema(String name) {
    // this kind of schema does not have sub-schemas
    return null;
  }

  public Collection<String> getSubSchemaNames() {
    // this kind of schema does not have sub-schemas
    return Collections.emptyList();
  }

  /** Returns the map of tables by name, populating the map on first use. */
  private synchronized Map<String, TableInSchema> computeMap() {
    if (map == null) {
      map = new HashMap<String, TableInSchema>();
      File[] files = directoryFile.listFiles(
          new FilenameFilter() {
            public boolean accept(File dir, String name) {
              return name.endsWith(".csv");
            }
          });
      for (File file : files) {
        String tableName = file.getName();
        if (tableName.endsWith(".csv")) {
          tableName = tableName.substring(
              0, tableName.length() - ".csv".length());
        }
        final CsvTable table = CsvTable.create(this, file, tableName);
        map.put(
            tableName,
            new TableInSchemaImpl(this, tableName, TableType.TABLE, table));
      }
    }
    return map;
  }
}

// End CsvSchema.java
