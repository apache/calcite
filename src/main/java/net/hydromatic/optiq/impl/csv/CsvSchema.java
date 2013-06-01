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

import net.hydromatic.linq4j.expressions.Expression;

import net.hydromatic.optiq.*;
import net.hydromatic.optiq.impl.TableInSchemaImpl;
import net.hydromatic.optiq.impl.java.MapSchema;

import org.eigenbase.reltype.RelDataType;

import java.io.*;
import java.util.*;

/**
 * Schema mapped onto a directory of CSV files. Each table in the schema
 * is a CSV file in that directory.
 */
public class CsvSchema extends MapSchema {
  final File directoryFile;
  private final boolean smart;

  /**
   * Creates a CSV schema.
   *
   * @param parentSchema Parent schema
   * @param directoryFile Directory that holds .csv files
   * @param expression Java expression to create an instance of this schema
   *                   in generated code
   * @param smart      Whether to instantiate smart tables that undergo
   *                   query optimization
   */
  public CsvSchema(
      Schema parentSchema,
      File directoryFile,
      Expression expression,
      boolean smart) {
    super(parentSchema, expression);
    this.directoryFile = directoryFile;
    this.smart = smart;
  }

  @Override
  protected Collection<TableInSchema> initialTables() {
    final List<TableInSchema> list = new ArrayList<TableInSchema>();
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
      final List<CsvFieldType> fieldTypes = new ArrayList<CsvFieldType>();
      final RelDataType rowType =
          CsvTable.deduceRowType(typeFactory, file, fieldTypes);
      final CsvTable table;
      if (smart) {
        table = new CsvSmartTable(this, tableName, file, rowType, fieldTypes);
      } else {
        table = new CsvTable(this, tableName, file, rowType, fieldTypes);
      }
      list.add(new TableInSchemaImpl(this, tableName, TableType.TABLE, table));
    }
    return list;
  }
}

// End CsvSchema.java
