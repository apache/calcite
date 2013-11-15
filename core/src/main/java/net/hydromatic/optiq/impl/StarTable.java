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

import net.hydromatic.linq4j.Enumerator;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.Schemas;
import net.hydromatic.optiq.Table;

import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.validate.SqlValidatorUtil;

import com.google.common.collect.ImmutableList;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

/**
 * Virtual table that is composed of two or more tables joined together.
 *
 * <p>Star tables do not occur in end-user queries. They are introduced by the
 * optimizer to help matching queries to materializations, and used only
 * during the planning process.</p>
 *
 * <p>When a materialization is defined, if it involves a join, it is converted
 * to a query on top of a star table. Queries that are candidates to map onto
 * the materialization are mapped onto the same star table.</p>
 */
public class StarTable<T>
    extends AbstractTable<T>
{
  // TODO: we'll also need a list of join conditions between tables. For now
  //  we assume that join conditions match
  public final ImmutableList<Table> tables;

  /** Creates a StarTable. */
  public StarTable(Schema schema, Type elementType, RelDataType rowType,
      String tableName, List<Table> tables) {
    super(schema, elementType, rowType, tableName);
    this.tables = ImmutableList.copyOf(tables);
  }

  /** Creates a StarTable and registers it in a schema. */
  public static <T> StarTable<T> of(Schema schema, String tableName,
      List<Table> tables) {
    final List<RelDataType> typeList = new ArrayList<RelDataType>();
    final List<String> nameList = new ArrayList<String>();
    for (Table table : tables) {
      typeList.addAll(RelOptUtil.getFieldTypeList(table.getRowType()));
      nameList.addAll(table.getRowType().getFieldNames());
    }
    final RelDataType rowType =
        schema.getTypeFactory().createStructType(
            typeList, SqlValidatorUtil.uniquify(nameList));
    return new StarTable<T>(schema, Object[].class, rowType, tableName, tables);
  }

  public Enumerator<T> enumerator() {
    throw new UnsupportedOperationException();
  }

  public StarTable add(Table table) {
    final List<Table> tables1 = new ArrayList<Table>(tables);
    tables1.add(table);
    return of(schema, tableName + tables.size(), tables1);
  }

  public List<String> getPath() {
    return Schemas.path(schema, tableName);
  }

  /** Returns the column offset of the first column of {@code table} in this
   * star table's output row type.
   *
   * @param table Table
   * @return Column offset
   * @throws IllegalArgumentException if table is not in this star
   */
  public int columnOffset(Table table) {
    int n = 0;
    for (Table table1 : tables) {
      if (table1 == table) {
        return n;
      }
      n += table1.getRowType().getFieldCount();
    }
    throw new IllegalArgumentException("star table " + this
        + " does not contain table " + table);
  }
}

// End StarTable.java
