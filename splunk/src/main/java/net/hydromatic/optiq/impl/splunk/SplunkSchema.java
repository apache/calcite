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
package net.hydromatic.optiq.impl.splunk;

import net.hydromatic.linq4j.*;
import net.hydromatic.linq4j.expressions.Expression;

import net.hydromatic.optiq.*;
import net.hydromatic.optiq.Table;
import net.hydromatic.optiq.impl.TableInSchemaImpl;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.impl.splunk.search.SplunkConnection;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;

import com.google.common.collect.*;

import java.lang.reflect.Type;
import java.util.*;

/**
 * Splunk schema.
 */
public class SplunkSchema implements Schema {
  /** The name of the one and only table. */
  public static final String SPLUNK_TABLE_NAME = "splunk";

  public final QueryProvider queryProvider;
  private final Schema parentSchema;
  private final String name;
  public final SplunkConnection splunkConnection;
  private final JavaTypeFactory typeFactory;
  private final Expression expression;
  private final SplunkTable table;
  private final Map<String, TableInSchema> tableMap;

  /** Creates a SplunkSchema. */
  public SplunkSchema(
      QueryProvider queryProvider,
      Schema parentSchema,
      String name,
      SplunkConnection splunkConnection,
      JavaTypeFactory typeFactory,
      Expression expression) {
    this.queryProvider = queryProvider;
    this.parentSchema = parentSchema;
    this.name = name;
    this.splunkConnection = splunkConnection;
    this.typeFactory = typeFactory;
    this.expression = expression;
    RelDataType stringType = typeFactory.createType(String.class);
    final RelDataType rowType =
        typeFactory.createStructType(
            new RelDataTypeFactory.FieldInfoBuilder()
                .add("source", stringType)
                .add("sourcetype", stringType)
                .add("_extra", stringType));
    final Type elementType = typeFactory.getJavaClass(rowType);
    this.table =
        new SplunkTable(elementType, rowType, this, SPLUNK_TABLE_NAME);
    this.tableMap =
        ImmutableMap.<String, TableInSchema>of(SPLUNK_TABLE_NAME,
            new TableInSchemaImpl(this, SPLUNK_TABLE_NAME, TableType.TABLE,
                table));
  }

  public Schema getParentSchema() {
    return parentSchema;
  }

  public String getName() {
    return name;
  }

  public Expression getExpression() {
    return expression;
  }

  public Map<String, TableInSchema> getTables() {
    return tableMap;
  }

  public JavaTypeFactory getTypeFactory() {
    return typeFactory;
  }

  public Collection<TableFunctionInSchema> getTableFunctions(String name) {
    return ImmutableList.of();
  }

  public Table getTable(String name) {
    return name.equals(SPLUNK_TABLE_NAME)
        ? table
        : null;
  }

  public QueryProvider getQueryProvider() {
    return queryProvider;
  }

  public Multimap<String, TableFunctionInSchema> getTableFunctions() {
    return ImmutableMultimap.of();
  }

  public <T> Table<T> getTable(String name, Class<T> elementType) {
    //noinspection unchecked
    return getTable(name);
  }

  public Schema getSubSchema(String name) {
    return null;
  }

  public Collection<String> getSubSchemaNames() {
    return Collections.emptyList();
  }
}

// End SplunkSchema.java
