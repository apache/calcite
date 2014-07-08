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
package org.apache.optiq.model;

import org.apache.optiq.*;
import org.apache.optiq.impl.*;
import org.apache.optiq.impl.jdbc.JdbcSchema;
import org.apache.optiq.jdbc.OptiqConnection;
import org.apache.optiq.jdbc.OptiqSchema;

import org.apache.optiq.util.Pair;
import org.apache.optiq.util.Util;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.common.collect.ImmutableList;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import javax.sql.DataSource;

import static org.apache.optiq.util.Stacks.*;

/**
 * Reads a model and creates schema objects accordingly.
 */
public class ModelHandler {
  private final OptiqConnection connection;
  private final List<Pair<String, SchemaPlus>> schemaStack =
      new ArrayList<Pair<String, SchemaPlus>>();

  public ModelHandler(OptiqConnection connection, String uri)
    throws IOException {
    super();
    this.connection = connection;
    final ObjectMapper mapper = new ObjectMapper();
    mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
    mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
    JsonRoot root;
    if (uri.startsWith("inline:")) {
      root = mapper.readValue(
          uri.substring("inline:".length()), JsonRoot.class);
    } else {
      root = mapper.readValue(new File(uri), JsonRoot.class);
    }
    visit(root);
  }

  /** Creates and validates a ScalarFunctionImpl. */
  public static void create(SchemaPlus schema, String functionName,
      List<String> path, String className, String methodName) {
    final Class<?> clazz;
    try {
      clazz = Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("UDF class '"
          + className + "' not found");
    }
    // Must look for TableMacro before ScalarFunction. Both have an "eval"
    // method.
    final TableFunction tableFunction = TableFunctionImpl.create(clazz);
    if (tableFunction != null) {
      schema.add(functionName, tableFunction);
      return;
    }
    final TableMacro macro = TableMacroImpl.create(clazz);
    if (macro != null) {
      schema.add(functionName, macro);
      return;
    }
    if (methodName != null && methodName.equals("*")) {
      for (Map.Entry<String, ScalarFunction> entry
          : ScalarFunctionImpl.createAll(clazz).entries()) {
        schema.add(entry.getKey(), entry.getValue());
      }
      return;
    } else {
      final ScalarFunction function =
          ScalarFunctionImpl.create(clazz, Util.first(methodName, "eval"));
      if (function != null) {
        schema.add(Util.first(functionName, methodName), function);
        return;
      }
    }
    if (methodName == null) {
      final AggregateFunction aggFunction = AggregateFunctionImpl.create(clazz);
      if (aggFunction != null) {
        schema.add(functionName, aggFunction);
        return;
      }
    }
    throw new RuntimeException("Not a valid function class: " + clazz
        + ". Scalar functions and table macros have an 'eval' method; "
        + "aggregate functions have 'init' and 'add' methods, and optionally "
        + "'initAdd', 'merge' and 'result' methods.");
  }

  public void visit(JsonRoot root) {
    final Pair<String, SchemaPlus> pair =
        Pair.of(null, connection.getRootSchema());
    push(schemaStack, pair);
    for (JsonSchema schema : root.schemas) {
      schema.accept(this);
    }
    pop(schemaStack, pair);
    if (root.defaultSchema != null) {
      try {
        connection.setSchema(root.defaultSchema);
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public void visit(JsonMapSchema jsonSchema) {
    final SchemaPlus parentSchema = currentMutableSchema("schema");
    final SchemaPlus schema =
        parentSchema.add(jsonSchema.name, new AbstractSchema());
    if (jsonSchema.path != null) {
      schema.setPath(stringListList(jsonSchema.path));
    }
    populateSchema(jsonSchema, schema);
    if (schema.getName().equals("mat")) {
      // Inject by hand a Star Table. Later we'll add a JSON model element.
      final List<Table> tables = new ArrayList<Table>();
      final String[] tableNames = {
        "sales_fact_1997", "time_by_day", "product", "product_class"
      };
      final SchemaPlus schema2 = parentSchema.getSubSchema("foodmart");
      for (String tableName : tableNames) {
        tables.add(schema2.getTable(tableName));
      }
      final String tableName = "star";
      schema.add(tableName, StarTable.of(tables));
    }
  }

  private static ImmutableList<ImmutableList<String>> stringListList(
      List path) {
    final ImmutableList.Builder<ImmutableList<String>> builder =
        ImmutableList.builder();
    for (Object s : path) {
      builder.add(stringList(s));
    }
    return builder.build();
  }

  private static ImmutableList<String> stringList(Object s) {
    if (s instanceof String) {
      return ImmutableList.of((String) s);
    } else if (s instanceof List) {
      final ImmutableList.Builder<String> builder2 =
          ImmutableList.builder();
      for (Object o : (List) s) {
        if (o instanceof String) {
          builder2.add((String) o);
        } else {
          throw new RuntimeException("Invalid path element " + o
              + "; was expecting string");
        }
      }
      return builder2.build();
    } else {
      throw new RuntimeException("Invalid path element " + s
          + "; was expecting string or list of string");
    }
  }

  private void populateSchema(JsonSchema jsonSchema, SchemaPlus schema) {
    boolean cache = jsonSchema.cache == null || jsonSchema.cache;
    schema.setCacheEnabled(cache);
    final Pair<String, SchemaPlus> pair = Pair.of(jsonSchema.name, schema);
    push(schemaStack, pair);
    jsonSchema.visitChildren(this);
    pop(schemaStack, pair);
  }

  public void visit(JsonCustomSchema jsonSchema) {
    try {
      final SchemaPlus parentSchema = currentMutableSchema("sub-schema");
      final Class clazz = Class.forName(jsonSchema.factory);
      final SchemaFactory schemaFactory = (SchemaFactory) clazz.newInstance();
      final Schema schema =
          schemaFactory.create(
              parentSchema, jsonSchema.name, jsonSchema.operand);
      final SchemaPlus optiqSchema = parentSchema.add(jsonSchema.name, schema);
      populateSchema(jsonSchema, optiqSchema);
    } catch (Exception e) {
      throw new RuntimeException("Error instantiating " + jsonSchema, e);
    }
  }

  public void visit(JsonJdbcSchema jsonSchema) {
    final SchemaPlus parentSchema = currentMutableSchema("jdbc schema");
    final DataSource dataSource =
        JdbcSchema.dataSource(jsonSchema.jdbcUrl,
            jsonSchema.jdbcDriver,
            jsonSchema.jdbcUser,
            jsonSchema.jdbcPassword);
    JdbcSchema schema =
        JdbcSchema.create(parentSchema, jsonSchema.name, dataSource,
            jsonSchema.jdbcCatalog, jsonSchema.jdbcSchema);
    final SchemaPlus optiqSchema = parentSchema.add(jsonSchema.name, schema);
    populateSchema(jsonSchema, optiqSchema);
  }

  public void visit(JsonMaterialization jsonMaterialization) {
    try {
      final SchemaPlus schema = currentSchema();
      if (!schema.isMutable()) {
        throw new RuntimeException(
            "Cannot define materialization; parent schema '"
            + currentSchemaName()
            + "' is not a SemiMutableSchema");
      }
      OptiqSchema optiqSchema = OptiqSchema.from(schema);
      schema.add(jsonMaterialization.view,
          MaterializedViewTable.create(
              optiqSchema, jsonMaterialization.sql, null,
              jsonMaterialization.table));
    } catch (Exception e) {
      throw new RuntimeException("Error instantiating " + jsonMaterialization,
          e);
    }
  }

  public void visit(JsonCustomTable jsonTable) {
    try {
      final SchemaPlus schema = currentMutableSchema("table");
      final Class clazz = Class.forName(jsonTable.factory);
      final TableFactory tableFactory = (TableFactory) clazz.newInstance();
      final Table table =
          tableFactory.create(schema, jsonTable.name, jsonTable.operand, null);
      schema.add(jsonTable.name, table);
    } catch (Exception e) {
      throw new RuntimeException("Error instantiating " + jsonTable, e);
    }
  }

  public void visit(JsonView jsonView) {
    try {
      final SchemaPlus schema = currentMutableSchema("view");
      final List<String> path = Util.first(jsonView.path, currentSchemaPath());
      schema.add(jsonView.name,
          ViewTable.viewMacro(schema, jsonView.sql, path));
    } catch (Exception e) {
      throw new RuntimeException("Error instantiating " + jsonView, e);
    }
  }

  private List<String> currentSchemaPath() {
    return Collections.singletonList(peek(schemaStack).left);
  }

  private SchemaPlus currentSchema() {
    return peek(schemaStack).right;
  }

  private String currentSchemaName() {
    return peek(schemaStack).left;
  }

  private SchemaPlus currentMutableSchema(String elementType) {
    final SchemaPlus schema = currentSchema();
    if (!schema.isMutable()) {
      throw new RuntimeException(
          "Cannot define " + elementType + "; parent schema '"
          + schema.getName() + "' is not mutable");
    }
    return schema;
  }

  public void visit(JsonFunction jsonFunction) {
    try {
      final SchemaPlus schema = currentMutableSchema("function");
      final List<String> path =
          Util.first(jsonFunction.path, currentSchemaPath());
      create(schema,
          jsonFunction.name,
          path,
          jsonFunction.className,
          jsonFunction.methodName);
    } catch (Exception e) {
      throw new RuntimeException("Error instantiating " + jsonFunction, e);
    }
  }
}

// End ModelHandler.java
