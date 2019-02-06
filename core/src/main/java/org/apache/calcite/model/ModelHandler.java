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

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.materialize.Lattice;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.AggregateFunction;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFactory;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AggregateFunctionImpl;
import org.apache.calcite.schema.impl.MaterializedViewTable;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.schema.impl.TableFunctionImpl;
import org.apache.calcite.schema.impl.TableMacroImpl;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.sql.SqlDialectFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.sql.DataSource;

/**
 * Reads a model and creates schema objects accordingly.
 */
public class ModelHandler {
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper()
      .configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
      .configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
      .configure(JsonParser.Feature.ALLOW_COMMENTS, true);
  private static final ObjectMapper YAML_MAPPER = new YAMLMapper();

  private final CalciteConnection connection;
  private final Deque<Pair<String, SchemaPlus>> schemaStack = new ArrayDeque<>();
  private final String modelUri;
  Lattice.Builder latticeBuilder;
  Lattice.TileBuilder tileBuilder;

  public ModelHandler(CalciteConnection connection, String uri)
      throws IOException {
    super();
    this.connection = connection;
    this.modelUri = uri;

    JsonRoot root;
    ObjectMapper mapper;
    if (uri.startsWith("inline:")) {
      // trim here is to correctly autodetect if it is json or not in case of leading spaces
      String inline = uri.substring("inline:".length()).trim();
      mapper = (inline.startsWith("/*") || inline.startsWith("{"))
          ? JSON_MAPPER
          : YAML_MAPPER;
      root = mapper.readValue(inline, JsonRoot.class);
    } else {
      mapper = uri.endsWith(".yaml") || uri.endsWith(".yml") ? YAML_MAPPER : JSON_MAPPER;
      root = mapper.readValue(new File(uri), JsonRoot.class);
    }
    visit(root);
  }

  /** @deprecated Use {@link #addFunctions}. */
  @Deprecated
  public static void create(SchemaPlus schema, String functionName,
      List<String> path, String className, String methodName) {
    addFunctions(schema, functionName, path, className, methodName, false);
  }

  /** Creates and validates a {@link ScalarFunctionImpl}, and adds it to a
   * schema. If {@code methodName} is "*", may add more than one function.
   *
   * @param schema Schema to add to
   * @param functionName Name of function; null to derived from method name
   * @param path Path to look for functions
   * @param className Class to inspect for methods that may be user-defined
   *                  functions
   * @param methodName Method name;
   *                  null means use the class as a UDF;
   *                  "*" means add all methods
   * @param upCase Whether to convert method names to upper case, so that they
   *               can be called without using quotes
   */
  public static void addFunctions(SchemaPlus schema, String functionName,
      List<String> path, String className, String methodName, boolean upCase) {
    final Class<?> clazz;
    try {
      clazz = Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("UDF class '"
          + className + "' not found");
    }
    final TableFunction tableFunction =
        TableFunctionImpl.create(clazz, Util.first(methodName, "eval"));
    if (tableFunction != null) {
      schema.add(functionName, tableFunction);
      return;
    }
    // Must look for TableMacro before ScalarFunction. Both have an "eval"
    // method.
    final TableMacro macro = TableMacroImpl.create(clazz);
    if (macro != null) {
      schema.add(functionName, macro);
      return;
    }
    if (methodName != null && methodName.equals("*")) {
      for (Map.Entry<String, ScalarFunction> entry
          : ScalarFunctionImpl.createAll(clazz).entries()) {
        String name = entry.getKey();
        if (upCase) {
          name = name.toUpperCase(Locale.ROOT);
        }
        schema.add(name, entry.getValue());
      }
      return;
    } else {
      final ScalarFunction function =
          ScalarFunctionImpl.create(clazz, Util.first(methodName, "eval"));
      if (function != null) {
        final String name;
        if (functionName != null) {
          name = functionName;
        } else if (upCase) {
          name = methodName.toUpperCase(Locale.ROOT);
        } else {
          name = methodName;
        }
        schema.add(name, function);
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

  private void checkRequiredAttributes(Object json, String... attributeNames) {
    for (String attributeName : attributeNames) {
      try {
        final Class<?> c = json.getClass();
        final Field f = c.getField(attributeName);
        final Object o = f.get(json);
        if (o == null) {
          throw new RuntimeException("Field '" + attributeName
              + "' is required in " + c.getSimpleName());
        }
      } catch (NoSuchFieldException | IllegalAccessException e) {
        throw new RuntimeException("while accessing field " + attributeName,
            e);
      }
    }
  }

  public void visit(JsonRoot jsonRoot) {
    checkRequiredAttributes(jsonRoot, "version");
    final Pair<String, SchemaPlus> pair =
        Pair.of(null, connection.getRootSchema());
    schemaStack.push(pair);
    for (JsonSchema schema : jsonRoot.schemas) {
      schema.accept(this);
    }
    final Pair<String, SchemaPlus> p = schemaStack.pop();
    assert p == pair;
    if (jsonRoot.defaultSchema != null) {
      try {
        connection.setSchema(jsonRoot.defaultSchema);
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public void visit(JsonMapSchema jsonSchema) {
    checkRequiredAttributes(jsonSchema, "name");
    final SchemaPlus parentSchema = currentMutableSchema("schema");
    final SchemaPlus schema =
        parentSchema.add(jsonSchema.name, new AbstractSchema());
    if (jsonSchema.path != null) {
      schema.setPath(stringListList(jsonSchema.path));
    }
    populateSchema(jsonSchema, schema);
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
    if (jsonSchema.cache != null) {
      schema.setCacheEnabled(jsonSchema.cache);
    }
    final Pair<String, SchemaPlus> pair = Pair.of(jsonSchema.name, schema);
    schemaStack.push(pair);
    jsonSchema.visitChildren(this);
    final Pair<String, SchemaPlus> p = schemaStack.pop();
    assert p == pair;
  }

  public void visit(JsonCustomSchema jsonSchema) {
    try {
      final SchemaPlus parentSchema = currentMutableSchema("sub-schema");
      checkRequiredAttributes(jsonSchema, "name", "factory");
      final SchemaFactory schemaFactory =
          AvaticaUtils.instantiatePlugin(SchemaFactory.class,
              jsonSchema.factory);
      final Schema schema =
          schemaFactory.create(
              parentSchema, jsonSchema.name, operandMap(jsonSchema, jsonSchema.operand));
      final SchemaPlus schemaPlus = parentSchema.add(jsonSchema.name, schema);
      populateSchema(jsonSchema, schemaPlus);
    } catch (Exception e) {
      throw new RuntimeException("Error instantiating " + jsonSchema, e);
    }
  }

  /** Adds extra entries to an operand to a custom schema. */
  protected Map<String, Object> operandMap(JsonSchema jsonSchema,
      Map<String, Object> operand) {
    if (operand == null) {
      return ImmutableMap.of();
    }
    final ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    builder.putAll(operand);
    for (ExtraOperand extraOperand : ExtraOperand.values()) {
      if (!operand.containsKey(extraOperand.camelName)) {
        switch (extraOperand) {
        case MODEL_URI:
          builder.put(extraOperand.camelName, modelUri);
          break;
        case BASE_DIRECTORY:
          File f = null;
          if (!modelUri.startsWith("inline:")) {
            final File file = new File(modelUri);
            f = file.getParentFile();
          }
          if (f == null) {
            f = new File("");
          }
          builder.put(extraOperand.camelName, f);
          break;
        case TABLES:
          if (jsonSchema instanceof JsonCustomSchema) {
            builder.put(extraOperand.camelName,
                ((JsonCustomSchema) jsonSchema).tables);
          }
          break;
        }
      }
    }
    return builder.build();
  }

  public void visit(JsonJdbcSchema jsonSchema) {
    checkRequiredAttributes(jsonSchema, "name");
    final SchemaPlus parentSchema = currentMutableSchema("jdbc schema");
    final DataSource dataSource =
        JdbcSchema.dataSource(jsonSchema.jdbcUrl,
            jsonSchema.jdbcDriver,
            jsonSchema.jdbcUser,
            jsonSchema.jdbcPassword);
    final JdbcSchema schema;
    if (jsonSchema.sqlDialectFactory == null || jsonSchema.sqlDialectFactory.isEmpty()) {
      schema =
          JdbcSchema.create(parentSchema, jsonSchema.name, dataSource,
              jsonSchema.jdbcCatalog, jsonSchema.jdbcSchema);
    } else {
      SqlDialectFactory factory = AvaticaUtils.instantiatePlugin(
          SqlDialectFactory.class, jsonSchema.sqlDialectFactory);
      schema =
          JdbcSchema.create(parentSchema, jsonSchema.name, dataSource,
              factory, jsonSchema.jdbcCatalog, jsonSchema.jdbcSchema);
    }
    final SchemaPlus schemaPlus = parentSchema.add(jsonSchema.name, schema);
    populateSchema(jsonSchema, schemaPlus);
  }

  public void visit(JsonMaterialization jsonMaterialization) {
    try {
      checkRequiredAttributes(jsonMaterialization, "sql");
      final SchemaPlus schema = currentSchema();
      if (!schema.isMutable()) {
        throw new RuntimeException(
            "Cannot define materialization; parent schema '"
                + currentSchemaName()
                + "' is not a SemiMutableSchema");
      }
      CalciteSchema calciteSchema = CalciteSchema.from(schema);

      final String viewName;
      final boolean existing;
      if (jsonMaterialization.view == null) {
        // If the user did not supply a view name, that means the materialized
        // view is pre-populated. Generate a synthetic view name.
        viewName = "$" + schema.getTableNames().size();
        existing = true;
      } else {
        viewName = jsonMaterialization.view;
        existing = false;
      }
      List<String> viewPath = calciteSchema.path(viewName);
      schema.add(viewName,
          MaterializedViewTable.create(calciteSchema,
              jsonMaterialization.getSql(), jsonMaterialization.viewSchemaPath, viewPath,
              jsonMaterialization.table, existing));
    } catch (Exception e) {
      throw new RuntimeException("Error instantiating " + jsonMaterialization,
          e);
    }
  }

  public void visit(JsonLattice jsonLattice) {
    try {
      checkRequiredAttributes(jsonLattice, "name", "sql");
      final SchemaPlus schema = currentSchema();
      if (!schema.isMutable()) {
        throw new RuntimeException("Cannot define lattice; parent schema '"
            + currentSchemaName()
            + "' is not a SemiMutableSchema");
      }
      CalciteSchema calciteSchema = CalciteSchema.from(schema);
      Lattice.Builder latticeBuilder =
          Lattice.builder(calciteSchema, jsonLattice.getSql())
              .auto(jsonLattice.auto)
              .algorithm(jsonLattice.algorithm);
      if (jsonLattice.rowCountEstimate != null) {
        latticeBuilder.rowCountEstimate(jsonLattice.rowCountEstimate);
      }
      if (jsonLattice.statisticProvider != null) {
        latticeBuilder.statisticProvider(jsonLattice.statisticProvider);
      }
      populateLattice(jsonLattice, latticeBuilder);
      schema.add(jsonLattice.name, latticeBuilder.build());
    } catch (Exception e) {
      throw new RuntimeException("Error instantiating " + jsonLattice, e);
    }
  }

  private void populateLattice(JsonLattice jsonLattice,
      Lattice.Builder latticeBuilder) {
    // By default, the default measure list is just {count(*)}.
    if (jsonLattice.defaultMeasures == null) {
      final JsonMeasure countMeasure = new JsonMeasure();
      countMeasure.agg = "count";
      jsonLattice.defaultMeasures = ImmutableList.of(countMeasure);
    }
    assert this.latticeBuilder == null;
    this.latticeBuilder = latticeBuilder;
    jsonLattice.visitChildren(this);
    this.latticeBuilder = null;
  }

  public void visit(JsonCustomTable jsonTable) {
    try {
      checkRequiredAttributes(jsonTable, "name", "factory");
      final SchemaPlus schema = currentMutableSchema("table");
      final TableFactory tableFactory =
          AvaticaUtils.instantiatePlugin(TableFactory.class,
              jsonTable.factory);
      final Table table =
          tableFactory.create(schema, jsonTable.name,
              operandMap(null, jsonTable.operand), null);
      for (JsonColumn column : jsonTable.columns) {
        column.accept(this);
      }
      schema.add(jsonTable.name, table);
    } catch (Exception e) {
      throw new RuntimeException("Error instantiating " + jsonTable, e);
    }
  }

  public void visit(JsonColumn jsonColumn) {
    checkRequiredAttributes(jsonColumn, "name");
  }

  public void visit(JsonView jsonView) {
    try {
      checkRequiredAttributes(jsonView, "name");
      final SchemaPlus schema = currentMutableSchema("view");
      final List<String> path = Util.first(jsonView.path, currentSchemaPath());
      final List<String> viewPath = ImmutableList.<String>builder().addAll(path)
          .add(jsonView.name).build();
      schema.add(jsonView.name,
          ViewTable.viewMacro(schema, jsonView.getSql(), path, viewPath,
              jsonView.modifiable));
    } catch (Exception e) {
      throw new RuntimeException("Error instantiating " + jsonView, e);
    }
  }

  private List<String> currentSchemaPath() {
    return Collections.singletonList(schemaStack.peek().left);
  }

  private SchemaPlus currentSchema() {
    return schemaStack.peek().right;
  }

  private String currentSchemaName() {
    return schemaStack.peek().left;
  }

  private SchemaPlus currentMutableSchema(String elementType) {
    final SchemaPlus schema = currentSchema();
    if (!schema.isMutable()) {
      throw new RuntimeException("Cannot define " + elementType
          + "; parent schema '" + schema.getName() + "' is not mutable");
    }
    return schema;
  }

  public void visit(final JsonType jsonType) {
    checkRequiredAttributes(jsonType, "name");
    try {
      final SchemaPlus schema = currentMutableSchema("type");
      schema.add(jsonType.name, typeFactory -> {
        if (jsonType.type != null) {
          return typeFactory.createSqlType(SqlTypeName.get(jsonType.type));
        } else {
          final RelDataTypeFactory.Builder builder = typeFactory.builder();
          for (JsonTypeAttribute jsonTypeAttribute : jsonType.attributes) {
            final SqlTypeName typeName =
                SqlTypeName.get(jsonTypeAttribute.type);
            RelDataType type = typeFactory.createSqlType(typeName);
            if (type == null) {
              type = currentSchema().getType(jsonTypeAttribute.type)
                  .apply(typeFactory);
            }
            builder.add(jsonTypeAttribute.name, type);
          }
          return builder.build();
        }
      });
    } catch (Exception e) {
      throw new RuntimeException("Error instantiating " + jsonType, e);
    }
  }

  public void visit(JsonFunction jsonFunction) {
    // "name" is not required - a class can have several functions
    checkRequiredAttributes(jsonFunction, "className");
    try {
      final SchemaPlus schema = currentMutableSchema("function");
      final List<String> path =
          Util.first(jsonFunction.path, currentSchemaPath());
      addFunctions(schema, jsonFunction.name, path, jsonFunction.className,
          jsonFunction.methodName, false);
    } catch (Exception e) {
      throw new RuntimeException("Error instantiating " + jsonFunction, e);
    }
  }

  public void visit(JsonMeasure jsonMeasure) {
    checkRequiredAttributes(jsonMeasure, "agg");
    assert latticeBuilder != null;
    final boolean distinct = false; // no distinct field in JsonMeasure.yet
    final Lattice.Measure measure =
        latticeBuilder.resolveMeasure(jsonMeasure.agg, distinct,
            jsonMeasure.args);
    if (tileBuilder != null) {
      tileBuilder.addMeasure(measure);
    } else if (latticeBuilder != null) {
      latticeBuilder.addMeasure(measure);
    } else {
      throw new AssertionError("nowhere to put measure");
    }
  }

  public void visit(JsonTile jsonTile) {
    assert tileBuilder == null;
    tileBuilder = Lattice.Tile.builder();
    for (JsonMeasure jsonMeasure : jsonTile.measures) {
      jsonMeasure.accept(this);
    }
    for (Object dimension : jsonTile.dimensions) {
      final Lattice.Column column = latticeBuilder.resolveColumn(dimension);
      tileBuilder.addDimension(column);
    }
    latticeBuilder.addTile(tileBuilder.build());
    tileBuilder = null;
  }

  /** Extra operands automatically injected into a
   * {@link JsonCustomSchema#operand}, as extra context for the adapter. */
  public enum ExtraOperand {
    /** URI of model, e.g. "target/test-classes/model.json",
     * "http://localhost/foo/bar.json", "inline:{...}",
     * "target/test-classes/model.yaml",
     * "http://localhost/foo/bar.yaml", "inline:..."
     * */
    MODEL_URI("modelUri"),

    /** Base directory from which to read files. */
    BASE_DIRECTORY("baseDirectory"),

    /** Tables defined in this schema. */
    TABLES("tables");

    public final String camelName;

    ExtraOperand(String camelName) {
      this.camelName = camelName;
    }
  }
}

// End ModelHandler.java
