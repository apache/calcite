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
package org.apache.calcite.prepare;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.AggregateFunction;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlMoniker;
import org.apache.calcite.sql.validate.SqlMonikerImpl;
import org.apache.calcite.sql.validate.SqlMonikerType;
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import org.apache.calcite.sql.validate.SqlUserDefinedTableMacro;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Util;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;

/**
 * Implementation of {@link org.apache.calcite.prepare.Prepare.CatalogReader}
 * and also {@link org.apache.calcite.sql.SqlOperatorTable} based on tables and
 * functions defined schemas.
 */
public class CalciteCatalogReader implements Prepare.CatalogReader,
    SqlOperatorTable {
  final CalciteSchema rootSchema;
  final JavaTypeFactory typeFactory;
  private final List<String> defaultSchema;
  private final boolean elideRecord = true;
  private final boolean caseSensitive;

  public CalciteCatalogReader(
      CalciteSchema rootSchema,
      boolean caseSensitive,
      List<String> defaultSchema,
      JavaTypeFactory typeFactory) {
    super();
    assert rootSchema != defaultSchema;
    this.rootSchema = rootSchema;
    this.caseSensitive = caseSensitive;
    this.defaultSchema = defaultSchema;
    this.typeFactory = typeFactory;
  }

  public CalciteCatalogReader withSchemaPath(List<String> schemaPath) {
    return new CalciteCatalogReader(rootSchema, caseSensitive, schemaPath,
        typeFactory);
  }

  public RelOptTableImpl getTable(final List<String> names) {
    // First look in the default schema, if any.
    if (defaultSchema != null) {
      RelOptTableImpl table = getTableFrom(names, defaultSchema);
      if (table != null) {
        return table;
      }
    }
    // If not found, look in the root schema
    return getTableFrom(names, ImmutableList.<String>of());
  }

  private RelOptTableImpl getTableFrom(List<String> names,
      List<String> schemaNames) {
    CalciteSchema schema =
        getSchema(Iterables.concat(schemaNames, Util.skipLast(names)));
    if (schema == null) {
      return null;
    }
    final String name = Util.last(names);
    CalciteSchema.TableEntry entry = schema.getTable(name, caseSensitive);
    if (entry == null) {
      entry = schema.getTableBasedOnNullaryFunction(name, caseSensitive);
    }
    if (entry != null) {
      final Table table = entry.getTable();
      final String name2 = entry.name;
      return RelOptTableImpl.create(this, table.getRowType(typeFactory),
          schema.add(name2, table), null);
    }
    return null;
  }

  private Collection<Function> getFunctionsFrom(List<String> names) {
    final List<Function> functions2 = Lists.newArrayList();
    final List<? extends List<String>> schemaNameList;
    if (names.size() > 1) {
      // If name is qualified, ignore path.
      schemaNameList = ImmutableList.of(ImmutableList.<String>of());
    } else {
      CalciteSchema schema = getSchema(defaultSchema);
      if (schema == null) {
        schemaNameList = ImmutableList.of();
      } else {
        schemaNameList = schema.getPath();
      }
    }
    for (List<String> schemaNames : schemaNameList) {
      CalciteSchema schema =
          getSchema(Iterables.concat(schemaNames, Util.skipLast(names)));
      if (schema != null) {
        final String name = Util.last(names);
        functions2.addAll(schema.getFunctions(name, true));
      }
    }
    return functions2;
  }

  private CalciteSchema getSchema(Iterable<String> schemaNames) {
    CalciteSchema schema = rootSchema;
    for (String schemaName : schemaNames) {
      schema = schema.getSubSchema(schemaName, caseSensitive);
      if (schema == null) {
        return null;
      }
    }
    return schema;
  }

  public RelDataType getNamedType(SqlIdentifier typeName) {
    return null;
  }

  public List<SqlMoniker> getAllSchemaObjectNames(List<String> names) {
    final CalciteSchema schema = getSchema(names);
    if (schema == null) {
      return ImmutableList.of();
    }
    final List<SqlMoniker> result = new ArrayList<SqlMoniker>();
    final Map<String, CalciteSchema> schemaMap = schema.getSubSchemaMap();

    for (String subSchema : schemaMap.keySet()) {
      result.add(
          new SqlMonikerImpl(schema.path(subSchema), SqlMonikerType.SCHEMA));
    }

    for (String table : schema.getTableNames()) {
      result.add(
          new SqlMonikerImpl(schema.path(table), SqlMonikerType.TABLE));
    }

    final NavigableSet<String> functions = schema.getFunctionNames();
    for (String function : functions) { // views are here as well
      result.add(
          new SqlMonikerImpl(schema.path(function), SqlMonikerType.FUNCTION));
    }
    return result;
  }

  public List<String> getSchemaName() {
    return defaultSchema;
  }

  public RelOptTableImpl getTableForMember(List<String> names) {
    return getTable(names);
  }

  public RelDataTypeField field(RelDataType rowType, String alias) {
    return SqlValidatorUtil.lookupField(caseSensitive, elideRecord, rowType,
        alias);
  }

  public int fieldOrdinal(RelDataType rowType, String alias) {
    RelDataTypeField field = field(rowType, alias);
    return field != null ? field.getIndex() : -1;
  }

  public boolean matches(String string, String name) {
    return Util.matches(caseSensitive, string, name);
  }

  public int match(List<String> strings, String name) {
    return Util.findMatch(strings, name, caseSensitive);
  }

  public RelDataType createTypeFromProjection(final RelDataType type,
      final List<String> columnNameList) {
    return SqlValidatorUtil.createTypeFromProjection(type, columnNameList,
        typeFactory, caseSensitive, elideRecord);
  }

  public void lookupOperatorOverloads(final SqlIdentifier opName,
      SqlFunctionCategory category,
      SqlSyntax syntax,
      List<SqlOperator> operatorList) {
    if (syntax != SqlSyntax.FUNCTION) {
      return;
    }
    final Collection<Function> functions = getFunctionsFrom(opName.names);
    if (functions.isEmpty()) {
      return;
    }
    operatorList.addAll(
        Collections2.transform(functions,
            new com.google.common.base.Function<Function, SqlOperator>() {
              public SqlOperator apply(Function function) {
                return toOp(opName, function);
              }
            }));
  }

  private SqlOperator toOp(SqlIdentifier name, Function function) {
    List<RelDataType> argTypes = new ArrayList<RelDataType>();
    List<SqlTypeFamily> typeFamilies = new ArrayList<SqlTypeFamily>();
    for (FunctionParameter o : function.getParameters()) {
      final RelDataType type = o.getType(typeFactory);
      argTypes.add(type);
      typeFamilies.add(
          Util.first(type.getSqlTypeName().getFamily(), SqlTypeFamily.ANY));
    }
    final RelDataType returnType;
    if (function instanceof ScalarFunction) {
      return new SqlUserDefinedFunction(name,
          ReturnTypes.explicit(Schemas.proto((ScalarFunction) function)),
          InferTypes.explicit(argTypes), OperandTypes.family(typeFamilies),
          toSql(argTypes), function);
    } else if (function instanceof AggregateFunction) {
      returnType = ((AggregateFunction) function).getReturnType(typeFactory);
      return new SqlUserDefinedAggFunction(name,
          ReturnTypes.explicit(returnType), InferTypes.explicit(argTypes),
          OperandTypes.family(typeFamilies), (AggregateFunction) function);
    } else if (function instanceof TableMacro) {
      return new SqlUserDefinedTableMacro(name,
          ReturnTypes.CURSOR,
          InferTypes.explicit(argTypes), OperandTypes.family(typeFamilies),
          (TableMacro) function);
    } else if (function instanceof TableFunction) {
      return new SqlUserDefinedTableFunction(name,
          ReturnTypes.CURSOR,
          InferTypes.explicit(argTypes), OperandTypes.family(typeFamilies),
          toSql(argTypes), (TableFunction) function);
    } else {
      throw new AssertionError("unknown function type " + function);
    }
  }

  private List<RelDataType> toSql(List<RelDataType> types) {
    return Lists.transform(types,
        new com.google.common.base.Function<RelDataType, RelDataType>() {
          public RelDataType apply(RelDataType input) {
            if (input instanceof RelDataTypeFactoryImpl.JavaType
                && ((RelDataTypeFactoryImpl.JavaType) input).getJavaClass()
                == Object.class) {
              return typeFactory.createTypeWithNullability(
                  typeFactory.createSqlType(SqlTypeName.ANY), true);
            }
            return typeFactory.toSql(input);
          }
        });
  }

  public List<SqlOperator> getOperatorList() {
    return null;
  }

  public RelDataTypeFactory getTypeFactory() {
    return typeFactory;
  }

  public void registerRules(RelOptPlanner planner) throws Exception {
  }
}

// End CalciteCatalogReader.java
