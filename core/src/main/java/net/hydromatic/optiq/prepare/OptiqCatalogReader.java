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
package net.hydromatic.optiq.prepare;

import net.hydromatic.optiq.*;
import net.hydromatic.optiq.Function;
import net.hydromatic.optiq.Table;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.jdbc.OptiqSchema;

import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.util.Pair;
import org.eigenbase.util.Util;

import com.google.common.collect.*;

import java.util.*;

/**
 * Implementation of {@link net.hydromatic.optiq.prepare.Prepare.CatalogReader}
 * and also {@link org.eigenbase.sql.SqlOperatorTable} based on tables and
 * functions defined schemas.
 */
public class OptiqCatalogReader implements Prepare.CatalogReader,
    SqlOperatorTable {
  final OptiqSchema rootSchema;
  final JavaTypeFactory typeFactory;
  private final List<String> defaultSchema;
  private final boolean caseSensitive;

  public OptiqCatalogReader(
      OptiqSchema rootSchema,
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

  public OptiqCatalogReader withSchemaPath(List<String> schemaPath) {
    return new OptiqCatalogReader(rootSchema, caseSensitive, schemaPath,
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
    OptiqSchema schema =
        getSchema(Iterables.concat(schemaNames, Util.skipLast(names)));
    if (schema == null) {
      return null;
    }
    final String name = Util.last(names);
    Pair<String, Table> pair = schema.getTable(name, caseSensitive);
    if (pair == null) {
      pair = schema.getTableBasedOnNullaryFunction(name, caseSensitive);
    }
    if (pair != null) {
      final Table table = pair.getValue();
      final String name2 = pair.getKey();
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
      OptiqSchema schema = getSchema(defaultSchema);
      if (schema == null) {
        schemaNameList = ImmutableList.of();
      } else {
        schemaNameList = schema.getPath();
      }
    }
    for (List<String> schemaNames : schemaNameList) {
      OptiqSchema schema =
          getSchema(Iterables.concat(schemaNames, Util.skipLast(names)));
      if (schema != null) {
        final String name = Util.last(names);
        functions2.addAll(schema.getFunctions(name, true));
      }
    }
    return functions2;
  }

  private OptiqSchema getSchema(Iterable<String> schemaNames) {
    OptiqSchema schema = rootSchema;
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
    final OptiqSchema schema = getSchema(names);
    if (schema == null) {
      return ImmutableList.of();
    }
    final List<SqlMoniker> result = new ArrayList<SqlMoniker>();
    final Map<String, OptiqSchema> schemaMap = schema.getSubSchemaMap();

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
    return SqlValidatorUtil.lookupField(caseSensitive, rowType, alias);
  }

  public int fieldOrdinal(RelDataType rowType, String alias) {
    RelDataTypeField field = field(rowType, alias);
    return field != null ? field.getIndex() : -1;
  }

  public int match(List<String> strings, String name) {
    return Util.match2(strings, name, caseSensitive);
  }

  public RelDataType createTypeFromProjection(final RelDataType type,
      final List<String> columnNameList) {
    return SqlValidatorUtil.createTypeFromProjection(type, columnNameList,
        typeFactory, caseSensitive);
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
              return typeFactory.createSqlType(SqlTypeName.ANY);
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

// End OptiqCatalogReader.java
