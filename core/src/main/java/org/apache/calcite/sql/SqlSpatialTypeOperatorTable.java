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
package org.apache.calcite.sql;

import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.AccumOperation;
import org.apache.calcite.runtime.CollectOperation;
import org.apache.calcite.runtime.GeographyValue;
import org.apache.calcite.runtime.SpatialTypeFunctions;
import org.apache.calcite.runtime.UnionOperation;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AggregateFunctionImpl;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.fun.SqlSpatialTypeFunctions;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;

import com.google.common.collect.ImmutableList;

import org.jspecify.annotations.Nullable;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Implementation of {@link org.apache.calcite.sql.SqlSpatialTypeOperatorTable} containing
 * the spatial operators and functions.
 */
public class SqlSpatialTypeOperatorTable implements SqlOperatorTable {

  private static final SqlOperator GEOGRAPHY_IS_EMPTY =
      geographyAccessor("ST_ISEMPTY", "isEmpty", SqlTypeName.BOOLEAN);
  private static final SqlOperator GEOGRAPHY_GEOMETRY_TYPE =
      geographyAccessor("ST_GEOMETRYTYPE", "geometryType", SqlTypeName.VARCHAR);

  private final SqlOperatorTable operatorTable;

  public SqlSpatialTypeOperatorTable() {
    // Create a root schema to hold the spatial functions.
    CalciteSchema rootSchema = CalciteSchema.createRootSchema(false, false);
    SchemaPlus schema = rootSchema.plus();

    // Register the spatial functions.
    ModelHandler.addFunctions(schema, null, ImmutableList.of(),
        SpatialTypeFunctions.class.getName(), "*", true);

    // Register the sql spatial functions.
    ModelHandler.addFunctions(schema, null, ImmutableList.of(),
        SqlSpatialTypeFunctions.class.getName(), "*", true);

    // Register the spatial aggregate functions.
    schema.add("ST_UNION",
        requireNonNull(AggregateFunctionImpl.create(UnionOperation.class)));
    schema.add("ST_ACCUM",
        requireNonNull(AggregateFunctionImpl.create(AccumOperation.class)));
    schema.add("ST_COLLECT",
        requireNonNull(AggregateFunctionImpl.create(CollectOperation.class)));

    // Create a catalog reader to retrieve the operators.
    CalciteCatalogReader catalogReader =
        new CalciteCatalogReader(rootSchema,
            ImmutableList.of(),
            new JavaTypeFactoryImpl(),
            CalciteConnectionConfigImpl.DEFAULT);

    operatorTable = SqlOperatorTables.of(ImmutableList.<SqlOperator>builder()
        .addAll(catalogReader.getOperatorList())
        .add(GEOGRAPHY_IS_EMPTY, GEOGRAPHY_GEOMETRY_TYPE)
        .build());
  }

  private static SqlOperator geographyAccessor(String name, String method,
      SqlTypeName resultType) {
    final RelDataType geography = new JavaTypeFactoryImpl().createSqlType(SqlTypeName.GEOGRAPHY);
    return new SqlUserDefinedFunction(new SqlIdentifier(name, SqlParserPos.ZERO),
        SqlKind.OTHER_FUNCTION, ReturnTypes.explicit(resultType)
            .andThen(SqlTypeTransforms.TO_NULLABLE),
        InferTypes.explicit(ImmutableList.of(geography)),
        OperandTypes.operandMetadata(ImmutableList.of(SqlTypeFamily.GEOGRAPHY),
            typeFactory -> ImmutableList.of(typeFactory.createSqlType(SqlTypeName.GEOGRAPHY)),
            i -> "geography", i -> false),
        ScalarFunctionImpl.create(
            Types.lookupMethod(GeographyValue.class, method, GeographyValue.class)));
  }

  /** Whether this is one of the built-in local geography accessors. */
  public static boolean isGeographyAccessor(SqlOperator operator) {
    return operator == GEOGRAPHY_IS_EMPTY || operator == GEOGRAPHY_GEOMETRY_TYPE;
  }

  @Override public void lookupOperatorOverloads(SqlIdentifier opName,
      @Nullable SqlFunctionCategory category, SqlSyntax syntax,
      List<SqlOperator> operatorList, SqlNameMatcher nameMatcher) {
    operatorTable.lookupOperatorOverloads(opName, category, syntax, operatorList, nameMatcher);
  }

  @Override public List<SqlOperator> getOperatorList() {
    return operatorTable.getOperatorList();
  }
}
