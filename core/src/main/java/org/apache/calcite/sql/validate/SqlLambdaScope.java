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
package org.apache.calcite.sql.validate;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLambda;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Litmus;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Scope for a {@link SqlLambda LAMBDA EXPRESSION}.
 */
public class SqlLambdaScope extends ListScope {
  private final SqlLambda lambdaExpr;
  private final Map<String, RelDataType> parameterTypes;

  public SqlLambdaScope(
      SqlValidatorScope parent, SqlLambda lambdaExpr) {
    super(parent);
    this.lambdaExpr = lambdaExpr;

    // default parameter type is ANY
    final RelDataType any =
        validator.typeFactory.createTypeWithNullability(
            validator.typeFactory.createSqlType(SqlTypeName.ANY), true);
    parameterTypes = new HashMap<>();
    lambdaExpr.getParameters().forEach(param -> parameterTypes.put(param.toString(), any));
  }

  /** True if the identifier matches one of the parameter names. */
  public boolean isParameter(SqlIdentifier id) {
    return this.parameterTypes.containsKey(id.toString());
  }

  @Override public SqlNode getNode() {
    return lambdaExpr;
  }

  @Override public SqlQualified fullyQualify(SqlIdentifier identifier) {
    boolean found = lambdaExpr.getParameters()
        .stream()
        .anyMatch(param -> param.equalsDeep(identifier, Litmus.IGNORE));
    if (found) {
      return SqlQualified.create(this, 1, null, identifier);
    } else {
      throw validator.newValidationError(identifier,
          RESOURCE.paramNotFoundInLambdaExpression(identifier.toString(), lambdaExpr.toString()));
    }
  }

  @Override public @Nullable RelDataType resolveColumn(String columnName, SqlNode ctx) {
    checkArgument(parameterTypes.containsKey(columnName),
        "column %s not found", columnName);
    return parameterTypes.get(columnName);
  }

  public Map<String, RelDataType> getParameterTypes() {
    return parameterTypes;
  }
}
