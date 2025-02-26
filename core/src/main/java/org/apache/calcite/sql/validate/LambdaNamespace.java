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
import org.apache.calcite.sql.SqlLambda;
import org.apache.calcite.sql.SqlNode;

import org.checkerframework.checker.nullness.qual.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * Namespace for {@code lambda expression}.
 */
public class LambdaNamespace extends AbstractNamespace {
  private final SqlLambda lambdaExpression;

  /**
   * Creates a LambdaNamespace.
   */
  LambdaNamespace(SqlValidatorImpl validator, SqlLambda lambdaExpression,
      SqlNode enclosingNode) {
    super(validator, enclosingNode);
    this.lambdaExpression = lambdaExpression;
  }

  @Override protected RelDataType validateImpl(RelDataType targetRowType) {
    validator.validateLambda(lambdaExpression);
    requireNonNull(rowType, "rowType");
    return rowType;
  }

  @Override public @Nullable SqlNode getNode() {
    return lambdaExpression;
  }
}
