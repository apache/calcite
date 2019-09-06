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
package org.apache.calcite.jdbc;

import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidatorImpl;

import com.google.common.collect.ImmutableList;

import java.util.Properties;

/**
 * A SqlValidator with schema and type factory of the given
 * {@link org.apache.calcite.jdbc.CalcitePrepare.Context}.
 *
 * <p>This class is only used to derive data type for DDL sql node.
 * Usually we deduce query sql node data type(i.e. the {@code SqlSelect})
 * during the validation phrase. DDL nodes don't have validation,
 * they can be executed directly through
 * {@link org.apache.calcite.sql.SqlExecutableStatement#execute(CalcitePrepare.Context)}.
 * During the execution, {@link org.apache.calcite.sql.SqlDataTypeSpec} uses
 * this validator to derive its type.
 */
public class ContextSqlValidator extends SqlValidatorImpl {

  /**
   * Create a {@code ContextSqlValidator}.
   * @param context Prepare context.
   * @param mutable Whether to get the mutable schema.
   */
  public ContextSqlValidator(CalcitePrepare.Context context, boolean mutable) {
    super(SqlStdOperatorTable.instance(), getCatalogReader(context, mutable),
        context.getTypeFactory(), SqlConformanceEnum.DEFAULT);
  }

  private static CalciteCatalogReader getCatalogReader(
      CalcitePrepare.Context context, boolean mutable) {
    return new CalciteCatalogReader(
        mutable ? context.getMutableRootSchema() : context.getRootSchema(),
        ImmutableList.of(),
        context.getTypeFactory(),
        new CalciteConnectionConfigImpl(new Properties()));
  }
}

// End ContextSqlValidator.java
