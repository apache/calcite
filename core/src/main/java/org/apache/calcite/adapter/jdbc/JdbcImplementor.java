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
package org.apache.calcite.adapter.jdbc;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.lang.reflect.Type;
import java.util.List;

/**
 * State for generating a SQL statement.
 */
public class JdbcImplementor extends RelToSqlConverter {

  private final JdbcCorrelationDataContextBuilder dataContextBuilder;
  private final JavaTypeFactory typeFactory;

  JdbcImplementor(SqlDialect dialect, JavaTypeFactory typeFactory,
      JdbcCorrelationDataContextBuilder dataContextBuilder) {
    super(dialect);
    this.typeFactory = typeFactory;
    this.dataContextBuilder = dataContextBuilder;
  }

  public JdbcImplementor(SqlDialect dialect, JavaTypeFactory typeFactory) {
    this(dialect, typeFactory, new JdbcCorrelationDataContextBuilder() {
      private int counter = 1;
      @Override public int add(CorrelationId id, int ordinal, Type type) {
        return counter++;
      }
    });
  }

  public Result implement(RelNode node) {
    return dispatch(node);
  }

  @Override protected Context getAliasContext(RexCorrelVariable variable) {
    Context context = correlTableMap.get(variable.id);
    if (context != null) {
      return context;
    }
    List<RelDataTypeField>  fieldList = variable.getType().getFieldList();
    // We need to provide a context which also includes the correlation variables
    // as dynamic parameters.
    return new Context(dialect, fieldList.size()) {
      @Override public SqlNode field(int ordinal) {
        RelDataTypeField field = fieldList.get(ordinal);
        return new SqlDynamicParam(
            dataContextBuilder.add(variable.id, ordinal,
            typeFactory.getJavaClass(field.getType())), SqlParserPos.ZERO);
      }

      @Override public SqlImplementor implementor() {
        return JdbcImplementor.this;
      }
    };
  }
}
