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
package org.apache.calcite.test;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.sql.Types;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/** Tests JDBC metadata type mapping in {@link JdbcSchema}. */
class JdbcSchemaSqlTypeTest {

  @Test void jdbcSchemaSqlTypeMapsNumericZeroPrecision() throws Exception {
    final RelDataTypeFactory factory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final Method sqlType =
        JdbcSchema.class.getDeclaredMethod(
            "sqlType",
            RelDataTypeFactory.class,
            int.class,
            int.class,
            int.class,
            String.class);
    sqlType.setAccessible(true);
    final RelDataType t =
        (RelDataType) sqlType.invoke(null, factory, Types.NUMERIC, 0, 6, null);
    assertThat(t.getSqlTypeName(), is(SqlTypeName.DECIMAL));
    assertThat(t.getPrecision(), is(19));
    assertThat(t.getScale(), is(6));
  }

  @Test void jdbcSchemaSqlTypeMapsDecimalZeroPrecision() throws Exception {
    final RelDataTypeFactory factory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final Method sqlType =
        JdbcSchema.class.getDeclaredMethod(
            "sqlType",
            RelDataTypeFactory.class,
            int.class,
            int.class,
            int.class,
            String.class);
    sqlType.setAccessible(true);
    final RelDataType t =
        (RelDataType) sqlType.invoke(null, factory, Types.DECIMAL, 0, 2, null);
    assertThat(t.getSqlTypeName(), is(SqlTypeName.DECIMAL));
    assertThat(t.getPrecision(), is(19));
    assertThat(t.getScale(), is(2));
  }
}
