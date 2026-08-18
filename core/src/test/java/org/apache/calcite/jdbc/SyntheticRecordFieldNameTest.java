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

import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Type;
import java.util.List;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

/**
 * Tests for the Java field names of the synthetic classes built by
 * {@link JavaTypeFactoryImpl} from a {@code RelRecordType}.
 *
 * <p>SQL quoted identifiers admit characters that are not legal in a
 * Java identifier. When a SQL field name is a valid Java identifier it
 * is reused as the field name of the synthetic class; otherwise the
 * factory falls back to a positional name ({@code f0}, {@code f1}, ...).
 * The original SQL names are preserved on the {@link RelDataType}
 * regardless.
 */
public class SyntheticRecordFieldNameTest {

  @Test void testValidSqlNamesReusedAsJavaNames() {
    final JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
    final RelDataType rowType = typeFactory.builder()
        .add("empid", SqlTypeName.INTEGER)
        .add("name", SqlTypeName.VARCHAR)
        .add("deptno", SqlTypeName.INTEGER)
        .build();
    final Type javaType = typeFactory.getJavaClass(rowType);
    assertThat(javaType, instanceOf(Types.RecordType.class));
    final List<Types.RecordField> fields =
        ((Types.RecordType) javaType).getRecordFields();
    assertThat(fields, hasSize(3));
    assertThat(fields.get(0).getName(), is("empid"));
    assertThat(fields.get(1).getName(), is("name"));
    assertThat(fields.get(2).getName(), is("deptno"));
  }

  @Test void testNonIdentifierSqlNamesFallBackToPositional() {
    final JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
    final RelDataType rowType = typeFactory.builder()
        .add("has space", SqlTypeName.INTEGER)
        .add("a.b", SqlTypeName.VARCHAR)
        .add("ok", SqlTypeName.INTEGER)
        .build();
    final Type javaType = typeFactory.getJavaClass(rowType);
    assertThat(javaType, instanceOf(Types.RecordType.class));
    final List<Types.RecordField> fields =
        ((Types.RecordType) javaType).getRecordFields();
    assertThat(fields, hasSize(3));
    // The first two SQL names are not legal Java identifiers, so they
    // are replaced by positional names; the third one is fine and is
    // reused verbatim
    assertThat(fields.get(0).getName(), is("f0"));
    assertThat(fields.get(1).getName(), is("f1"));
    assertThat(fields.get(2).getName(), is("ok"));
    for (Types.RecordField f : fields) {
      assertThat("field names must be valid Java identifiers",
          Types.isValidJavaIdentifier(f.getName()), is(true));
    }
    // The SQL names survive on the relational rowtype, which is where
    // column labels come from
    final JavaTypeFactoryImpl.SyntheticRecordType syntheticType =
        (JavaTypeFactoryImpl.SyntheticRecordType) javaType;
    assertThat(syntheticType.relType, is(rowType));
    assertThat(rowType.getFieldNames().get(0), is("has space"));
    assertThat(rowType.getFieldNames().get(1), is("a.b"));
    assertThat(rowType.getFieldNames().get(2), is("ok"));
  }

  @Test void testListOverloadStillUsesPositionalNames() {
    // The createSyntheticType(List<Type> l) overload produces f0..fn
    // (the caller supplies no names)
    final JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
    final Type fromTypes =
        typeFactory.createSyntheticType(ImmutableList.of(Integer.class, String.class));
    final List<Types.RecordField> fields =
        ((Types.RecordType) fromTypes).getRecordFields();
    assertThat(fields, hasSize(2));
    assertThat(fields.get(0).getName(), is("f0"));
    assertThat(fields.get(1).getName(), is("f1"));
  }
}
