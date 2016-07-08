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
package org.apache.calcite.rex;

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Test for {@link RexBuilder}
 */
public class RexBuilderTest {

  /**
   * Test RexBuilder.ensureType()
   */
  @Test
  public void testEnsureTypeWithAny() {
    final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RexBuilder builder = new RexBuilder(typeFactory);

    RexNode node =  new RexLiteral(
            Boolean.TRUE, typeFactory.createSqlType(SqlTypeName.BOOLEAN), SqlTypeName.BOOLEAN);
    RexNode ensuredNode = builder.ensureType(
            typeFactory.createSqlType(SqlTypeName.ANY), node, true);

    assertEquals(node, ensuredNode);
  }

  /**
   * Test RexBuilder.ensureType()
   */
  @Test
  public void testEnsureTypeWithItself() {
    final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RexBuilder builder = new RexBuilder(typeFactory);

    RexNode node =  new RexLiteral(
            Boolean.TRUE, typeFactory.createSqlType(SqlTypeName.BOOLEAN), SqlTypeName.BOOLEAN);
    RexNode ensuredNode = builder.ensureType(
            typeFactory.createSqlType(SqlTypeName.BOOLEAN), node, true);

    assertEquals(node, ensuredNode);
  }

  /**
   * Test RexBuilder.ensureType()
   */
  @Test
  public void testEnsureTypeWithDifference() {
    final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RexBuilder builder = new RexBuilder(typeFactory);

    RexNode node =  new RexLiteral(
            Boolean.TRUE, typeFactory.createSqlType(SqlTypeName.BOOLEAN), SqlTypeName.BOOLEAN);
    RexNode ensuredNode = builder.ensureType(
            typeFactory.createSqlType(SqlTypeName.INTEGER), node, true);

    assertNotEquals(node, ensuredNode);
    assertEquals(ensuredNode.getType(), typeFactory.createSqlType(SqlTypeName.INTEGER));
  }

}

// End RexBuilderTest.java
