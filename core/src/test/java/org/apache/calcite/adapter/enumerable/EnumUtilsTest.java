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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests for EnumUtilsTest.
 */
public class EnumUtilsTest {
  @Test
  public void testEnumUtilsFromInternalWithoutVarargsType() {
    Class<?>[] targetTypes = new Class<?>[2];
    targetTypes[0] = String.class;
    targetTypes[1] = String.class;

    List<Expression> expressions = new ArrayList<>();
    Expression e1 = Expressions.constant("test1");
    Expression e2 = Expressions.constant("test2");

    expressions.add(e1);
    expressions.add(e2);

    Assert.assertEquals(expressions, EnumUtils.fromInternal(targetTypes, expressions));
  }

  @Test
  public void testEnumUtilsFromInternalWithVarargsType() {
    Class<?>[] targetTypes = new Class<?>[1];
    targetTypes[0] = String[].class;

    List<Expression> expressions = new ArrayList<>();
    Expression e1 = Expressions.constant("test1");
    Expression e2 = Expressions.constant("test2");
    Expression e3 = Expressions.constant("test3");

    expressions.add(e1);
    expressions.add(e2);
    expressions.add(e3);

    Assert.assertEquals(expressions, EnumUtils.fromInternal(targetTypes, expressions));
  }

  @Test
  public void testEnumUtilsFromInternalWithBothVarargsTypeAndNormalType() {
    Class<?>[] targetTypes = new Class<?>[2];
    targetTypes[0] = Long.class;
    targetTypes[1] = String[].class;

    List<Expression> expressions = new ArrayList<>();
    Expression e1 = Expressions.constant(1L, Long.class);
    Expression e2 = Expressions.constant("test2");
    Expression e3 = Expressions.constant("test3");

    expressions.add(e1);
    expressions.add(e2);
    expressions.add(e3);

    Assert.assertEquals(expressions, EnumUtils.fromInternal(targetTypes, expressions));
  }

  @Test
  public void testEnumUtilsFromInternalWithBothVarargsTypeAndArrayType() {
    Class<?>[] targetTypes = new Class<?>[2];
    targetTypes[0] = String[].class;
    targetTypes[1] = String[].class;

    List<Expression> expressions = new ArrayList<>();
    Expression e1 = Expressions.constant(new String[]{"1", "2", "L"}, String[].class);
    Expression e2 = Expressions.constant("test2");
    Expression e3 = Expressions.constant("test3");

    expressions.add(e1);
    expressions.add(e2);
    expressions.add(e3);

    Assert.assertEquals(expressions, EnumUtils.fromInternal(targetTypes, expressions));
  }

  @Test
  public void testEnumUtilsFromInternalWithBothVarargsTypeAndArrayTypeTwo() {
    Class<?>[] targetTypes = new Class<?>[3];
    targetTypes[0] = int[].class;
    targetTypes[1] = String.class;
    targetTypes[2] = String[].class;

    List<Expression> expressions = new ArrayList<>();
    Expression e1 = Expressions.constant(new int[]{1, 2, 3}, int[].class);
    Expression e2 = Expressions.constant("test1");
    Expression e3 = Expressions.constant("test2");
    Expression e4 = Expressions.constant("test3");

    expressions.add(e1);
    expressions.add(e2);
    expressions.add(e3);
    expressions.add(e4);

    Assert.assertEquals(expressions, EnumUtils.fromInternal(targetTypes, expressions));
  }
}

// End EnumUtilsTest.java
