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
package org.apache.calcite.util;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Unit test for {@link Permutation}.
 */
class PermutationTestCase {
  @Test void testOne() {
    final Permutation perm = new Permutation(4);
    assertThat(perm, hasToString("[0, 1, 2, 3]"));
    assertThat(perm.size(), hasToString("4"));

    perm.set(0, 2);
    assertThat(perm, hasToString("[2, 1, 0, 3]"));

    perm.set(1, 0);
    assertThat(perm, hasToString("[2, 0, 1, 3]"));

    final Permutation invPerm = perm.inverse();
    assertThat(invPerm, hasToString("[1, 2, 0, 3]"));

    // changing perm doesn't change inverse
    perm.set(0, 0);
    assertThat(perm, hasToString("[0, 2, 1, 3]"));
    assertThat(invPerm, hasToString("[1, 2, 0, 3]"));
  }

  @Test void testTwo() {
    final Permutation perm = new Permutation(new int[]{3, 2, 0, 1});
    assertFalse(perm.isIdentity());
    assertThat(perm, hasToString("[3, 2, 0, 1]"));

    Permutation perm2 = (Permutation) perm.clone();
    assertThat(perm2, hasToString("[3, 2, 0, 1]"));
    assertThat(perm, is(perm2));
    assertThat(perm2, is(perm));

    perm.set(2, 1);
    assertThat(perm, hasToString("[3, 2, 1, 0]"));
    assertThat(perm, not(equalTo(perm2)));

    // clone not affected
    assertThat(perm2, hasToString("[3, 2, 0, 1]"));

    perm2.set(2, 3);
    assertThat(perm2, hasToString("[0, 2, 3, 1]"));
  }

  @Test void testInsert() {
    Permutation perm = new Permutation(new int[]{3, 0, 4, 2, 1});
    perm.insertTarget(2);
    assertThat(perm, hasToString("[4, 0, 5, 3, 1, 2]"));

    // insert at start
    perm = new Permutation(new int[]{3, 0, 4, 2, 1});
    perm.insertTarget(0);
    assertThat(perm, hasToString("[4, 1, 5, 3, 2, 0]"));

    // insert at end
    perm = new Permutation(new int[]{3, 0, 4, 2, 1});
    perm.insertTarget(5);
    assertThat(perm, hasToString("[3, 0, 4, 2, 1, 5]"));

    // insert into empty
    perm = new Permutation(new int[]{});
    perm.insertTarget(0);
    assertThat(perm, hasToString("[0]"));
  }

  @Test void testEmpty() {
    final Permutation perm = new Permutation(0);
    assertTrue(perm.isIdentity());
    assertThat(perm, hasToString("[]"));
    assertThat(perm, is(perm));
    assertThat(perm, is(perm.inverse()));

    try {
      perm.set(1, 0);
      fail("expected exception");
    } catch (ArrayIndexOutOfBoundsException e) {
      // success
    }

    try {
      perm.set(-1, 2);
      fail("expected exception");
    } catch (ArrayIndexOutOfBoundsException e) {
      // success
    }
  }

  @Test void testProjectPermutation() {
    final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    final RexBuilder builder = new RexBuilder(typeFactory);
    final RelDataType doubleType =
        typeFactory.createSqlType(SqlTypeName.DOUBLE);

    // A project with [1, 1] is not a permutation, so should return null
    final Permutation perm =
        Project.getPermutation(2,
            ImmutableList.of(builder.makeInputRef(doubleType, 1),
                builder.makeInputRef(doubleType, 1)));
    assertThat(perm, nullValue());

    // A project with [0, 1, 0] is not a permutation, so should return null
    final Permutation perm1 =
        Project.getPermutation(2,
            ImmutableList.of(builder.makeInputRef(doubleType, 0),
                builder.makeInputRef(doubleType, 1),
                builder.makeInputRef(doubleType, 0)));
    assertThat(perm1, nullValue());

    // A project of [1, 0] is a valid permutation!
    final Permutation perm2 =
        Project.getPermutation(2,
            ImmutableList.of(builder.makeInputRef(doubleType, 1),
                builder.makeInputRef(doubleType, 0)));
    assertThat(perm2, is(new Permutation(new int[]{1, 0})));
  }
}
