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

import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.function.Predicate;

/**
 * Fluent class that contains information necessary to run a test.
 */
public class MaterializedViewFixture {
  public final String query;
  public final MaterializedViewTester tester;
  public final CalciteAssert.@Nullable SchemaSpec schemaSpec;
  public final ImmutableList<Pair<String, String>> materializationList;
  public final @Nullable Predicate<String> checker;

  public static MaterializedViewFixture create(String query,
      MaterializedViewTester tester) {
    return new MaterializedViewFixture(tester, query, null, ImmutableList.of(),
        null);
  }

  private MaterializedViewFixture(MaterializedViewTester tester, String query,
      CalciteAssert.@Nullable SchemaSpec schemaSpec,
      ImmutableList<Pair<String, String>> materializationList,
      @Nullable Predicate<String> checker) {
    this.query = query;
    this.tester = tester;
    this.schemaSpec = schemaSpec;
    this.materializationList = materializationList;
    this.checker = checker;
  }

  public void ok() {
    tester.checkMaterialize(this);
  }

  public void noMat() {
    tester.checkNoMaterialize(this);
  }

  public MaterializedViewFixture withDefaultSchemaSpec(
      CalciteAssert.@Nullable SchemaSpec schemaSpec) {
    if (schemaSpec == this.schemaSpec) {
      return this;
    }
    return new MaterializedViewFixture(tester, query, schemaSpec,
        materializationList, checker);
  }

  public MaterializedViewFixture withMaterializations(
      Iterable<? extends Pair<String, String>> materialize) {
    final ImmutableList<Pair<String, String>> materializationList =
        ImmutableList.copyOf(materialize);
    if (materializationList.equals(this.materializationList)) {
      return this;
    }
    return new MaterializedViewFixture(tester, query, schemaSpec,
        materializationList, checker);
  }

  public MaterializedViewFixture withQuery(String query) {
    if (query.equals(this.query)) {
      return this;
    }
    return new MaterializedViewFixture(tester, query, schemaSpec,
        materializationList, checker);
  }

  public MaterializedViewFixture withChecker(Predicate<String> checker) {
    if (checker == this.checker) {
      return this;
    }
    return new MaterializedViewFixture(tester, query, schemaSpec,
        materializationList, checker);
  }

  public MaterializedViewFixture checkingThatResultContains(
      String... expectedStrings) {
    return withChecker(s -> resultContains(s, expectedStrings));
  }

  /** Returns whether the result contains all the given strings. */
  public static boolean resultContains(String result, final String... expected) {
    String sLinux = Util.toLinux(result);
    for (String st : expected) {
      if (!sLinux.contains(Util.toLinux(st))) {
        return false;
      }
    }
    return true;
  }

}
