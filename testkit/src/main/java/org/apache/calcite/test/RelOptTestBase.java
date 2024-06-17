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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RelBuilder;

import java.util.function.Function;

/**
 * RelOptTestBase is an abstract base for tests which exercise a planner and/or
 * rules via {@link DiffRepository}.
 */
abstract class RelOptTestBase {
  //~ Methods ----------------------------------------------------------------

  /** Creates a fixture for a test. Derived class must override and set
   * {@link RelOptFixture#diffRepos}. */
  RelOptFixture fixture() {
    return RelOptFixture.DEFAULT;
  }

  /**
   * Creates a test context with a SQL query.
   * Default catalog: {@link org.apache.calcite.test.catalog.MockCatalogReaderSimple#init()}.
   */
  protected final RelOptFixture sql(String sql) {
    return fixture().sql(sql);
  }

  /** Initiates a test case with a given {@link RelNode} supplier. */
  protected final RelOptFixture relFn(Function<RelBuilder, RelNode> relFn) {
    return fixture().relFn(relFn);
  }

}
