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
package org.apache.calcite.test.catalog;

import org.apache.calcite.rel.type.RelDataTypeFactory;

import com.google.common.collect.ImmutableList;

/**
 * Simple catalog reader for testing.
 */
public class MockCatalogReaderSimpleNamedParam extends MockCatalogReaderSimple {
  /**
   * Creates a MockCatalogReaderSimpleNamedParam with a named Parameter.
   *
   * <p>Caller must then call {@link #init} to populate with data.</p>
   *
   * @param typeFactory   Type factory
   * @param caseSensitive case sensitivity
   */
  public MockCatalogReaderSimpleNamedParam(RelDataTypeFactory typeFactory,
      boolean caseSensitive) {
    super(typeFactory, caseSensitive);
  }

  @Override public MockCatalogReader init() {
    super.init();
    final Fixture fixture = new Fixture(typeFactory);

    // Register namedParamTable. Hardcoded for now to avoid completely refactoring the testing suite
    final MockTable namedParamTable =
        MockTable.create(this, ImmutableList.of(DEFAULT_CATALOG, "BodoNamedParams"), false, 1);
    namedParamTable.addColumn("a", fixture.intType);
    namedParamTable.addColumn("b", fixture.intType);
    registerTable(namedParamTable);
    return this;

  }
}
