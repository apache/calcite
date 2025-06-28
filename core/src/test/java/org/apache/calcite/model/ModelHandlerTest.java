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
package org.apache.calcite.model;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.lookup.LikePattern;
import org.apache.calcite.util.Sources;

import com.google.common.collect.ImmutableSet;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import static java.util.Objects.requireNonNull;

/**
 * Unit test for {@link ModelHandler}.
 */
public class ModelHandlerTest {

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7022">[CALCITE-7022]
   * Decouple ModelHandler from CalciteConnection</a>.
   * The test ensures/demonstrates that a Schema can be easily parsed/created from a model
   * file (JSON/YAML) without necessitating the creation of complex/heavy objects
   * (e.g., CalciteConnection). */
  @Test void testPopulateRootSchemaFromURL() throws IOException {
    SchemaPlus root = CalciteSchema.createRootSchema(false, false).plus();
    String mURI =
        Sources.of(requireNonNull(ModelHandlerTest.class.getResource("/hsqldb-scott.json")))
            .path();
    ModelHandler h = new ModelHandler(root, mURI);
    SchemaPlus scott = root.subSchemas().get("SCOTT");
    Set<String> tables = scott.tables().getNames(new LikePattern("%"));
    assertThat(tables, is(ImmutableSet.of("EMP", "DEPT", "BONUS", "SALGRADE")));
    assertThat(h.defaultSchemaName(), is("SCOTT"));
  }

}
