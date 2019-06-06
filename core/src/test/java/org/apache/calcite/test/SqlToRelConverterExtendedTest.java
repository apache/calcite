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

import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.externalize.RelJsonReader;
import org.apache.calcite.rel.externalize.RelJsonWriter;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.util.TestUtil;

import org.junit.After;
import org.junit.Before;

import java.io.IOException;

/**
 * Runs {@link org.apache.calcite.test.SqlToRelConverterTest} with extensions.
 */
public class SqlToRelConverterExtendedTest extends SqlToRelConverterTest {
  Hook.Closeable closeable;

  @Before public void before() {
    this.closeable =
        Hook.CONVERTED.addThread(SqlToRelConverterExtendedTest::foo);
  }

  @After public void after() {
    if (this.closeable != null) {
      this.closeable.close();
      this.closeable = null;
    }
  }

  public static void foo(RelNode rel) {
    // Convert rel tree to JSON.
    final RelJsonWriter writer = new RelJsonWriter();
    rel.explain(writer);
    final String json = writer.asString();

    // Find the schema. If there are no tables in the plan, we won't need one.
    final RelOptSchema[] schemas = {null};
    rel.accept(new RelShuttleImpl() {
      @Override public RelNode visit(TableScan scan) {
        schemas[0] = scan.getTable().getRelOptSchema();
        return super.visit(scan);
      }
    });

    // Convert JSON back to rel tree.
    Frameworks.withPlanner((cluster, relOptSchema, rootSchema) -> {
      final RelJsonReader reader = new RelJsonReader(
          cluster,
          schemas[0], rootSchema);
      try {
        RelNode x = reader.read(json);
      } catch (IOException e) {
        throw TestUtil.rethrow(e);
      }
      return null;
    });
  }
}

// End SqlToRelConverterExtendedTest.java
