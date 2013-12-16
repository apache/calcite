/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.optiq.test;

import net.hydromatic.linq4j.function.Function1;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.runtime.Hook;
import net.hydromatic.optiq.tools.Frameworks;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptSchema;
import org.eigenbase.test.SqlToRelConverterTest;

import org.junit.After;
import org.junit.Before;

import java.io.IOException;

/**
 * Runs {@link org.eigenbase.test.SqlToRelConverterTest} with extensions.
 */
public class SqlToRelConverterExtendedTest extends SqlToRelConverterTest {
  Hook.Closeable closeable;

  @Before public void before() {
    //noinspection unchecked
    this.closeable = Hook.CONVERTED.add(
        (Function1) new Function1<RelNode, Void>() {
          public Void apply(RelNode a0) {
            foo(a0);
            return null;
          }
        });
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
      @Override
      public RelNode visit(TableAccessRelBase scan) {
        schemas[0] = scan.getTable().getRelOptSchema();
        return super.visit(scan);
      }
    });

    // Convert JSON back to rel tree.
    Frameworks.withPlanner(
        new Frameworks.PlannerAction<Object>() {
          public Object apply(RelOptCluster cluster,
              RelOptSchema relOptSchema, Schema schema) {
            final RelJsonReader reader = new RelJsonReader(
                cluster,
                schemas[0],
                schema);
            try {
              RelNode x = reader.read(json);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
            return null;
          }
        });
  }
}

// End SqlToRelConverterExtendedTest.java
