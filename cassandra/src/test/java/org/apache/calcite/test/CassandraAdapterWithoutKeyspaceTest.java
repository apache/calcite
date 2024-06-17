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

import com.datastax.oss.driver.api.core.CqlSession;
import com.google.common.collect.ImmutableMap;

import org.cassandraunit.CQLDataLoader;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

/**
 * Tests for the {@code org.apache.calcite.adapter.cassandra} package.
 *
 * <p>Instantiates a CQL session without keyspace, but passes it to
 * {@code org.apache.calcite.adapter.cassandra.CassandraTable}.
 * All generated CQL queries should still succeed and explicitly
 * reference the keyspace.
 */
@Execution(ExecutionMode.SAME_THREAD)
@ExtendWith(CassandraExtension.class)
class CassandraAdapterWithoutKeyspaceTest {
  private static final ImmutableMap<String, String> TWISSANDRA_WITHOUT_KEYSPACE =
          CassandraExtension.getDataset("/model-without-keyspace.json");

  @BeforeAll
  static void load(CqlSession session) {
    new CQLDataLoader(session)
        .load(new ClassPathCQLDataSet("twissandra-small.cql"));
  }

  @Test void testSelect() {
    CalciteAssert.that()
        .with(TWISSANDRA_WITHOUT_KEYSPACE)
        .query("select * from \"users\"")
        .returnsCount(10);
  }
}
