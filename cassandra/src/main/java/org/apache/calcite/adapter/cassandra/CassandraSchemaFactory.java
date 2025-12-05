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
package org.apache.calcite.adapter.cassandra;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.util.trace.CalciteTrace;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static java.lang.Integer.parseInt;

/**
 * Factory that creates a {@link CassandraSchema}.
 */
@SuppressWarnings("UnusedDeclaration")
public class CassandraSchemaFactory implements SchemaFactory {

  private static final int DEFAULT_CASSANDRA_PORT = 9042;
  private static final Map<Map<String, Object>, CqlSession> INFO_TO_SESSION =
      new ConcurrentHashMap<>();
  private static final Set<String> SESSION_DEFINING_KEYS =
      ImmutableSet.of("host", "port", "keyspace", "username", "password");
  protected static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  public CassandraSchemaFactory() {
    super();
  }

  @Override public Schema create(SchemaPlus parentSchema, String name,
      Map<String, Object> operand) {

    final Map<String, Object> sessionMap = projectMapOverKeys(operand, SESSION_DEFINING_KEYS);

    INFO_TO_SESSION.computeIfAbsent(sessionMap, m -> {
      String host = (String) m.get("host");
      String username = (String) m.get("username");
      String password = (String) m.get("password");
      int port = getPort(m);

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Creating session for info {}", m);
      }
      try {
        CqlSessionBuilder builder =
            username != null && password != null
                ? CqlSession.builder()
                  .addContactPoint(new InetSocketAddress(host, port))
                  .withAuthCredentials(username, password)
                : CqlSession.builder()
                  .addContactPoint(new InetSocketAddress(host, port));

        if (m.containsKey("keyspace")) {
          String keyspace = (String) m.get("keyspace");
          builder = builder.withKeyspace(keyspace);
        }

        return builder
            .withLocalDatacenter("datacenter1")
            .build();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    CqlSession session = INFO_TO_SESSION.get(sessionMap);

    String keyspace = session.getKeyspace()
        .map(CqlIdentifier::asInternal)
        .orElse(name);

    return new CassandraSchema(session, parentSchema, keyspace, name);
  }

  private static Map<String, Object> projectMapOverKeys(
      Map<String, Object> map, Set<String> keysToKeep) {
    return map.entrySet().stream()
        .filter(e -> keysToKeep.contains(e.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private static int getPort(Map<String, Object> map) {
    if (map.containsKey("port")) {
      Object portObj = map.get("port");
      if (portObj instanceof String) {
        return parseInt((String) portObj);
      } else {
        return (int) portObj;
      }
    } else {
      return DEFAULT_CASSANDRA_PORT;
    }
  }
}
