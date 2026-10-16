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
package org.apache.calcite.adapter.mongodb;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Schema;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests {@link MongoSchemaFactory}. */
class MongoSchemaFactoryTest {
  /** Creates a schema from an operand that names the authentication mechanism
   * and a username, and gives no password. */
  private static Schema createWithoutPassword(String authMechanism) {
    final Map<String, Object> operand = new HashMap<>();
    operand.put("host", "mongodb://localhost");
    operand.put("database", "test");
    operand.put("authMechanism", authMechanism);
    operand.put("username", "calcite");
    return new MongoSchemaFactory().create(
        CalciteSchema.createRootSchema(false, false).plus(), "mongo", operand);
  }

  /** X.509 and Kerberos authentication take no password, so the operand may
   * leave it out. */
  @ParameterizedTest(name = "{0}")
  @ValueSource(strings = {"MONGODB-X509", "GSSAPI"})
  void aMechanismWithoutPasswordAcceptsAnOperandWithoutOne(String authMechanism) {
    assertThat(createWithoutPassword(authMechanism), instanceOf(MongoSchema.class));
  }

  @ParameterizedTest(name = "{0}")
  @ValueSource(strings = {"PLAIN", "SCRAM-SHA-1", "SCRAM-SHA-256"})
  void aPasswordMechanismRefusesAnOperandWithoutOne(String authMechanism) {
    NullPointerException e =
        assertThrows(NullPointerException.class, () ->
            createWithoutPassword(authMechanism));
    assertThat(e.getMessage(), is("password"));
  }
}
