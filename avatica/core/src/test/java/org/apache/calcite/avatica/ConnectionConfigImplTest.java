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
package org.apache.calcite.avatica;

import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Test class for {@link ConnectionConfigImpl}.
 */
public class ConnectionConfigImplTest {

  @Test public void testTrustStore() {
    final String truststore = "/my/truststore.jks";
    final String pw = "supremelysecret";
    Properties props = new Properties();
    props.setProperty(BuiltInConnectionProperty.TRUSTSTORE.name(), truststore);
    props.setProperty(BuiltInConnectionProperty.TRUSTSTORE_PASSWORD.name(), pw);
    ConnectionConfigImpl config = new ConnectionConfigImpl(props);
    assertEquals(truststore, config.truststore().getAbsolutePath());
    assertEquals(pw, config.truststorePassword());
  }

  @Test public void testNoTruststore() {
    Properties props = new Properties();
    ConnectionConfigImpl config = new ConnectionConfigImpl(props);
    assertNull(config.truststore());
    assertNull(config.truststorePassword());
  }
}

// End ConnectionConfigImplTest.java
