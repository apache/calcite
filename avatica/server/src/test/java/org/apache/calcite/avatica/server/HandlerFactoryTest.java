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
package org.apache.calcite.avatica.server;

import org.apache.calcite.avatica.remote.Driver.Serialization;
import org.apache.calcite.avatica.remote.Service;

import org.eclipse.jetty.server.Handler;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertTrue;

/**
 * Tests the {@link HandlerFactory} implementation.
 */
public class HandlerFactoryTest {

  private HandlerFactory factory;
  private Service service;

  @Before
  public void setup() {
    this.factory = new HandlerFactory();
    this.service = Mockito.mock(Service.class);
  }

  @Test
  public void testJson() {
    Handler handler = factory.getHandler(service, Serialization.JSON);
    assertTrue("Expected an implementation of the AvaticaHandler, "
        + "but got " + handler.getClass(), handler instanceof AvaticaJsonHandler);
  }

  @Test
  public void testProtobuf() {
    Handler handler = factory.getHandler(service, Serialization.PROTOBUF);
    assertTrue("Expected an implementation of the AvaticaProtobufHandler, "
        + "but got " + handler.getClass(), handler instanceof AvaticaProtobufHandler);
  }
}

// End HandlerFactoryTest.java
