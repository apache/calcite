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

import org.eclipse.jetty.security.SpnegoLoginService;

import java.lang.reflect.Field;
import java.util.Objects;

/**
 * A customization of {@link SpnegoLoginService} which directly specifies the server's
 * principal instead of requiring a file to exist. Known to work with Jetty-9.2.x, any other
 * version would require testing/inspection to ensure the logic is still sound.
 */
public class PropertyBasedSpnegoLoginService extends SpnegoLoginService {

  private static final String TARGET_NAME_FIELD_NAME = "_targetName";
  private final String serverPrincipal;

  public PropertyBasedSpnegoLoginService(String realm, String serverPrincipal) {
    super(realm);
    this.serverPrincipal = Objects.requireNonNull(serverPrincipal);
  }

  @Override protected void doStart() throws Exception {
    // Override the parent implementation, setting _targetName to be the serverPrincipal
    // without the need for a one-line file to do the same thing.
    //
    // AbstractLifeCycle's doStart() method does nothing, so we aren't missing any extra logic.
    final Field targetNameField = SpnegoLoginService.class.getDeclaredField(TARGET_NAME_FIELD_NAME);
    targetNameField.setAccessible(true);
    targetNameField.set(this, serverPrincipal);
  }
}

// End PropertyBasedSpnegoLoginService.java
