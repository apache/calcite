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

import java.util.Objects;

import javax.servlet.http.HttpServletRequest;

/**
 * A {@link RemoteUserExtractor} that extracts the remote user from an HTTP query string parameter.
 */
public class HttpQueryStringParameterRemoteUserExtractor implements RemoteUserExtractor {
  private final String parameter;

  public HttpQueryStringParameterRemoteUserExtractor(String parameter) {
    this.parameter = Objects.requireNonNull(parameter);
  }

  @Override public String extract(HttpServletRequest request) throws RemoteUserExtractionException {
    final String remoteUser = request.getParameter(parameter);
    if (remoteUser == null) {
      throw new RemoteUserExtractionException(
          "Failed to extract user from HTTP query string parameter: " + parameter);
    }
    return remoteUser;
  }

}

// End HttpQueryStringParameterRemoteUserExtractor.java
