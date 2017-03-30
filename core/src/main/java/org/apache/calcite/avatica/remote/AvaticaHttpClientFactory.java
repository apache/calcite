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
package org.apache.calcite.avatica.remote;

import org.apache.calcite.avatica.ConnectionConfig;

import java.net.URL;

/**
 * A factory for constructing {@link AvaticaHttpClient}'s.
 */
public interface AvaticaHttpClientFactory {

  /**
   * Construct the appropriate implementation of {@link AvaticaHttpClient}.
   *
   * @param url URL that the client is for.
   * @param config Configuration to use when constructing the implementation.
   * @return An instance of {@link AvaticaHttpClient}.
   */
  AvaticaHttpClient getClient(URL url, ConnectionConfig config, KerberosConnection kerberosUtil);

}

// End AvaticaHttpClientFactory.java
