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

import java.io.File;

/**
 * Allows a truststore (and password) to be provided to enable TLS authentication.
 */
public interface TrustStoreConfigurable {

  /**
   * Sets a truststore containing the collection of trust SSL/TLS server certificates
   * to use for HTTPS calls and the password for that truststore.
   *
   * @param truststore The truststore on the local filesystem
   * @param password The truststore's password
   */
  void setTrustStore(File truststore, String password);
}

// End TrustStoreConfigurable.java
