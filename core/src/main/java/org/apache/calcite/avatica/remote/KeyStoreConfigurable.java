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
 * Allows a keystore (and keystorepassword, keypassword) to be
 * provided to enable MTLS authentication
 */
public interface KeyStoreConfigurable {

    /**
     * Sets a keystore containing the collection of client side certificates
     * to use for HTTPS mutual authentication along with
     * password for keystore and password for key
     *
     * @param keystore The keystore on the local filesystem
     * @param keystorepassword The keystore's password
     * @param keypassword The key's password
     */
  void setKeyStore(File keystore, String keystorepassword, String keypassword);
}

// End KeyStoreConfigurable.java
