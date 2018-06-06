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

/**
 * An interface to decorate an {@link AvaticaHttpClient} that can support configuration on
 * SSL hostname verification.
 */
public interface HostnameVerificationConfigurable {
  /**
   * Describes the support hostname verification methods of {@link AvaticaHttpClient}.
   */
  enum HostnameVerification {
    /**
     * The common name (CN) on the certificate must match the server's hostname.
     */
    STRICT,
    /**
     * No verification is performed.
     */
    NONE,
  }

  /**
   * Instructs the {@link AvaticaHttpClient} how to perform hostname verification for SSL
   * connections.
   *
   * @param verification The mode of hostname verification
   */
  void setHostnameVerification(HostnameVerification verification);
}

// End HostnameVerificationConfigurable.java
