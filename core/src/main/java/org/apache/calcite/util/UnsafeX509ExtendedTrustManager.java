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
package org.apache.calcite.util;

import java.net.Socket;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;

/**
 * This class is used to disable SSL Certificate Verification in Calcite adapters that make http
 * calls. This trust manager will validate any SSL certificate, whether valid or not. This should
 * <b>not</b> be used in production environments.
 */
@SuppressWarnings("java:S4830")
public final class UnsafeX509ExtendedTrustManager extends X509ExtendedTrustManager {

  /**
   * Return a new instance of the unsafe, all-trusting trust manager.
   */
  static final X509ExtendedTrustManager INSTANCE = new UnsafeX509ExtendedTrustManager();
  private static final X509Certificate[] EMPTY_CERTIFICATES = new X509Certificate[0];

  private UnsafeX509ExtendedTrustManager() {}

  public static X509ExtendedTrustManager getInstance() {
    return INSTANCE;
  }

  @Override public void checkClientTrusted(X509Certificate[] certificates, String authType) {
    // No op
  }

  @Override public void checkClientTrusted(X509Certificate[] certificates,
      String authType, Socket socket) {
    // No op
  }

  @Override public void checkClientTrusted(X509Certificate[] certificates,
      String authType, SSLEngine sslEngine) {
    // No op
  }

  @Override public void checkServerTrusted(X509Certificate[] certificates, String authType) {
    // No op
  }

  @Override public void checkServerTrusted(X509Certificate[] certificates,
      String authType, Socket socket) {
    // No op
  }

  @Override public void checkServerTrusted(X509Certificate[] certificates,
      String authType, SSLEngine sslEngine) {
    // No op
  }

  @Override public X509Certificate[] getAcceptedIssuers() {
    return EMPTY_CERTIFICATES;
  }
}
