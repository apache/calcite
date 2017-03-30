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

import org.apache.calcite.avatica.remote.AvaticaHttpClientFactory;
import org.apache.calcite.avatica.remote.Service;

import java.io.File;

/**
 * Connection configuration.
 */
public interface ConnectionConfig {
  /** @see BuiltInConnectionProperty#SCHEMA */
  String schema();
  /** @see BuiltInConnectionProperty#TIME_ZONE */
  String timeZone();
  /** @see BuiltInConnectionProperty#FACTORY */
  Service.Factory factory();
  /** @see BuiltInConnectionProperty#URL */
  String url();
  /** @see BuiltInConnectionProperty#SERIALIZATION */
  String serialization();
  /** @see BuiltInConnectionProperty#AUTHENTICATION */
  String authentication();
  /** @see BuiltInConnectionProperty#AVATICA_USER */
  String avaticaUser();
  /** @see BuiltInConnectionProperty#AVATICA_PASSWORD */
  String avaticaPassword();
  /** @see BuiltInConnectionProperty#HTTP_CLIENT_FACTORY */
  AvaticaHttpClientFactory httpClientFactory();
  /** @see BuiltInConnectionProperty#HTTP_CLIENT_IMPL */
  String httpClientClass();
  /** @see BuiltInConnectionProperty#PRINCIPAL */
  String kerberosPrincipal();
  /** @see BuiltInConnectionProperty#KEYTAB */
  File kerberosKeytab();
  /** @see BuiltInConnectionProperty#TRUSTSTORE */
  File truststore();
  /** @see BuiltInConnectionProperty#TRUSTSTORE_PASSWORD */
  String truststorePassword();
}

// End ConnectionConfig.java
