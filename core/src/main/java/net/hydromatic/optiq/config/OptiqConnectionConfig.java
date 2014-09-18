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
package net.hydromatic.optiq.config;

import net.hydromatic.avatica.Casing;
import net.hydromatic.avatica.ConnectionConfig;
import net.hydromatic.avatica.Quoting;

/** Interface for reading connection properties within Optiq code. There is
 * a method for every property. At some point there will be similar config
 * classes for system and statement properties. */
public interface OptiqConnectionConfig extends ConnectionConfig {
  /** @see net.hydromatic.optiq.config.OptiqConnectionProperty#AUTO_TEMP */
  boolean autoTemp();
  /** @see net.hydromatic.optiq.config.OptiqConnectionProperty#MATERIALIZATIONS_ENABLED */
  boolean materializationsEnabled();
  /** @see net.hydromatic.optiq.config.OptiqConnectionProperty#CREATE_MATERIALIZATIONS */
  boolean createMaterializations();
  /** @see net.hydromatic.optiq.config.OptiqConnectionProperty#MODEL */
  String model();
  /** @see net.hydromatic.optiq.config.OptiqConnectionProperty#LEX */
  Lex lex();
  /** @see net.hydromatic.optiq.config.OptiqConnectionProperty#QUOTING */
  Quoting quoting();
  /** @see net.hydromatic.optiq.config.OptiqConnectionProperty#UNQUOTED_CASING */
  Casing unquotedCasing();
  /** @see net.hydromatic.optiq.config.OptiqConnectionProperty#QUOTED_CASING */
  Casing quotedCasing();
  /** @see net.hydromatic.optiq.config.OptiqConnectionProperty#CASE_SENSITIVE */
  boolean caseSensitive();
  /** @see net.hydromatic.optiq.config.OptiqConnectionProperty#SPARK */
  boolean spark();
  /** @see net.hydromatic.optiq.config.OptiqConnectionProperty#TYPE_SYSTEM */
  <T> T typeSystem(Class<T> typeSystemClass, T defaultTypeSystem);
}

// End OptiqConnectionConfig.java
