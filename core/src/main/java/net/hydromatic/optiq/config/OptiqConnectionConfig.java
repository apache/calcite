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
  boolean autoTemp();
  boolean materializationsEnabled();
  String model();
  Lex lex();
  Quoting quoting();
  Casing unquotedCasing();
  Casing quotedCasing();
  boolean caseSensitive();
  boolean spark();
}

// End OptiqConnectionConfig.java
