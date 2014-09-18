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
import net.hydromatic.avatica.ConnectionConfigImpl;
import net.hydromatic.avatica.Quoting;

import java.util.Properties;

/** Implementation of {@link OptiqConnectionConfig}. */
public class OptiqConnectionConfigImpl extends ConnectionConfigImpl
    implements OptiqConnectionConfig {
  public OptiqConnectionConfigImpl(Properties properties) {
    super(properties);
  }

  /** Returns a copy of this configuration with one property changed. */
  public OptiqConnectionConfigImpl set(OptiqConnectionProperty property,
      String value) {
    final Properties properties1 = new Properties(properties);
    properties1.setProperty(property.camelName(), value);
    return new OptiqConnectionConfigImpl(properties1);
  }

  public boolean autoTemp() {
    return OptiqConnectionProperty.AUTO_TEMP.wrap(properties).getBoolean();
  }

  public boolean materializationsEnabled() {
    return OptiqConnectionProperty.MATERIALIZATIONS_ENABLED.wrap(properties)
        .getBoolean();
  }

  public boolean createMaterializations() {
    return OptiqConnectionProperty.CREATE_MATERIALIZATIONS.wrap(properties)
        .getBoolean();
  }

  public String model() {
    return OptiqConnectionProperty.MODEL.wrap(properties).getString();
  }

  public Lex lex() {
    return OptiqConnectionProperty.LEX.wrap(properties).getEnum(Lex.class);
  }

  public Quoting quoting() {
    return OptiqConnectionProperty.QUOTING.wrap(properties)
        .getEnum(Quoting.class, lex().quoting);
  }

  public Casing unquotedCasing() {
    return OptiqConnectionProperty.UNQUOTED_CASING.wrap(properties)
        .getEnum(Casing.class, lex().unquotedCasing);
  }

  public Casing quotedCasing() {
    return OptiqConnectionProperty.QUOTED_CASING.wrap(properties)
        .getEnum(Casing.class, lex().quotedCasing);
  }

  public boolean caseSensitive() {
    return OptiqConnectionProperty.CASE_SENSITIVE.wrap(properties)
        .getBoolean(lex().caseSensitive);
  }

  public boolean spark() {
    return OptiqConnectionProperty.SPARK.wrap(properties).getBoolean();
  }

  public <T> T typeSystem(Class<T> typeSystemClass, T defaultTypeSystem) {
    return OptiqConnectionProperty.TYPE_SYSTEM.wrap(properties)
        .getPlugin(typeSystemClass, defaultTypeSystem);
  }
}

// End OptiqConnectionConfigImpl.java
