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
package org.apache.calcite.jdbc;

import org.apache.calcite.avatica.AvaticaDatabaseMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.sql.SqlJdbcFunctionCall;
import org.apache.calcite.sql.parser.SqlParser;

import java.util.HashMap;
import java.util.Map;


/**
 * Definition of a database static properties.
 * {@link Meta.PropertyName} enumerates current database properties,
 * supporting some of the static String properties return via
 * {@link AvaticaDatabaseMetaData}
 */
public enum DatabaseProperties {
  NUMERIC_FUNCTIONS(Meta.DatabaseProperties.NUMERIC_FUNCTIONS,
    SqlJdbcFunctionCall.getNumericFunctions()),
  STRING_FUNCTIONS(Meta.DatabaseProperties.STRING_FUNCTIONS,
    SqlJdbcFunctionCall.getStringFunctions()),
  SYSTEM_FUNCTIONS(Meta.DatabaseProperties.SYSTEM_FUNCTIONS,
    SqlJdbcFunctionCall.getSystemFunctions()),
  TIME_DATE_FUNCTIONS(Meta.DatabaseProperties.TIME_DATE_FUNCTIONS,
    SqlJdbcFunctionCall.getTimeDateFunctions()),
  SQL_KEYWORDS(Meta.DatabaseProperties.SQL_KEYWORDS,
    SqlParser.create("").getMetadata().getJdbcKeywords());

  private Meta.DatabaseProperties databaseProperty;
  private String defaultValue;
  private static final Map<Meta.DatabaseProperties, DatabaseProperties> NAME_TO_PROPS;

  static {
    NAME_TO_PROPS = new HashMap<Meta.DatabaseProperties, DatabaseProperties>();
    for (DatabaseProperties p : DatabaseProperties.values()) {
      NAME_TO_PROPS.put(p.databaseProperty, p);
    }
  }

  DatabaseProperties(Meta.DatabaseProperties dbProp, String defaultValue) {
    this.databaseProperty = dbProp;
    this.defaultValue = defaultValue;
  }

  public Meta.DatabaseProperties databaseProperty() {
    return this.databaseProperty;
  }

  public String defaultValue() {
    return this.defaultValue;
  }

  public static String getProperty(Meta.DatabaseProperties dbProps) {
    final DatabaseProperties dbProp = NAME_TO_PROPS.get(dbProps);
    if (dbProp != null) {
      return dbProp.defaultValue;
    } else {
      return "";
    }
  }
}
