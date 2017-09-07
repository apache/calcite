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
package org.apache.calcite.sql.dialect;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Default sequence information implementation.
 */
public class SequenceInformationImpl implements SqlDialect.SequenceInformation {
  private final String catalog;
  private final String schema;
  private final String name;
  private final SqlTypeName typeName;

  private final int increment;

  public SequenceInformationImpl(String catalog, String schema, String name, SqlTypeName typeName,
                                 int increment) {
    this.catalog = catalog;
    this.schema = schema;
    this.name = name;
    this.typeName = typeName;
    this.increment = increment;
  }

  @Override public String getCatalog() {
    return catalog;
  }

  @Override public String getSchema() {
    return schema;
  }

  @Override public String getName() {
    return name;
  }

  @Override public SqlTypeName getType() {
    return typeName;
  }

  @Override public int getIncrement() {
    return increment;
  }
}

// End SequenceInformationImpl.java
