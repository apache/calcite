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
package org.apache.calcite.access;

import org.apache.calcite.sql.SqlAccessEnum;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorTable;

import java.util.List;

/**
 * Wraps data needed for guard to decide whether access should be granted or not.
 */
public class AuthorizationRequest {

  private final SqlAccessEnum requiredAccess;
  private final SqlNode node;
  private final SqlValidatorTable table;
  private final List<String> objectPath;
  private final SqlValidatorCatalogReader catalogReader;

  public AuthorizationRequest(
      SqlAccessEnum requiredAccess,
      SqlNode node,
      SqlValidatorTable table,
      List<String> objectPath,
      SqlValidatorCatalogReader catalogReader) {
    this.requiredAccess = requiredAccess;
    this.node = node;
    this.table = table;
    this.objectPath = objectPath;
    this.catalogReader = catalogReader;
  }

  public SqlAccessEnum getRequiredAccess() {
    return requiredAccess;
  }

  public SqlNode getNode() {
    return node;
  }

  public SqlValidatorTable getTable() {
    return table;
  }

  public List<String> getObjectPath() {
    return objectPath;
  }

  public SqlValidatorCatalogReader getCatalogReader() {
    return catalogReader;
  }

}

// End AuthorizationRequest.java
