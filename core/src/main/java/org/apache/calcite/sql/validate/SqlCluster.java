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
package org.apache.calcite.sql.validate;

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.runtime.Feature;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.parser.SqlParserPos;

import static java.util.Objects.requireNonNull;

/**
 * Contains system state for validation.
 */
public class SqlCluster {

  private final SqlOperatorTable opTab;
  private final RelDataTypeFactory relDataTypeFactory;
  private final SqlValidatorCatalogReader catalogReader;
  private int nextGeneratedId;


  public SqlCluster(
      SqlOperatorTable opTab,
      SqlValidatorCatalogReader catalogReader,
      RelDataTypeFactory typeFactory) {
    this.opTab = opTab;
    this.relDataTypeFactory = typeFactory;
    this.catalogReader = catalogReader;
  }

  public SqlOperatorTable getOpTab() {
    return opTab;
  }

  public RelDataTypeFactory getRelDataTypeFactory() {
    return relDataTypeFactory;
  }

  public SqlValidatorCatalogReader getCatalogReader() {
    return catalogReader;
  }

  public int getAndIncermentNextGeneratedId() {
    return nextGeneratedId++;
  }


  public CalciteContextException newValidationError(
      SqlNode node,
      Resources.ExInst<SqlValidatorException> e) {
    requireNonNull(node, "node");
    final SqlParserPos pos = node.getParserPosition();
    return SqlUtil.newContextException(pos, e);
  }

  /**
   * Validates that a particular feature is enabled. By default, all features
   * are enabled; subclasses may override this method to be more
   * discriminating.
   *
   * @param feature feature being used, represented as a resource instance
   * @param context parser position context for error reporting, or null if
   */
  public void validateFeature(
      Feature feature,
      SqlParserPos context) {
    // By default, do nothing except to verify that the resource
    // represents a real feature definition.
    assert feature.getProperties().get("FeatureDefinition") != null;
  }
}
