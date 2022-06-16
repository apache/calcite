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

import org.apache.calcite.sql.fun.SqlLibrary;

/**
 * Abstract base class for implementing {@link SqlConformance}.
 *
 * <p>Every method in {@code SqlConformance} is implemented,
 * and behaves the same as in {@link SqlConformanceEnum#DEFAULT}.
 */
public abstract class SqlAbstractConformance implements SqlConformance {
  @Override public boolean isLiberal() {
    return SqlConformanceEnum.DEFAULT.isLiberal();
  }

  @Override public boolean allowCharLiteralAlias() {
    return SqlConformanceEnum.DEFAULT.allowCharLiteralAlias();
  }

  @Override public boolean isGroupByAlias() {
    return SqlConformanceEnum.DEFAULT.isGroupByAlias();
  }

  @Override public boolean isGroupByOrdinal() {
    return SqlConformanceEnum.DEFAULT.isGroupByOrdinal();
  }

  @Override public boolean isHavingAlias() {
    return SqlConformanceEnum.DEFAULT.isHavingAlias();
  }

  @Override public boolean isSortByOrdinal() {
    return SqlConformanceEnum.DEFAULT.isSortByOrdinal();
  }

  @Override public boolean isSortByAlias() {
    return SqlConformanceEnum.DEFAULT.isSortByAlias();
  }

  @Override public boolean isSortByAliasObscures() {
    return SqlConformanceEnum.DEFAULT.isSortByAliasObscures();
  }

  @Override public boolean isFromRequired() {
    return SqlConformanceEnum.DEFAULT.isFromRequired();
  }

  @Override public boolean splitQuotedTableName() {
    return SqlConformanceEnum.DEFAULT.splitQuotedTableName();
  }

  @Override public boolean allowHyphenInUnquotedTableName() {
    return SqlConformanceEnum.DEFAULT.allowHyphenInUnquotedTableName();
  }

  @Override public boolean isBangEqualAllowed() {
    return SqlConformanceEnum.DEFAULT.isBangEqualAllowed();
  }

  @Override public boolean isMinusAllowed() {
    return SqlConformanceEnum.DEFAULT.isMinusAllowed();
  }

  @Override public boolean isApplyAllowed() {
    return SqlConformanceEnum.DEFAULT.isApplyAllowed();
  }

  @Override public boolean isInsertSubsetColumnsAllowed() {
    return SqlConformanceEnum.DEFAULT.isInsertSubsetColumnsAllowed();
  }

  @Override public boolean allowNiladicParentheses() {
    return SqlConformanceEnum.DEFAULT.allowNiladicParentheses();
  }

  @Override public boolean allowExplicitRowValueConstructor() {
    return SqlConformanceEnum.DEFAULT.allowExplicitRowValueConstructor();
  }

  @Override public boolean allowExtend() {
    return SqlConformanceEnum.DEFAULT.allowExtend();
  }

  @Override public boolean isLimitStartCountAllowed() {
    return SqlConformanceEnum.DEFAULT.isLimitStartCountAllowed();
  }

  @Override public boolean isOffsetLimitAllowed() {
    return SqlConformanceEnum.DEFAULT.isOffsetLimitAllowed();
  }

  @Override public boolean isPercentRemainderAllowed() {
    return SqlConformanceEnum.DEFAULT.isPercentRemainderAllowed();
  }

  @Override public boolean allowGeometry() {
    return SqlConformanceEnum.DEFAULT.allowGeometry();
  }

  @Override public boolean shouldConvertRaggedUnionTypesToVarying() {
    return SqlConformanceEnum.DEFAULT.shouldConvertRaggedUnionTypesToVarying();
  }

  @Override public boolean allowExtendedTrim() {
    return SqlConformanceEnum.DEFAULT.allowExtendedTrim();
  }

  @Override public boolean allowPluralTimeUnits() {
    return SqlConformanceEnum.DEFAULT.allowPluralTimeUnits();
  }

  @Override public boolean allowQualifyingCommonColumn() {
    return SqlConformanceEnum.DEFAULT.allowQualifyingCommonColumn();
  }

  @Override public boolean allowAliasUnnestItems() {
    return SqlConformanceEnum.DEFAULT.allowAliasUnnestItems();
  }

  @Override public SqlLibrary semantics() {
    return SqlConformanceEnum.DEFAULT.semantics();
  }

}
