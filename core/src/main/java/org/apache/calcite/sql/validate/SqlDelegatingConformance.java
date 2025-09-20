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
 * Implementation of {@link SqlConformance} that delegates all methods to
 * another object. You can create a sub-class that overrides particular
 * methods.
 */
public class SqlDelegatingConformance implements SqlConformance {
  private final SqlConformance delegate;

  /** Creates a SqlDelegatingConformance. */
  protected SqlDelegatingConformance(SqlConformance delegate) {
    this.delegate = delegate;
  }

  @Override public boolean isLiberal() {
    return delegate.isLiberal();
  }

  @Override public boolean allowCharLiteralAlias() {
    return delegate.allowCharLiteralAlias();
  }

  @Override public boolean isSupportedDualTable() {
    return delegate.isSupportedDualTable();
  }

  @Override public boolean isGroupByAlias() {
    return delegate.isGroupByAlias();
  }

  @Override public SelectAliasLookup isSelectAlias() {
    return delegate.isSelectAlias();
  }

  @Override public boolean isNonStrictGroupBy() {
    return delegate.isNonStrictGroupBy();
  }

  @Override public boolean isGroupByOrdinal() {
    return delegate.isGroupByOrdinal();
  }

  @Override public boolean isHavingAlias() {
    return delegate.isHavingAlias();
  }

  @Override public boolean isSortByOrdinal() {
    return delegate.isSortByOrdinal();
  }

  @Override public boolean isSortByAlias() {
    return delegate.isSortByAlias();
  }

  @Override public boolean isSortByAliasObscures() {
    return delegate.isSortByAliasObscures();
  }

  @Override public boolean isFromRequired() {
    return delegate.isFromRequired();
  }

  @Override public boolean splitQuotedTableName() {
    return delegate.splitQuotedTableName();
  }

  @Override public boolean allowHyphenInUnquotedTableName() {
    return delegate.allowHyphenInUnquotedTableName();
  }

  @Override public boolean isBangEqualAllowed() {
    return delegate.isBangEqualAllowed();
  }

  @Override public boolean isPercentRemainderAllowed() {
    return delegate.isPercentRemainderAllowed();
  }

  @Override public boolean isMinusAllowed() {
    return delegate.isMinusAllowed();
  }

  @Override public boolean isApplyAllowed() {
    return delegate.isApplyAllowed();
  }

  @Override public boolean isInsertSubsetColumnsAllowed() {
    return delegate.isInsertSubsetColumnsAllowed();
  }

  @Override public boolean allowAliasUnnestItems() {
    return delegate.allowAliasUnnestItems();
  }

  @Override public boolean allowNiladicParentheses() {
    return delegate.allowNiladicParentheses();
  }

  @Override public boolean allowNiladicConstantWithoutParentheses() {
    return delegate.allowNiladicConstantWithoutParentheses();
  }
  @Override public boolean allowExplicitRowValueConstructor() {
    return delegate.allowExplicitRowValueConstructor();
  }

  @Override public boolean allowExtend() {
    return delegate.allowExtend();
  }

  @Override public boolean isLimitStartCountAllowed() {
    return delegate.isLimitStartCountAllowed();
  }

  @Override public boolean isOffsetLimitAllowed() {
    return delegate.isOffsetLimitAllowed();
  }

  @Override public boolean allowGeometry() {
    return delegate.allowGeometry();
  }

  @Override public boolean shouldConvertRaggedUnionTypesToVarying() {
    return delegate.shouldConvertRaggedUnionTypesToVarying();
  }

  @Override public boolean allowExtendedTrim() {
    return delegate.allowExtendedTrim();
  }

  @Override public boolean allowPluralTimeUnits() {
    return delegate.allowPluralTimeUnits();
  }

  @Override public boolean allowQualifyingCommonColumn() {
    return delegate.allowQualifyingCommonColumn();
  }

  @Override public boolean isValueAllowed() {
    return delegate.isValueAllowed();
  }

  @Override public SqlLibrary semantics() {
    return delegate.semantics();
  }

  @Override public boolean allowLenientCoercion() {
    return delegate.allowLenientCoercion();
  }

  @Override public boolean checkedArithmetic() {
    return delegate.checkedArithmetic();
  }

  @Override public boolean supportsUnsignedTypes() {
    return delegate.supportsUnsignedTypes();
  }
}
