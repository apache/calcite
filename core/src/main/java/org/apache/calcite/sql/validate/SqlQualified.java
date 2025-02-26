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

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.util.Util;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

import static java.util.Objects.hash;

/**
 * Fully-qualified identifier.
 *
 * <p>The result of calling
 * {@link org.apache.calcite.sql.validate.SqlValidatorScope#fullyQualify(org.apache.calcite.sql.SqlIdentifier)},
 * a fully-qualified identifier contains the name (in correct case),
 * parser position, type, and scope of each component of the identifier.
 *
 * <p>It is immutable.
 */
public class SqlQualified {
  public final int prefixLength;
  public final @Nullable SqlValidatorNamespace namespace;
  public final SqlIdentifier identifier;

  private SqlQualified(@Nullable SqlValidatorScope scope, int prefixLength,
      @Nullable SqlValidatorNamespace namespace, SqlIdentifier identifier) {
    Util.discard(scope);
    this.prefixLength = prefixLength;
    this.namespace = namespace;
    this.identifier = identifier;
  }

  @Override public int hashCode() {
    return hash(identifier.names, prefixLength);
  }

  @Override public boolean equals(@Nullable Object obj) {
    // Two SqlQualified instances are equivalent if they are of the same
    // identifier and same prefix length. Thus, in
    //
    //  SELECT e.address, e.address.zipcode
    //  FROM employees AS e
    //
    // "e.address" is {identifier=[e, address], prefixLength=1}
    // and is distinct from "e.address.zipcode".
    //
    // We assume that all SqlQualified instances being compared are resolved
    // from the same SqlValidatorScope, and therefore we do not need to look
    // at namespace to distinguish them.
    return this == obj
        || obj instanceof SqlQualified
        && prefixLength == ((SqlQualified) obj).prefixLength
        && identifier.names.equals(((SqlQualified) obj).identifier.names);
  }

  @Override public String toString() {
    return "{id: " + identifier + ", prefix: " + prefixLength + "}";
  }

  public static SqlQualified create(@Nullable SqlValidatorScope scope, int prefixLength,
      @Nullable SqlValidatorNamespace namespace, SqlIdentifier identifier) {
    return new SqlQualified(scope, prefixLength, namespace, identifier);
  }

  public final List<String> prefix() {
    return identifier.names.subList(0, prefixLength);
  }

  public final List<String> suffix() {
    return Util.skip(identifier.names, prefixLength);
  }
}
