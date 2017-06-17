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

import com.google.common.collect.ImmutableList;

import java.util.List;

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
  private final SqlValidatorScope scope;
  public final int prefixLength;
  public final SqlValidatorNamespace namespace;
  public final SqlIdentifier identifier;

  private SqlQualified(SqlValidatorScope scope, int prefixLength,
      SqlValidatorNamespace namespace, SqlIdentifier identifier) {
    this.scope = scope;
    this.prefixLength = prefixLength;
    this.namespace = namespace;
    this.identifier = identifier;
  }

  @Override public String toString() {
    return "{id: " + identifier.toString() + ", prefix: " + prefixLength + "}";
  }

  public static SqlQualified create(SqlValidatorScope scope, int prefixLength,
      SqlValidatorNamespace namespace, SqlIdentifier identifier) {
    return new SqlQualified(scope, prefixLength, namespace, identifier);
  }

  public List<String> translatedNames() {
    if (scope == null) {
      return identifier.names;
    }
    final SqlNameMatcher nameMatcher =
        scope.getValidator().getCatalogReader().nameMatcher();
    final ImmutableList.Builder<String> builder = ImmutableList.builder();
    final SqlValidatorScope.ResolvedImpl resolved =
        new SqlValidatorScope.ResolvedImpl();
    final List<String> prefix = Util.skipLast(identifier.names);
    scope.resolve(prefix, nameMatcher, false, resolved);
    SqlValidatorNamespace namespace =
        resolved.count() == 1 ? resolved.only().namespace : null;
    builder.add(identifier.names.get(0));
    for (String name : Util.skip(identifier.names)) {
      if (namespace != null) {
        name = namespace.translate(name);
        namespace = null;
      }
      builder.add(name);
    }
    return builder.build();
  }

  public final List<String> prefix() {
    return identifier.names.subList(0, prefixLength);
  }

  public final List<String> suffix() {
    return Util.skip(identifier.names, prefixLength);
  }

  public final List<String> suffixTranslated() {
    return Util.skip(translatedNames(), prefixLength);
  }
}

// End SqlQualified.java
