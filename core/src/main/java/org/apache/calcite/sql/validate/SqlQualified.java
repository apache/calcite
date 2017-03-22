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
  public final int prefixLength;
  public final SqlValidatorNamespace namespace;
  public final SqlIdentifier identifier;

  private SqlQualified(SqlValidatorScope scope, int prefixLength,
      SqlValidatorNamespace namespace, SqlIdentifier identifier) {
    Util.discard(scope);
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

  public final List<String> prefix() {
    return identifier.names.subList(0, prefixLength);
  }

  public final List<String> suffix() {
    return Util.skip(identifier.names, prefixLength);
  }
}

// End SqlQualified.java
