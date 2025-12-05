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
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * A generic implementation of {@link SqlMoniker}.
 */
public class SqlMonikerImpl implements SqlMoniker {
  //~ Instance fields --------------------------------------------------------

  private final ImmutableList<String> names;
  private final SqlMonikerType type;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a moniker with an array of names.
   */
  public SqlMonikerImpl(List<String> names, SqlMonikerType type) {
    this.names = ImmutableList.copyOf(names);
    this.type = requireNonNull(type, "type");
  }

  /**
   * Creates a moniker with a single name.
   */
  public SqlMonikerImpl(String name, SqlMonikerType type) {
    this(ImmutableList.of(name), type);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public boolean equals(@Nullable Object obj) {
    return this == obj
        || obj instanceof SqlMonikerImpl
        && type == ((SqlMonikerImpl) obj).type
        && names.equals(((SqlMonikerImpl) obj).names);
  }

  @Override public int hashCode() {
    return Objects.hash(type, names);
  }

  @Override public SqlMonikerType getType() {
    return type;
  }

  @Override public List<String> getFullyQualifiedNames() {
    return names;
  }

  @Override public SqlIdentifier toIdentifier() {
    return new SqlIdentifier(names, SqlParserPos.ZERO);
  }

  @Override public String toString() {
    return Util.sepList(names, ".");
  }

  @Override public String id() {
    return type + "(" + this + ")";
  }
}
