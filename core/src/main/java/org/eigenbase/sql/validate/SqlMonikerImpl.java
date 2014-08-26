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
package org.eigenbase.sql.validate;

import java.util.List;

import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.util.Util;

import com.google.common.collect.ImmutableList;

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
    assert names != null;
    assert type != null;
    for (String name : names) {
      assert name != null;
    }
    this.names = ImmutableList.copyOf(names);
    this.type = type;
  }

  /**
   * Creates a moniker with a single name.
   */
  public SqlMonikerImpl(String name, SqlMonikerType type) {
    this(ImmutableList.of(name), type);
  }

  //~ Methods ----------------------------------------------------------------

  public SqlMonikerType getType() {
    return type;
  }

  public List<String> getFullyQualifiedNames() {
    return names;
  }

  public SqlIdentifier toIdentifier() {
    return new SqlIdentifier(names, SqlParserPos.ZERO);
  }

  public String toString() {
    return Util.sepList(names, ".");
  }

  public String id() {
    return type + "(" + this + ")";
  }
}

// End SqlMonikerImpl.java
