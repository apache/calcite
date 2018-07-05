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
package org.apache.calcite.sql.advise;

import org.apache.calcite.sql.validate.SqlMoniker;

import java.util.List;

/**
 * This class is used to return values for
 * {@link SqlAdvisor#getCompletionHints (String, int, String[])}.
 */
public class SqlAdvisorHint {
  /** Fully qualified object name as string. */
  public final String id;
  /** Fully qualified object name as array of names. */
  public final String[] names;
  /** One of {@link org.apache.calcite.sql.validate.SqlMonikerType}. */
  public final String type;

  public SqlAdvisorHint(String id, String[] names, String type) {
    this.id = id;
    this.names = names;
    this.type = type;
  }

  public SqlAdvisorHint(SqlMoniker id) {
    this.id = id.toString();
    final List<String> names = id.getFullyQualifiedNames();
    this.names = names == null
      ? null
      : names.toArray(new String[0]);
    type = id.getType().name();
  }
}

// End SqlAdvisorHint.java
