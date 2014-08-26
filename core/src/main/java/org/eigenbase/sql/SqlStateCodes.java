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
package org.eigenbase.sql;

/**
 * Contains {@link org.eigenbase.util.Glossary#SQL2003} SQL state codes. Sql
 * Sate codes are defined in
 *
 * <pre><code> &#64;sql.2003 Part 2 Section 23.1</code></pre>
 */
public enum SqlStateCodes {
  CARDINALITY_VIOLATION("cardinality violation", "21", "000"),

  NULL_VALUE_NOT_ALLOWED("null value not allowed", "22", "004"),

  NUMERIC_VALUE_OUT_OF_RANGE("numeric value out of range", "22", "003");

  private final String msg;
  private final String stateClass;
  private final String stateSubClass;

  SqlStateCodes(
      String msg,
      String stateClass,
      String stateSubClass) {
    this.msg = msg;
    this.stateClass = stateClass;
    this.stateSubClass = stateSubClass;
  }

  public String getStateClass() {
    return stateClass;
  }

  public String getStateSubClass() {
    return stateSubClass;
  }

  public String getState() {
    return stateClass + stateSubClass;
  }
}

// End SqlStateCodes.java
