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
package org.apache.calcite.sql;

/**
 * Output format for {@code EXPLAIN PLAN} statement.
 */
public enum SqlExplainFormat implements Symbolizable {
  /** Indicates that the plan should be output as a piece of indented text. */
  TEXT,

  /** Indicates that the plan should be output in XML format. */
  XML,

  /** Indicates that the plan should be output in JSON format. */
  JSON,

  /** Indicates that the plan should be output in dot format. */
  DOT
}
