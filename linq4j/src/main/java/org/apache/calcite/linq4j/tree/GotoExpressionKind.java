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
package org.apache.calcite.linq4j.tree;

/**
 * Specifies what kind of jump a {@link GotoStatement} represents.
 */
public enum GotoExpressionKind {
  /**
   * A GotoExpression that represents a jump to some location.
   */
  Goto("goto "),

  /**
   * A GotoExpression that represents a return statement.
   */
  Return("return"),

  /**
   * A GotoExpression that represents a break statement.
   */
  Break("break"),

  /**
   * A GotoExpression that represents a continue statement.
   */
  Continue("continue"),

  /**
   * A GotoExpression that evaluates an expression and carries on.
   */
  Sequence("");

  final String prefix;

  GotoExpressionKind(String prefix) {
    this.prefix = prefix;
  }
}

// End GotoExpressionKind.java
