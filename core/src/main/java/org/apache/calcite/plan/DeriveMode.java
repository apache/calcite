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
package org.apache.calcite.plan;

/**
 * The mode of trait derivation.
 */
public enum DeriveMode {
  /**
   * Uses the left most child's traits to decide what
   * traits to require from the other children. This
   * generally applies to most operators.
   */
  LEFT_FIRST,

  /**
   * Uses the right most child's traits to decide what
   * traits to require from the other children. Operators
   * like index nested loop join may find this useful.
   */
  RIGHT_FIRST,

  /**
   * Iterates over each child, uses current child's traits
   * to decide what traits to require from the other
   * children. It includes both LEFT_FIRST and RIGHT_FIRST.
   * System that doesn't enable join commutativity should
   * consider this option. Special customized operators
   * like a Join who has 3 inputs may find this useful too.
   */
  BOTH,

  /**
   * Leave it to you, you decide what you cook. This will
   * allow planner to pass all the traits from all the
   * children, the user decides how to make use of these
   * traits and whether to derive new rel nodes.
   */
  OMAKASE,

  /**
   * Trait derivation is prohibited.
   */
  PROHIBITED
}
