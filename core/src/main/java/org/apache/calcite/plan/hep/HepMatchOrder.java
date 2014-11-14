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
package org.eigenbase.relopt.hep;

/**
 * HepMatchOrder specifies the order of graph traversal when looking for rule
 * matches.
 */
public enum HepMatchOrder {
  /**
   * Match in arbitrary order. This is the default because it is the most
   * efficient, and most rules don't care about order.
   */
  ARBITRARY,

  /**
   * Match from leaves up. A match attempt at a descendant precedes all match
   * attempts at its ancestors.
   */
  BOTTOM_UP,

  /**
   * Match from root down. A match attempt at an ancestor always precedes all
   * match attempts at its descendants.
   */
  TOP_DOWN
}

// End HepMatchOrder.java
