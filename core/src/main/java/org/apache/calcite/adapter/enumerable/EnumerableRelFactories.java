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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;

/**
 * Contains factory interface and default implementation for creating various
 * rel nodes.
 */
public class EnumerableRelFactories {

  public static final org.apache.calcite.rel.core.RelFactories.SortFactory
      ENUMERABLE_SORT_FACTORY = new SortFactoryImpl();

  /**
   * Implementation of {@link org.apache.calcite.rel.core.RelFactories.SortFactory} that
   * returns a vanilla {@link EnumerableSort}.
   */
  private static class SortFactoryImpl
      implements org.apache.calcite.rel.core.RelFactories.SortFactory {
    public RelNode createSort(RelNode input, RelCollation collation,
                              RexNode offset, RexNode fetch) {
      return EnumerableSort.create(input, collation, offset, fetch);
    }
  }

  private EnumerableRelFactories() {
  }
}
