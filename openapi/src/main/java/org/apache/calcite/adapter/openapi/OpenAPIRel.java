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
package org.apache.calcite.adapter.openapi;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;

import java.util.HashMap;
import java.util.Map;

/**
 * Relational expression that uses OpenAPI calling convention.
 */
public interface OpenAPIRel extends RelNode {

  /** Calling convention for relational operations that occur in OpenAPI. */
  Convention CONVENTION = new Convention.Impl("OPENAPI", OpenAPIRel.class);

  /**
   * Callback for the implementation process that converts a tree of
   * {@link OpenAPIRel} nodes into an OpenAPI request.
   */
  class Implementor {
    public final Map<String, Object> filters = new HashMap<>();
    public final Map<String, Class> projections = new HashMap<>();
    public final Map<String, RelFieldCollation.Direction> sorts = new HashMap<>();
    public Long offset;
    public Long fetch;

    public OpenAPITable openAPITable;
    public RelOptTable table;

    /**
     * Visit a child of the current node, asking it to contribute to the current request.
     */
    public void visitChild(int ordinal, RelNode input) {
      assert ordinal == 0;
      ((OpenAPIRel) input).implement(this);
    }
  }

  /**
   * Called during query planning to push operations down to the OpenAPI layer.
   *
   * @param implementor the context for building the OpenAPI request
   */
  void implement(Implementor implementor);
}
