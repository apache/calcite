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

import org.apache.calcite.rel.type.RelDataTypeFactory;

import java.util.List;

/**
 * A <code>RelOptSchema</code> is a set of {@link RelOptTable} objects.
 */
public interface RelOptSchema {
  //~ Methods ----------------------------------------------------------------

  /**
   * Retrieves a {@link RelOptTable} based upon a member access.
   *
   * <p>For example, the Saffron expression <code>salesSchema.emps</code>
   * would be resolved using a call to <code>salesSchema.getTableForMember(new
   * String[]{"emps" })</code>.</p>
   *
   * <p>Note that name.length is only greater than 1 for queries originating
   * from JDBC.</p>
   *
   * @param names Qualified name
   */
  RelOptTable getTableForMember(List<String> names);

  /**
   * Returns the {@link RelDataTypeFactory type factory} used to generate
   * types for this schema.
   */
  RelDataTypeFactory getTypeFactory();

  /**
   * Registers all of the rules supported by this schema. Only called by
   * {@link RelOptPlanner#registerSchema}.
   */
  void registerRules(RelOptPlanner planner) throws Exception;
}

// End RelOptSchema.java
