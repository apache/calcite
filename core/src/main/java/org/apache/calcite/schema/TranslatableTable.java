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
package org.apache.calcite.schema;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;

/**
 * Extension to {@link Table} that specifies how it is to be translated to
 * a {@link org.apache.calcite.rel.RelNode planner node}.
 *
 * <p>It is optional for a Table to implement this interface. A Table that does
 * not implement this interface, a Table will be converted to an
 * EnumerableTableAccessRel. Generally a Table will implements this interface to
 * create a particular subclass of RelNode, and also register rules that act
 * on that particular subclass of RelNode.</p>
 */
public interface TranslatableTable extends Table {
  /** Converts this table into a {@link RelNode relational expression}. */
  RelNode toRel(
      RelOptTable.ToRelContext context,
      RelOptTable relOptTable);
}

// End TranslatableTable.java
