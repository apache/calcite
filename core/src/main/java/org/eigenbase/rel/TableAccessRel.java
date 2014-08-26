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
package org.eigenbase.rel;

import java.util.List;

import org.eigenbase.relopt.*;

/**
 * A <code>TableAccessRel</code> reads all the rows from a {@link RelOptTable}.
 *
 * <p>If the table is a <code>net.sf.saffron.ext.JdbcTable</code>, then this is
 * literally possible. But for other kinds of tables, there may be many ways to
 * read the data from the table. For some kinds of table, it may not even be
 * possible to read all of the rows unless some narrowing constraint is applied.
 *
 * <p>In the example of the <code>net.sf.saffron.ext.ReflectSchema</code>
 * schema,</p>
 *
 * <blockquote>
 * <pre>select from fields</pre>
 * </blockquote>
 *
 * <p>cannot be implemented, but</p>
 *
 * <blockquote>
 * <pre>select from fields as f
 * where f.getClass().getName().equals("java.lang.String")</pre>
 * </blockquote>
 *
 * <p>can. It is the optimizer's responsibility to find these ways, by applying
 * transformation rules.</p>
 */
public final class TableAccessRel extends TableAccessRelBase {
  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a TableAccessRel.
   *
   * @param cluster Cluster
   * @param table   Table
   */
  public TableAccessRel(
      RelOptCluster cluster,
      RelOptTable table) {
    super(
        cluster,
        cluster.traitSetOf(Convention.NONE),
        table);
  }

  /**
   * Creates a TableAccessRel by parsing serialized output.
   */
  public TableAccessRel(RelInput input) {
    super(input);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    assert inputs.isEmpty();
    return this;
  }
}

// End TableAccessRel.java
