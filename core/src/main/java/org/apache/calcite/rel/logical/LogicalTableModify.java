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

import java.util.*;

import org.eigenbase.relopt.*;

import net.hydromatic.optiq.prepare.Prepare;

/**
 * TableModificationRel is like TableAccessRel, but represents a request to
 * modify a table rather than read from it. It takes one child which produces
 * the modified rows. (For INSERT, the new values; for DELETE, the old values;
 * for UPDATE, all old values plus updated new values.)
 */
public final class TableModificationRel extends TableModificationRelBase {
  //~ Constructors -----------------------------------------------------------

  public TableModificationRel(
      RelOptCluster cluster,
      RelOptTable table,
      Prepare.CatalogReader schema,
      RelNode child,
      Operation operation,
      List<String> updateColumnList,
      boolean flattened) {
    super(
        cluster,
        cluster.traitSetOf(Convention.NONE),
        table,
        schema,
        child,
        operation,
        updateColumnList,
        flattened);
  }

  //~ Methods ----------------------------------------------------------------

  @Override
  public TableModificationRel copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    return new TableModificationRel(
        getCluster(),
        table,
        catalogReader,
        sole(inputs),
        getOperation(),
        getUpdateColumnList(),
        isFlattened());
  }
}

// End TableModificationRel.java
