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
package org.eigenbase.relopt;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.reltype.*;

import net.hydromatic.linq4j.expressions.Expression;

import com.google.common.collect.ImmutableList;

/**
 * Partial implementation of {@link RelOptTable}.
 */
public abstract class RelOptAbstractTable implements RelOptTable {
  //~ Instance fields --------------------------------------------------------

  protected final RelOptSchema schema;
  protected final RelDataType rowType;
  protected final String name;

  //~ Constructors -----------------------------------------------------------

  protected RelOptAbstractTable(
      RelOptSchema schema,
      String name,
      RelDataType rowType) {
    this.schema = schema;
    this.name = name;
    this.rowType = rowType;
  }

  //~ Methods ----------------------------------------------------------------

  public String getName() {
    return name;
  }

  public List<String> getQualifiedName() {
    return ImmutableList.of(name);
  }

  public double getRowCount() {
    return 100;
  }

  public RelDataType getRowType() {
    return rowType;
  }

  public RelOptSchema getRelOptSchema() {
    return schema;
  }

  // Override to define collations.
  public List<RelCollation> getCollationList() {
    return Collections.emptyList();
  }

  public <T> T unwrap(Class<T> clazz) {
    return clazz.isInstance(this)
        ? clazz.cast(this)
        : null;
  }

  // Override to define keys
  public boolean isKey(BitSet columns) {
    return false;
  }

  public RelNode toRel(ToRelContext context) {
    return new TableAccessRel(context.getCluster(), this);
  }

  public Expression getExpression(Class clazz) {
    throw new UnsupportedOperationException();
  }
}

// End RelOptAbstractTable.java
