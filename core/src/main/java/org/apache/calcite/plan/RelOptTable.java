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

/**
 * Represents a relational dataset in a {@link RelOptSchema}. It has methods to
 * describe and implement itself.
 */
public interface RelOptTable {
  //~ Methods ----------------------------------------------------------------

  /**
   * Obtains an identifier for this table. The identifier must be unique with
   * respect to the Connection producing this table.
   *
   * @return qualified name
   */
  List<String> getQualifiedName();

  /**
   * Returns an estimate of the number of rows in the table.
   */
  double getRowCount();

  /**
   * Describes the type of rows returned by this table.
   */
  RelDataType getRowType();

  /**
   * Returns the {@link RelOptSchema} this table belongs to.
   */
  RelOptSchema getRelOptSchema();

  /**
   * Converts this table into a {@link RelNode relational expression}.
   *
   * <p>The {@link org.eigenbase.relopt.RelOptPlanner planner} calls this
   * method to convert a table into an initial relational expression,
   * generally something abstract, such as a {@link
   * org.eigenbase.rel.TableAccessRel}, then optimizes this expression by
   * applying {@link org.eigenbase.relopt.RelOptRule rules} to transform it
   * into more efficient access methods for this table.</p>
   */
  RelNode toRel(ToRelContext context);

  /**
   * Returns a description of the physical ordering (or orderings) of the rows
   * returned from this table.
   *
   * @see RelNode#getCollationList()
   */
  List<RelCollation> getCollationList();

  /**
   * Returns whether the given columns are a key or a superset of a unique key
   * of this table.
   *
   * @param columns Ordinals of key columns
   * @return Whether the given columns are a key or a superset of a key
   */
  boolean isKey(BitSet columns);

  /**
   * Finds an interface implemented by this table.
   */
  <T> T unwrap(Class<T> clazz);

  /**
   * Generates code for this table.
   *
   * @param clazz The desired collection class; for example {@code Queryable}.
   */
  Expression getExpression(Class clazz);

  /** Can expand a view into relational expressions. */
  interface ViewExpander {
    RelNode expandView(
        RelDataType rowType,
        String queryString,
        List<String> schemaPath);
  }

  /** Contains the context needed to convert a a table into a relational
   * expression. */
  interface ToRelContext extends ViewExpander {
    RelOptCluster getCluster();
  }
}

// End RelOptTable.java
