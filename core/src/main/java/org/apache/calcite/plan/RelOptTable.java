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

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Wrapper;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

/**
 * Represents a relational dataset in a {@link RelOptSchema}. It has methods to
 * describe and implement itself.
 */
public interface RelOptTable extends Wrapper {
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
   * <p>The {@link org.apache.calcite.plan.RelOptPlanner planner} calls this
   * method to convert a table into an initial relational expression,
   * generally something abstract, such as a
   * {@link org.apache.calcite.rel.logical.LogicalTableScan},
   * then optimizes this expression by
   * applying {@link org.apache.calcite.plan.RelOptRule rules} to transform it
   * into more efficient access methods for this table.</p>
   */
  RelNode toRel(ToRelContext context);

  /**
   * Returns a description of the physical ordering (or orderings) of the rows
   * returned from this table.
   *
   * @see RelMetadataQuery#collations(RelNode)
   */
  List<RelCollation> getCollationList();

  /**
   * Returns a description of the physical distribution of the rows
   * in this table.
   *
   * @see RelMetadataQuery#distribution(RelNode)
   */
  RelDistribution getDistribution();

  /**
   * Returns whether the given columns are a key or a superset of a unique key
   * of this table.
   *
   * @param columns Ordinals of key columns
   * @return Whether the given columns are a key or a superset of a key
   */
  boolean isKey(ImmutableBitSet columns);

  /**
   * Returns the referential constraints existing for this table. These constraints
   * are represented over other tables using {@link RelReferentialConstraint} nodes.
   */
  List<RelReferentialConstraint> getReferentialConstraints();

  /**
   * Generates code for this table.
   *
   * @param clazz The desired collection class; for example {@code Queryable}.
   */
  Expression getExpression(Class clazz);

  /** Returns a table with the given extra fields.
   *
   * <p>The extended table includes the fields of this base table plus the
   * extended fields that do not have the same name as a field in the base
   * table.
   */
  RelOptTable extend(List<RelDataTypeField> extendedFields);

  /** Returns a list describing how each column is populated. The list has the
   *  same number of entries as there are fields, and is immutable. */
  List<ColumnStrategy> getColumnStrategies();

  /** Can expand a view into relational expressions. */
  interface ViewExpander {
    /**
     * Returns a relational expression that is to be substituted for an access
     * to a SQL view.
     *
     * @param rowType Row type of the view
     * @param queryString Body of the view
     * @param schemaPath Path of a schema wherein to find referenced tables
     * @param viewPath Path of the view, ending with its name; may be null
     * @return Relational expression
     */
    RelRoot expandView(RelDataType rowType, String queryString,
        List<String> schemaPath, List<String> viewPath);

    /**
     * Returns a relational expression which is to be substituted for an access
     * to a SQL view.
     *
     * @param rowType Row type of the view
     * @param queryString Body of the view
     * @param rootSchema Root schema of the schema tree
     * @param schemaPath List of schema names wherein to find referenced tables
     * @return Relational expression
     */
    RelRoot expandView(RelDataType rowType, String queryString,
        SchemaPlus rootSchema, List<String> schemaPath);
  }

  /** Contains the context needed to convert a a table into a relational
   * expression. */
  interface ToRelContext extends ViewExpander {
    RelOptCluster getCluster();
  }
}

// End RelOptTable.java
