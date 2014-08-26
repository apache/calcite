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
package net.hydromatic.optiq.tools;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.parser.SqlParseException;

/**
 * A fa&ccedil;ade that covers Optiq's query planning process: parse SQL,
 * validate the parse tree, convert the parse tree to a relational expression,
 * and optimize the relational expression.
 *
 * <p>Planner is NOT thread safe. However, it can be reused for
 * different queries. The consumer of this interface is responsible for calling
 * reset() after each use of Planner that corresponds to a different
 * query.
 */
public interface Planner {
  /**
   * Parses and validates a SQL statement.
   *
   * @param sql The SQL statement to parse.
   * @return The root node of the SQL parse tree.
   * @throws org.eigenbase.sql.parser.SqlParseException on parse error
   */
  SqlNode parse(String sql) throws SqlParseException;

  /**
   * Validates a SQL statement.
   *
   * @param sqlNode Root node of the SQL parse tree.
   * @return Validated node
   * @throws ValidationException if not valid
   */
  SqlNode validate(SqlNode sqlNode) throws ValidationException;

  /**
   * Converts a SQL parse tree into a tree of relational expressions.
   *
   * <p>You must call {@link #validate(org.eigenbase.sql.SqlNode)} first.</p>
   *
   * @param sql The root node of the SQL parse tree.
   * @return The root node of the newly generated RelNode tree.
   * @throws net.hydromatic.optiq.tools.RelConversionException if the node
   * cannot be converted or has not been validated
   */
  RelNode convert(SqlNode sql) throws RelConversionException;

  /** Returns the type factory. */
  RelDataTypeFactory getTypeFactory();

  /**
   * Converts one relational expression tree into another relational expression
   * based on a particular rule set and requires set of traits.
   *
   * @param ruleSetIndex The RuleSet to use for conversion purposes.  Note that
   *                     this is zero-indexed and is based on the list and order
   *                     of RuleSets provided in the construction of this
   *                     Planner.
   * @param requiredOutputTraits The set of RelTraits required of the root node
   *                             at the termination of the planning cycle.
   * @param rel The root of the RelNode tree to convert.
   * @return The root of the new RelNode tree.
   * @throws net.hydromatic.optiq.tools.RelConversionException on conversion
   *     error
   */
  RelNode transform(int ruleSetIndex,
      RelTraitSet requiredOutputTraits, RelNode rel)
      throws RelConversionException;

  /**
   * Resets this {@code Planner} to be used with a new query. This
   * should be called between each new query.
   */
  void reset();

  /**
   * Releases all internal resources utilized while this {@code Planner}
   * exists.  Once called, this Planner object is no longer valid.
   */
  void close();

  RelTraitSet getEmptyTraitSet();
}

// End Planner.java
