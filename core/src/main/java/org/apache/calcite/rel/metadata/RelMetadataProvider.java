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
package org.apache.calcite.rel.metadata;

import org.apache.calcite.rel.RelNode;

import com.google.common.collect.Multimap;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Method;
import java.util.List;

/**
 * RelMetadataProvider defines an interface for obtaining metadata about
 * relational expressions. This interface is weakly-typed and is not intended to
 * be called directly in most contexts; instead, use a strongly-typed facade
 * such as {@link RelMetadataQuery}.
 *
 * <p>For background and motivation, see <a
 * href="http://wiki.eigenbase.org/RelationalExpressionMetadata">wiki</a>.
 *
 * <p>If your provider is not a singleton, we recommend that you implement
 * {@link Object#equals(Object)} and {@link Object#hashCode()} methods. This
 * makes the cache of {@link JaninoRelMetadataProvider} more effective.
 */
public interface RelMetadataProvider {
  //~ Methods ----------------------------------------------------------------

  /**
   * Retrieves metadata of a particular type and for a particular sub-class
   * of relational expression.
   *
   * <p>The object returned is a function. It can be applied to a relational
   * expression of the given type to create a metadata object.
   *
   * <p>For example, you might call
   *
   * <blockquote><pre>
   * RelMetadataProvider provider;
   * LogicalFilter filter;
   * RexNode predicate;
   * Function&lt;RelNode, Metadata&gt; function =
   *   provider.apply(LogicalFilter.class, Selectivity.class};
   * Selectivity selectivity = function.apply(filter);
   * Double d = selectivity.selectivity(predicate);
   * </pre></blockquote>
   *
   * @deprecated Use {@link RelMetadataQuery}.
   *
   * @param relClass Type of relational expression
   * @param metadataClass Type of metadata
   * @return Function that will field a metadata instance; or null if this
   *     provider cannot supply metadata of this type
   */
  @Deprecated // to be removed before 2.0
  <@Nullable M extends @Nullable Metadata> @Nullable UnboundMetadata<M> apply(
      Class<? extends RelNode> relClass, Class<? extends M> metadataClass);

  @Deprecated // to be removed before 2.0
  <M extends Metadata> Multimap<Method, MetadataHandler<M>> handlers(
      MetadataDef<M> def);

  /**
   * Retrieves a list of {@link MetadataHandler} for implements a particular
   * {@link MetadataHandler}.class.  The resolution order is specificity of the
   * relNode class, with preference given to handlers that occur earlier in the
   * list.
   *
   * <p>For instance, given a return list of {A, B, C} where A implements
   * RelNode and Scan, B implements Scan, and C implements LogicalScan and
   * Filter.
   *
   * <p>Scan dispatches to a.method(Scan);
   * LogicalFilter dispatches to c.method(Filter);
   * LogicalScan dispatches to c.method(LogicalScan);
   * Aggregate dispatches to a.method(RelNode).
   *
   * <p>The behavior is undefined if the class hierarchy for dispatching is not
   * a tree.
   */
  List<MetadataHandler<?>> handlers(Class<? extends MetadataHandler<?>> handlerClass);
}
