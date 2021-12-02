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

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import org.apiguardian.api.API;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Base class for the HandlerBasedRelMetadataQuery that uses the metadata handler class
 * generated by the Janino.
 *
 * <p>To add a new implementation to this interface, follow
 * these steps:
 *
 * <ol>
 * <li>Extends {@link HandlerBasedRelMetadataQuery} (name it MyRelMetadataQuery for example)
 * to reuse the Calcite builtin metadata query interfaces. In this class, define all the
 * extended Handlers for your metadata and implement the metadata query interfaces.
 * <li>Write your customized provider class <code>RelMdXyz</code>. Follow
 * the pattern from an existing class such as {@link RelMdColumnOrigins},
 * overloading on all of the logical relational expressions to which the query
 * applies.
 * <li>Add a {@code SOURCE} static member to each of your provider class, similar to
 * {@link RelMdColumnOrigins#SOURCE}.
 * <li>Extends {@link DefaultRelMetadataProvider} (name it MyRelMetadataProvider for example)
 * and supplement the "SOURCE"s into the builtin list
 * (This is not required, use {@link ChainedRelMetadataProvider} to chain your customized
 * "SOURCE"s with default ones also works).
 * <li>Set {@code MyRelMetadataProvider} into the cluster instance.
 * <li>Use
 * {@link org.apache.calcite.plan.RelOptCluster#setMetadataQuerySupplier(Supplier)}
 * to set the metadata query {@link Supplier} into the cluster instance. This {@link Supplier}
 * should return a <strong>fresh new</strong> instance.
 * <li>Use the cluster instance to create
 * {@link org.apache.calcite.sql2rel.SqlToRelConverter}.</li>
 * <li>Query your metadata within {@link org.apache.calcite.plan.RelOptRuleCall} with the
 * interfaces you defined in {@code MyRelMetadataQuery}.
 * </ol>
 */
public class RelMetadataQueryBase {
  //~ Instance fields --------------------------------------------------------

  /**
   * Set of active metadata queries, and cache of previous results.
   *
   * <p>Exposed due to the way Janino visibility challeneges, will not be exposed on
   * RelMetadataQuery directly in the future. Will only be exposed on this object and
   * {@link HandlerBasedRelMetadataQuery} in the future.
   *
   * <p>Marked API.Status.INTERNAL since 1.29
   */
  @API(status = API.Status.INTERNAL)
  public final Table<RelNode, Object, Object> map = HashBasedTable.create();

  /**
   * Thread local Janino based provider.
   *
   * <p>Exposed due to the way Janino visibility challeneges, will not be exposed on
   * RelMetadataQuery directly in the future. Will only be exposed on this object and
   * {@link HandlerBasedRelMetadataQuery} in the future.
   *
   * <p>Marked API.Status.INTERNAL since 1.29
   */
  @API(status = API.Status.INTERNAL)
  public final @Nullable JaninoRelMetadataProvider metadataProvider;

  //~ Static fields/initializers ---------------------------------------------

  /**
   * Thread local Janino based provider.
   *
   * <p>Exposed due to the way Janino visibility challeneges, will not be exposed on
   * RelMetadataQuery directly in the future. Will only be exposed on this object and
   * {@link HandlerBasedRelMetadataQuery} in the future.
   *
   * <p>Marked API.Status.INTERNAL since 1.29
   */
  @API(status = API.Status.INTERNAL)
  public static final ThreadLocal<@Nullable JaninoRelMetadataProvider> THREAD_PROVIDERS =
      new ThreadLocal<>();

  //~ Constructors -----------------------------------------------------------

  protected RelMetadataQueryBase(@Nullable JaninoRelMetadataProvider metadataProvider) {
    this.metadataProvider = metadataProvider;
  }

  /**
   * Create an initial handler for the provided handler class.
   *
   * @deprecated Use {@link HandlerBasedRelMetadataQuery#createInitialHandler}.
   */
  @Deprecated // to be removed before 1.30
  protected static <H> H initialHandler(Class<H> handlerClass) {
    return handlerClass.cast(
        Proxy.newProxyInstance(RelMetadataQuery.class.getClassLoader(),
            new Class[] {handlerClass}, (proxy, method, args) -> {
              final RelNode r = requireNonNull((RelNode) args[0], "(RelNode) args[0]");
              throw new JaninoRelMetadataProvider.NoHandler(r.getClass());
            }));
  }

  //~ Methods ----------------------------------------------------------------

  /** Re-generates the handler for a given kind of metadata, adding support for
   * {@code class_} if it is not already present. */
  @Deprecated // to be removed before 2.0
  protected <M extends Metadata, H extends MetadataHandler<M>> H
      revise(Class<? extends RelNode> class_, MetadataDef<M> def) {
    requireNonNull(metadataProvider, "metadataProvider");
    return (H) revise(def.handlerClass);
  }

  /** Re-generates the handler for a given kind of metadata, adding support for
   * {@code class_} if it is not already present. */
  protected <H extends MetadataHandler<?>> H revise(Class<H> def) {
    requireNonNull(metadataProvider, "metadataProvider");
    return metadataProvider.revise(def);
  }

  /**
   * Removes cached metadata values for specified RelNode.
   *
   * @param rel RelNode whose cached metadata should be removed
   * @return true if cache for the provided RelNode was not empty
   */
  public boolean clearCache(RelNode rel) {
    Map<Object, Object> row = map.row(rel);
    if (row.isEmpty()) {
      return false;
    }

    row.clear();
    return true;
  }
}
