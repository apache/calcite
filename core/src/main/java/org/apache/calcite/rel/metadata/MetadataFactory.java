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

/**
 * Source of metadata about relational expressions.
 * fixme
 *      Metadata的工厂类。
 *      ？元数据通常是用来评估性能开销的各种统计数据。？
 *      每一个元数据都有一个继承了 Metadata 的接口，
 *      实现 参考 {@link BuiltInMetadata.Selectivity} 和 {@link BuiltInMetadata.ColumnUniqueness}
 *
 *
 * <p>
 *   The metadata is typically various kinds of statistics used to estimate(评估，衡量) costs.
 *
 * <p>
 *   Each kind of metadata has an interface that extends {@link Metadata} and
 *   has a method. Some examples: {@link BuiltInMetadata.Selectivity},
 *   {@link BuiltInMetadata.ColumnUniqueness}.
 */

public interface MetadataFactory {

  /** Returns a metadata interface to get a particular kind of metadata
   * from a particular relational expression. Returns null if that kind of
   * metadata is not available.
   *
   * @param <M> Metadata type
   *
   * @param rel Relational expression
   * @param mq Metadata query
   * @param metadataClazz Metadata class
   * @return Metadata bound to {@code rel} and {@code query}
   */
  <M extends Metadata> M query(RelNode rel, RelMetadataQuery mq, Class<M> metadataClazz);
}
