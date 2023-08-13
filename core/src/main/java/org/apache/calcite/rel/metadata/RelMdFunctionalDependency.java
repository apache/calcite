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
import org.apache.calcite.util.ImmutableBitSet;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Default implementation of
 * {@link RelMetadataQuery#functionallyDetermine(RelNode, ImmutableBitSet, int)}
 * for the standard logical algebra.
 *
 * <p>The goal of this provider is to determine whether
 * column is functionally dependent on columns.
 *
 * <p>If the functional dependency cannot be determined, we return false.
 */
public class RelMdFunctionalDependency
    implements MetadataHandler<BuiltInMetadata.FunctionalDependency> {
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          new RelMdFunctionalDependency(), BuiltInMetadata.FunctionalDependency.Handler.class);

  //~ Constructors -----------------------------------------------------------

  protected RelMdFunctionalDependency() {}

  //~ Methods ----------------------------------------------------------------

  @Override public MetadataDef<BuiltInMetadata.FunctionalDependency> getDef() {
    return BuiltInMetadata.FunctionalDependency.DEF;
  }

  /** Catch-all implementation for
   * {@link BuiltInMetadata.FunctionalDependency#functionallyDetermine(ImmutableBitSet, int)},
   * invoked using reflection.
   *
   * @see org.apache.calcite.rel.metadata.RelMetadataQuery#areColumnsUnique(
   * RelNode, ImmutableBitSet, boolean)
   */
  public @Nullable Boolean functionallyDetermine(RelNode rel, RelMetadataQuery mq,
      ImmutableBitSet columns, int column) {
    // This is a minimal implementation that needs to be gradually improved in the future.
    // If columns contains a unique key, the column is functionally dependent on columns.
    for (ImmutableBitSet subColumns : columns.powerSet()) {
      if (Boolean.TRUE.equals(mq.areColumnsUnique(rel, subColumns, false))) {
        return true;
      }
    }
    return false;
  }
}
