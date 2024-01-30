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
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.sql.SqlExplainLevel;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * RelMdExplainVisibility supplies a default implementation of
 * {@link RelMetadataQuery#isVisibleInExplain} for the standard logical algebra.
 */
public class RelMdExplainVisibility
    implements MetadataHandler<BuiltInMetadata.ExplainVisibility> {
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          new RelMdExplainVisibility(), BuiltInMetadata.ExplainVisibility.Handler.class);

  //~ Constructors -----------------------------------------------------------

  private RelMdExplainVisibility() {}

  //~ Methods ----------------------------------------------------------------

  @Override public MetadataDef<BuiltInMetadata.ExplainVisibility> getDef() {
    return BuiltInMetadata.ExplainVisibility.DEF;
  }

  /** Catch-all implementation for
   * {@link BuiltInMetadata.ExplainVisibility#isVisibleInExplain(SqlExplainLevel)},
   * invoked using reflection.
   *
   * @see org.apache.calcite.rel.metadata.RelMetadataQuery#isVisibleInExplain(RelNode, SqlExplainLevel)
   */
  public @Nullable Boolean isVisibleInExplain(RelNode rel, RelMetadataQuery mq,
      SqlExplainLevel explainLevel) {
    // no information available
    return null;
  }

  public @Nullable Boolean isVisibleInExplain(TableScan scan, RelMetadataQuery mq,
      SqlExplainLevel explainLevel) {
    final BuiltInMetadata.ExplainVisibility.Handler handler =
        scan.getTable().unwrap(BuiltInMetadata.ExplainVisibility.Handler.class);
    if (handler != null) {
      return handler.isVisibleInExplain(scan, mq, explainLevel);
    }
    // Fall back to the catch-all.
    return isVisibleInExplain((RelNode) scan, mq, explainLevel);
  }
}
