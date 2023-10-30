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
package org.apache.calcite.rel.metadata.janino;

public final class GeneratedMetadata_ExplainVisibilityHandler
  implements org.apache.calcite.rel.metadata.BuiltInMetadata.ExplainVisibility.Handler {
  private final Object methodKey0Null =
      new org.apache.calcite.rel.metadata.janino.DescriptiveCacheKey("Boolean Handler.isVisibleInExplain(null)");
  private final Object[] methodKey0 =
      org.apache.calcite.rel.metadata.janino.CacheUtil.generateEnum("Boolean isVisibleInExplain", org.apache.calcite.sql.SqlExplainLevel.values());
  public final org.apache.calcite.rel.metadata.RelMdExplainVisibility provider0;
  public GeneratedMetadata_ExplainVisibilityHandler(
      org.apache.calcite.rel.metadata.RelMdExplainVisibility provider0) {
    this.provider0 = provider0;
  }
  public org.apache.calcite.rel.metadata.MetadataDef getDef() {
    return provider0.getDef();
  }
  public java.lang.Boolean isVisibleInExplain(
      org.apache.calcite.rel.RelNode r,
      org.apache.calcite.rel.metadata.RelMetadataQuery mq,
      org.apache.calcite.sql.SqlExplainLevel a2) {
    while (r instanceof org.apache.calcite.rel.metadata.DelegatingMetadataRel) {
      r = ((org.apache.calcite.rel.metadata.DelegatingMetadataRel) r).getMetadataDelegateRel();
    }
    final Object key;
    if (a2 == null) {
      key = methodKey0Null;
    } else {
      key = methodKey0[a2.ordinal()];
    }
    final Object v = mq.map.get(r, key);
    if (v != null) {
      if (v == org.apache.calcite.rel.metadata.NullSentinel.ACTIVE) {
        throw new org.apache.calcite.rel.metadata.CyclicMetadataException();
      }
      if (v == org.apache.calcite.rel.metadata.NullSentinel.INSTANCE) {
        return null;
      }
      return (java.lang.Boolean) v;
    }
    mq.map.put(r, key,org.apache.calcite.rel.metadata.NullSentinel.ACTIVE);
    try {
      final java.lang.Boolean x = isVisibleInExplain_(r, mq, a2);
      mq.map.put(r, key, org.apache.calcite.rel.metadata.NullSentinel.mask(x));
      return x;
    } catch (java.lang.Exception e) {
      mq.map.row(r).clear();
      throw e;
    }
  }

  private java.lang.Boolean isVisibleInExplain_(
      org.apache.calcite.rel.RelNode r,
      org.apache.calcite.rel.metadata.RelMetadataQuery mq,
      org.apache.calcite.sql.SqlExplainLevel a2) {
    if (r instanceof org.apache.calcite.rel.core.TableScan) {
      return provider0.isVisibleInExplain((org.apache.calcite.rel.core.TableScan) r, mq, a2);
    } else if (r instanceof org.apache.calcite.rel.RelNode) {
      return provider0.isVisibleInExplain((org.apache.calcite.rel.RelNode) r, mq, a2);
    } else {
            throw new java.lang.IllegalArgumentException("No handler for method [public abstract java.lang.Boolean org.apache.calcite.rel.metadata.BuiltInMetadata$ExplainVisibility$Handler.isVisibleInExplain(org.apache.calcite.rel.RelNode,org.apache.calcite.rel.metadata.RelMetadataQuery,org.apache.calcite.sql.SqlExplainLevel)] applied to argument of type [" + r.getClass() + "]; we recommend you create a catch-all (RelNode) handler");
    }
  }

}
