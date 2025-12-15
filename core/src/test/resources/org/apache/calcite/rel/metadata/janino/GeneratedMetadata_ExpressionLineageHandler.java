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

public final class GeneratedMetadata_ExpressionLineageHandler
  implements org.apache.calcite.rel.metadata.BuiltInMetadata.ExpressionLineage.Handler {
  private final Object methodKey0 =
      new org.apache.calcite.rel.metadata.janino.DescriptiveCacheKey("Set Handler.getExpressionLineage(RelNode, RelMetadataQuery, RexNode)");
  public final org.apache.calcite.rel.metadata.RelMdExpressionLineage provider0;
  public GeneratedMetadata_ExpressionLineageHandler(
      org.apache.calcite.rel.metadata.RelMdExpressionLineage provider0) {
    this.provider0 = provider0;
  }
  public org.apache.calcite.rel.metadata.MetadataDef getDef() {
    return provider0.getDef();
  }
  public java.util.Set getExpressionLineage(
      org.apache.calcite.rel.RelNode r,
      org.apache.calcite.rel.metadata.RelMetadataQuery mq,
      org.apache.calcite.rex.RexNode a2) {
    while (r instanceof org.apache.calcite.rel.metadata.DelegatingMetadataRel) {
      r = ((org.apache.calcite.rel.metadata.DelegatingMetadataRel) r).getMetadataDelegateRel();
    }
    final Object key;
    key = org.apache.calcite.runtime.FlatLists.of(methodKey0, a2);
    final Object v = mq.map.get(r, key);
    if (v != null) {
      if (v == org.apache.calcite.rel.metadata.NullSentinel.ACTIVE) {
        throw new org.apache.calcite.rel.metadata.CyclicMetadataException();
      }
      if (v == org.apache.calcite.rel.metadata.NullSentinel.INSTANCE) {
        return null;
      }
      return (java.util.Set) v;
    }
    mq.map.put(r, key,org.apache.calcite.rel.metadata.NullSentinel.ACTIVE);
    try {
      final java.util.Set x = getExpressionLineage_(r, mq, a2);
      mq.map.put(r, key, org.apache.calcite.rel.metadata.NullSentinel.mask(x));
      return x;
    } catch (java.lang.Exception e) {
      mq.map.row(r).clear();
      throw e;
    }
  }

  private java.util.Set getExpressionLineage_(
      org.apache.calcite.rel.RelNode r,
      org.apache.calcite.rel.metadata.RelMetadataQuery mq,
      org.apache.calcite.rex.RexNode a2) {
    if (r instanceof org.apache.calcite.plan.volcano.RelSubset) {
      return provider0.getExpressionLineage((org.apache.calcite.plan.volcano.RelSubset) r, mq, a2);
    } else if (r instanceof org.apache.calcite.rel.core.Aggregate) {
      return provider0.getExpressionLineage((org.apache.calcite.rel.core.Aggregate) r, mq, a2);
    } else if (r instanceof org.apache.calcite.rel.core.Calc) {
      return provider0.getExpressionLineage((org.apache.calcite.rel.core.Calc) r, mq, a2);
    } else if (r instanceof org.apache.calcite.rel.core.Exchange) {
      return provider0.getExpressionLineage((org.apache.calcite.rel.core.Exchange) r, mq, a2);
    } else if (r instanceof org.apache.calcite.rel.core.Filter) {
      return provider0.getExpressionLineage((org.apache.calcite.rel.core.Filter) r, mq, a2);
    } else if (r instanceof org.apache.calcite.rel.core.Join) {
      return provider0.getExpressionLineage((org.apache.calcite.rel.core.Join) r, mq, a2);
    } else if (r instanceof org.apache.calcite.rel.core.Project) {
      return provider0.getExpressionLineage((org.apache.calcite.rel.core.Project) r, mq, a2);
    } else if (r instanceof org.apache.calcite.rel.core.Sample) {
      return provider0.getExpressionLineage((org.apache.calcite.rel.core.Sample) r, mq, a2);
    } else if (r instanceof org.apache.calcite.rel.core.Snapshot) {
      return provider0.getExpressionLineage((org.apache.calcite.rel.core.Snapshot) r, mq, a2);
    } else if (r instanceof org.apache.calcite.rel.core.Sort) {
      return provider0.getExpressionLineage((org.apache.calcite.rel.core.Sort) r, mq, a2);
    } else if (r instanceof org.apache.calcite.rel.core.TableModify) {
      return provider0.getExpressionLineage((org.apache.calcite.rel.core.TableModify) r, mq, a2);
    } else if (r instanceof org.apache.calcite.rel.core.TableScan) {
      return provider0.getExpressionLineage((org.apache.calcite.rel.core.TableScan) r, mq, a2);
    } else if (r instanceof org.apache.calcite.rel.core.Union) {
      return provider0.getExpressionLineage((org.apache.calcite.rel.core.Union) r, mq, a2);
    } else if (r instanceof org.apache.calcite.rel.core.Values) {
      return provider0.getExpressionLineage((org.apache.calcite.rel.core.Values) r, mq, a2);
    } else if (r instanceof org.apache.calcite.rel.RelNode) {
      return provider0.getExpressionLineage((org.apache.calcite.rel.RelNode) r, mq, a2);
    } else {
            throw new java.lang.IllegalArgumentException("No handler for method [public abstract java.util.Set org.apache.calcite.rel.metadata.BuiltInMetadata$ExpressionLineage$Handler.getExpressionLineage(org.apache.calcite.rel.RelNode,org.apache.calcite.rel.metadata.RelMetadataQuery,org.apache.calcite.rex.RexNode)] applied to argument of type [" + r.getClass() + "]; we recommend you create a catch-all (RelNode) handler");
    }
  }

}
