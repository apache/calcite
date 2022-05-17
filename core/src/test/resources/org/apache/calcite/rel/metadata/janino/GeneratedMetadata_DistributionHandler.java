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

public final class GeneratedMetadata_DistributionHandler
  implements org.apache.calcite.rel.metadata.BuiltInMetadata.Distribution.Handler {
  private final Object methodKey0 =
      new org.apache.calcite.rel.metadata.janino.DescriptiveCacheKey("RelDistribution Handler.distribution()");
  public final org.apache.calcite.rel.metadata.RelMdDistribution provider0;
  public GeneratedMetadata_DistributionHandler(
      org.apache.calcite.rel.metadata.RelMdDistribution provider0) {
    this.provider0 = provider0;
  }
  public org.apache.calcite.rel.metadata.MetadataDef getDef() {
    return provider0.getDef();
  }
  public org.apache.calcite.rel.RelDistribution distribution(
      org.apache.calcite.rel.RelNode r,
      org.apache.calcite.rel.metadata.RelMetadataQuery mq) {
    while (r instanceof org.apache.calcite.rel.metadata.DelegatingMetadataRel) {
      r = ((org.apache.calcite.rel.metadata.DelegatingMetadataRel) r).getMetadataDelegateRel();
    }
    final Object key;
    key = methodKey0;
    final Object v = mq.map.get(r, key);
    if (v != null) {
      if (v == org.apache.calcite.rel.metadata.NullSentinel.ACTIVE) {
        throw new org.apache.calcite.rel.metadata.CyclicMetadataException();
      }
      if (v == org.apache.calcite.rel.metadata.NullSentinel.INSTANCE) {
        return null;
      }
      return (org.apache.calcite.rel.RelDistribution) v;
    }
    mq.map.put(r, key,org.apache.calcite.rel.metadata.NullSentinel.ACTIVE);
    try {
      final org.apache.calcite.rel.RelDistribution x = distribution_(r, mq);
      mq.map.put(r, key, org.apache.calcite.rel.metadata.NullSentinel.mask(x));
      return x;
    } catch (java.lang.Exception e) {
      mq.map.row(r).clear();
      throw e;
    }
  }

  private org.apache.calcite.rel.RelDistribution distribution_(
      org.apache.calcite.rel.RelNode r,
      org.apache.calcite.rel.metadata.RelMetadataQuery mq) {
    if (r instanceof org.apache.calcite.rel.BiRel) {
      return provider0.distribution((org.apache.calcite.rel.BiRel) r, mq);
    } else if (r instanceof org.apache.calcite.rel.core.Exchange) {
      return provider0.distribution((org.apache.calcite.rel.core.Exchange) r, mq);
    } else if (r instanceof org.apache.calcite.rel.core.Project) {
      return provider0.distribution((org.apache.calcite.rel.core.Project) r, mq);
    } else if (r instanceof org.apache.calcite.rel.core.SetOp) {
      return provider0.distribution((org.apache.calcite.rel.core.SetOp) r, mq);
    } else if (r instanceof org.apache.calcite.rel.core.TableModify) {
      return provider0.distribution((org.apache.calcite.rel.core.TableModify) r, mq);
    } else if (r instanceof org.apache.calcite.rel.core.TableScan) {
      return provider0.distribution((org.apache.calcite.rel.core.TableScan) r, mq);
    } else if (r instanceof org.apache.calcite.rel.core.Values) {
      return provider0.distribution((org.apache.calcite.rel.core.Values) r, mq);
    } else if (r instanceof org.apache.calcite.rel.SingleRel) {
      return provider0.distribution((org.apache.calcite.rel.SingleRel) r, mq);
    } else if (r instanceof org.apache.calcite.rel.RelNode) {
      return provider0.distribution((org.apache.calcite.rel.RelNode) r, mq);
    } else {
            throw new java.lang.IllegalArgumentException("No handler for method [public abstract org.apache.calcite.rel.RelDistribution org.apache.calcite.rel.metadata.BuiltInMetadata$Distribution$Handler.distribution(org.apache.calcite.rel.RelNode,org.apache.calcite.rel.metadata.RelMetadataQuery)] applied to argument of type [" + r.getClass() + "]; we recommend you create a catch-all (RelNode) handler");
    }
  }

}
