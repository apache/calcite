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

public final class GeneratedMetadata_MemoryHandler
  implements org.apache.calcite.rel.metadata.BuiltInMetadata.Memory.Handler {
  private final Object methodKey0 =
      new org.apache.calcite.rel.metadata.janino.DescriptiveCacheKey("Double Handler.cumulativeMemoryWithinPhase()");
  private final Object methodKey1 =
      new org.apache.calcite.rel.metadata.janino.DescriptiveCacheKey("Double Handler.cumulativeMemoryWithinPhaseSplit()");
  private final Object methodKey2 =
      new org.apache.calcite.rel.metadata.janino.DescriptiveCacheKey("Double Handler.memory()");
  public final org.apache.calcite.rel.metadata.RelMdMemory provider1;
  public GeneratedMetadata_MemoryHandler(
      org.apache.calcite.rel.metadata.RelMdMemory provider1) {
    this.provider1 = provider1;
  }
  public org.apache.calcite.rel.metadata.MetadataDef getDef() {
    return provider1.getDef();
  }
  public java.lang.Double cumulativeMemoryWithinPhase(
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
      return (java.lang.Double) v;
    }
    mq.map.put(r, key,org.apache.calcite.rel.metadata.NullSentinel.ACTIVE);
    try {
      final java.lang.Double x = cumulativeMemoryWithinPhase_(r, mq);
      mq.map.put(r, key, org.apache.calcite.rel.metadata.NullSentinel.mask(x));
      return x;
    } catch (java.lang.Exception e) {
      mq.map.row(r).clear();
      throw e;
    }
  }

  private java.lang.Double cumulativeMemoryWithinPhase_(
      org.apache.calcite.rel.RelNode r,
      org.apache.calcite.rel.metadata.RelMetadataQuery mq) {
    if (r instanceof org.apache.calcite.rel.RelNode) {
      return provider1.cumulativeMemoryWithinPhase((org.apache.calcite.rel.RelNode) r, mq);
    } else {
            throw new java.lang.IllegalArgumentException("No handler for method [public abstract java.lang.Double org.apache.calcite.rel.metadata.BuiltInMetadata$Memory$Handler.cumulativeMemoryWithinPhase(org.apache.calcite.rel.RelNode,org.apache.calcite.rel.metadata.RelMetadataQuery)] applied to argument of type [" + r.getClass() + "]; we recommend you create a catch-all (RelNode) handler");
    }
  }
  public java.lang.Double cumulativeMemoryWithinPhaseSplit(
      org.apache.calcite.rel.RelNode r,
      org.apache.calcite.rel.metadata.RelMetadataQuery mq) {
    while (r instanceof org.apache.calcite.rel.metadata.DelegatingMetadataRel) {
      r = ((org.apache.calcite.rel.metadata.DelegatingMetadataRel) r).getMetadataDelegateRel();
    }
    final Object key;
    key = methodKey1;
    final Object v = mq.map.get(r, key);
    if (v != null) {
      if (v == org.apache.calcite.rel.metadata.NullSentinel.ACTIVE) {
        throw new org.apache.calcite.rel.metadata.CyclicMetadataException();
      }
      if (v == org.apache.calcite.rel.metadata.NullSentinel.INSTANCE) {
        return null;
      }
      return (java.lang.Double) v;
    }
    mq.map.put(r, key,org.apache.calcite.rel.metadata.NullSentinel.ACTIVE);
    try {
      final java.lang.Double x = cumulativeMemoryWithinPhaseSplit_(r, mq);
      mq.map.put(r, key, org.apache.calcite.rel.metadata.NullSentinel.mask(x));
      return x;
    } catch (java.lang.Exception e) {
      mq.map.row(r).clear();
      throw e;
    }
  }

  private java.lang.Double cumulativeMemoryWithinPhaseSplit_(
      org.apache.calcite.rel.RelNode r,
      org.apache.calcite.rel.metadata.RelMetadataQuery mq) {
    if (r instanceof org.apache.calcite.rel.RelNode) {
      return provider1.cumulativeMemoryWithinPhaseSplit((org.apache.calcite.rel.RelNode) r, mq);
    } else {
            throw new java.lang.IllegalArgumentException("No handler for method [public abstract java.lang.Double org.apache.calcite.rel.metadata.BuiltInMetadata$Memory$Handler.cumulativeMemoryWithinPhaseSplit(org.apache.calcite.rel.RelNode,org.apache.calcite.rel.metadata.RelMetadataQuery)] applied to argument of type [" + r.getClass() + "]; we recommend you create a catch-all (RelNode) handler");
    }
  }
  public java.lang.Double memory(
      org.apache.calcite.rel.RelNode r,
      org.apache.calcite.rel.metadata.RelMetadataQuery mq) {
    while (r instanceof org.apache.calcite.rel.metadata.DelegatingMetadataRel) {
      r = ((org.apache.calcite.rel.metadata.DelegatingMetadataRel) r).getMetadataDelegateRel();
    }
    final Object key;
    key = methodKey2;
    final Object v = mq.map.get(r, key);
    if (v != null) {
      if (v == org.apache.calcite.rel.metadata.NullSentinel.ACTIVE) {
        throw new org.apache.calcite.rel.metadata.CyclicMetadataException();
      }
      if (v == org.apache.calcite.rel.metadata.NullSentinel.INSTANCE) {
        return null;
      }
      return (java.lang.Double) v;
    }
    mq.map.put(r, key,org.apache.calcite.rel.metadata.NullSentinel.ACTIVE);
    try {
      final java.lang.Double x = memory_(r, mq);
      mq.map.put(r, key, org.apache.calcite.rel.metadata.NullSentinel.mask(x));
      return x;
    } catch (java.lang.Exception e) {
      mq.map.row(r).clear();
      throw e;
    }
  }

  private java.lang.Double memory_(
      org.apache.calcite.rel.RelNode r,
      org.apache.calcite.rel.metadata.RelMetadataQuery mq) {
    if (r instanceof org.apache.calcite.rel.RelNode) {
      return provider1.memory((org.apache.calcite.rel.RelNode) r, mq);
    } else {
            throw new java.lang.IllegalArgumentException("No handler for method [public abstract java.lang.Double org.apache.calcite.rel.metadata.BuiltInMetadata$Memory$Handler.memory(org.apache.calcite.rel.RelNode,org.apache.calcite.rel.metadata.RelMetadataQuery)] applied to argument of type [" + r.getClass() + "]; we recommend you create a catch-all (RelNode) handler");
    }
  }

}
