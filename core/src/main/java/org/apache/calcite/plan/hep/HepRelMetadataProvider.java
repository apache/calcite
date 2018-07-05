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
package org.apache.calcite.plan.hep;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.UnboundMetadata;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;

import java.lang.reflect.Method;

/**
 * HepRelMetadataProvider implements the {@link RelMetadataProvider} interface
 * by combining metadata from the rels inside of a {@link HepRelVertex}.
 */
class HepRelMetadataProvider implements RelMetadataProvider {
  //~ Methods ----------------------------------------------------------------

  @Override public boolean equals(Object obj) {
    return obj instanceof HepRelMetadataProvider;
  }

  @Override public int hashCode() {
    return 107;
  }

  public <M extends Metadata> UnboundMetadata<M> apply(
      Class<? extends RelNode> relClass,
      final Class<? extends M> metadataClass) {
    return (rel, mq) -> {
      if (!(rel instanceof HepRelVertex)) {
        return null;
      }
      HepRelVertex vertex = (HepRelVertex) rel;
      final RelNode rel2 = vertex.getCurrentRel();
      UnboundMetadata<M> function =
          rel.getCluster().getMetadataProvider().apply(rel2.getClass(),
              metadataClass);
      return function.bind(rel2, mq);
    };
  }

  public <M extends Metadata> Multimap<Method, MetadataHandler<M>> handlers(
      MetadataDef<M> def) {
    return ImmutableMultimap.of();
  }
}

// End HepRelMetadataProvider.java
