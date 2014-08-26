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
package org.eigenbase.relopt.hep;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;

import com.google.common.base.Function;

/**
 * HepRelMetadataProvider implements the {@link RelMetadataProvider} interface
 * by combining metadata from the rels inside of a {@link HepRelVertex}.
 */
class HepRelMetadataProvider implements RelMetadataProvider {
  //~ Methods ----------------------------------------------------------------

  public Function<RelNode, Metadata> apply(Class<? extends RelNode> relClass,
      final Class<? extends Metadata> metadataClass) {
    return new Function<RelNode, Metadata>() {
      public Metadata apply(RelNode rel) {
        if (!(rel instanceof HepRelVertex)) {
          return null;
        }

        HepRelVertex vertex = (HepRelVertex) rel;
        final RelNode rel2 = vertex.getCurrentRel();
        Function<RelNode, Metadata> function =
            rel.getCluster().getMetadataProvider().apply(
                rel2.getClass(), metadataClass);
        return function.apply(rel2);
      }
    };
  }
}

// End HepRelMetadataProvider.java
