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

import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Util;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

/**
 * RelMdNodeTypeCount supplies a default implementation of
 * {@link RelMetadataQuery#getNodeTypes} for the standard logical algebra.
 */
public class RelMdNodeTypes
    implements MetadataHandler<BuiltInMetadata.NodeTypes> {
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.NODE_TYPES.method, new RelMdNodeTypes());

  //~ Methods ----------------------------------------------------------------

  public MetadataDef<BuiltInMetadata.NodeTypes> getDef() {
    return BuiltInMetadata.NodeTypes.DEF;
  }

  /** Catch-all implementation for
   * {@link BuiltInMetadata.NodeTypes#getNodeTypes()},
   * invoked using reflection.
   *
   * @see org.apache.calcite.rel.metadata.RelMetadataQuery#getNodeTypes(RelNode)
   */
  public Multimap<Class<? extends RelNode>, RelNode> getNodeTypes(RelNode rel,
      RelMetadataQuery mq) {
    return getNodeTypes(rel, RelNode.class, mq);
  }

  public Multimap<Class<? extends RelNode>, RelNode> getNodeTypes(HepRelVertex rel,
      RelMetadataQuery mq) {
    return mq.getNodeTypes(rel.getCurrentRel());
  }

  public Multimap<Class<? extends RelNode>, RelNode> getNodeTypes(RelSubset rel,
      RelMetadataQuery mq) {
    return mq.getNodeTypes(Util.first(rel.getBest(), rel.getOriginal()));
  }

  public Multimap<Class<? extends RelNode>, RelNode> getNodeTypes(Union rel,
      RelMetadataQuery mq) {
    return getNodeTypes(rel, Union.class, mq);
  }

  public Multimap<Class<? extends RelNode>, RelNode> getNodeTypes(Intersect rel,
      RelMetadataQuery mq) {
    return getNodeTypes(rel, Intersect.class, mq);
  }

  public Multimap<Class<? extends RelNode>, RelNode> getNodeTypes(Minus rel,
      RelMetadataQuery mq) {
    return getNodeTypes(rel, Minus.class, mq);
  }

  public Multimap<Class<? extends RelNode>, RelNode> getNodeTypes(Filter rel,
      RelMetadataQuery mq) {
    return getNodeTypes(rel, Filter.class, mq);
  }

  public Multimap<Class<? extends RelNode>, RelNode> getNodeTypes(Calc rel,
      RelMetadataQuery mq) {
    return getNodeTypes(rel, Calc.class, mq);
  }

  public Multimap<Class<? extends RelNode>, RelNode> getNodeTypes(Project rel,
      RelMetadataQuery mq) {
    return getNodeTypes(rel, Project.class, mq);
  }

  public Multimap<Class<? extends RelNode>, RelNode> getNodeTypes(Sort rel,
      RelMetadataQuery mq) {
    return getNodeTypes(rel, Sort.class, mq);
  }

  public Multimap<Class<? extends RelNode>, RelNode> getNodeTypes(Join rel,
      RelMetadataQuery mq) {
    return getNodeTypes(rel, Join.class, mq);
  }

  public Multimap<Class<? extends RelNode>, RelNode> getNodeTypes(Aggregate rel,
      RelMetadataQuery mq) {
    return getNodeTypes(rel, Aggregate.class, mq);
  }

  public Multimap<Class<? extends RelNode>, RelNode> getNodeTypes(TableScan rel,
      RelMetadataQuery mq) {
    return getNodeTypes(rel, TableScan.class, mq);
  }

  public Multimap<Class<? extends RelNode>, RelNode> getNodeTypes(Values rel,
      RelMetadataQuery mq) {
    return getNodeTypes(rel, Values.class, mq);
  }

  private static Multimap<Class<? extends RelNode>, RelNode> getNodeTypes(RelNode rel,
      Class<? extends RelNode> c, RelMetadataQuery mq) {
    final Multimap<Class<? extends RelNode>, RelNode> nodeTypeCount = ArrayListMultimap.create();
    for (RelNode input : rel.getInputs()) {
      Multimap<Class<? extends RelNode>, RelNode> partialNodeTypeCount =
          mq.getNodeTypes(input);
      if (partialNodeTypeCount == null) {
        return null;
      }
      nodeTypeCount.putAll(partialNodeTypeCount);
    }
    nodeTypeCount.put(c, rel);
    return nodeTypeCount;
  }

}

// End RelMdNodeTypes.java
