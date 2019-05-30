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
package org.apache.calcite.rel.core;

import org.apache.calcite.linq4j.function.Experimental;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;

import java.util.List;
import java.util.Objects;

/**
 * Relational expression that iterates over its input and, in addition to
 * returning its results, will forward them into other consumers.
 *
 * <p>NOTE: The current API is experimental and subject to change without
 * notice.
 */
@Experimental
public abstract class Spool extends SingleRel {

  /**
   * Enumeration representing spool read / write type.
   */
  public enum Type {
    EAGER,
    LAZY
  }

  /**
   * How the spool consumes elements from its input.
   *
   * <ul>
   * <li>EAGER: the spool consumes the elements from its input at once at the
   *     initial request;
   * <li>LAZY: the spool consumes the elements from its input one by one by
   *     request.
   * </ul>
   */
  public final Type readType;

  /**
   * How the spool forwards elements to consumers.
   *
   * <ul>
   * <li>EAGER: the spool forwards each element as soon as it returns it;
   * <li>LAZY: the spool forwards all elements at once when it is done returning
   *     all of them.
   * </ul>
   */
  public final Type writeType;

  //~ Constructors -----------------------------------------------------------

  /** Creates a Spool. */
  protected Spool(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
      Type readType, Type writeType) {
    super(cluster, traitSet, input);
    this.readType = Objects.requireNonNull(readType);
    this.writeType = Objects.requireNonNull(writeType);
  }

  @Override public final RelNode copy(RelTraitSet traitSet,
      List<RelNode> inputs) {
    return copy(traitSet, sole(inputs), readType, writeType);
  }

  protected abstract Spool copy(RelTraitSet traitSet, RelNode input,
      Type readType, Type writeType);

  @Override public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("readType", readType)
        .item("writeType", writeType);
  }
}

// End Spool.java
