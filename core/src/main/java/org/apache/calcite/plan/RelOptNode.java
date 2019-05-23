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
package org.apache.calcite.plan;

import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

/**
 * Node in a planner.
 */
public interface RelOptNode {
  /**
   * Returns the ID of this relational expression, unique among all relational
   * expressions created since the server was started.
   *
   * @return Unique ID
   */
  int getId();

  /**
   * Returns a string which concisely describes the definition of this
   * relational expression. Two relational expressions are equivalent if
   * their digests and {@link #getRowType()} are the same.
   *
   * <p>The digest does not contain the relational expression's identity --
   * that would prevent similar relational expressions from ever comparing
   * equal -- but does include the identity of children (on the assumption
   * that children have already been normalized).
   *
   * <p>If you want a descriptive string which contains the identity, call
   * {@link Object#toString()}, which always returns "rel#{id}:{digest}".
   *
   * @return Digest of this {@code RelNode}
   */
  String getDigest();

  /**
   * Retrieves this RelNode's traits. Note that although the RelTraitSet
   * returned is modifiable, it <b>must not</b> be modified during
   * optimization. It is legal to modify the traits of a RelNode before or
   * after optimization, although doing so could render a tree of RelNodes
   * unimplementable. If a RelNode's traits need to be modified during
   * optimization, clone the RelNode and change the clone's traits.
   *
   * @return this RelNode's trait set
   */
  RelTraitSet getTraitSet();

  // TODO: We don't want to require that nodes have very detailed row type. It
  // may not even be known at planning time.
  RelDataType getRowType();

  /**
   * Returns a string which describes the relational expression and, unlike
   * {@link #getDigest()}, also includes the identity. Typically returns
   * "rel#{id}:{digest}".
   *
   * @return String which describes the relational expression and, unlike
   *   {@link #getDigest()}, also includes the identity
   */
  String getDescription();

  /**
   * Returns an array of this relational expression's inputs. If there are no
   * inputs, returns an empty list, not {@code null}.
   *
   * @return Array of this relational expression's inputs
   */
  List<? extends RelOptNode> getInputs();

  /**
   * Returns the cluster this relational expression belongs to.
   *
   * @return cluster
   */
  RelOptCluster getCluster();
}

// End RelOptNode.java
