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
package org.apache.calcite.rel.mutable;

import org.apache.calcite.avatica.util.Spaces;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

import com.google.common.base.Equivalence;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Objects;

/** Mutable equivalent of {@link RelNode}.
 *
 * <p>Each node has mutable state, and keeps track of its parent and position
 * within parent.
 * It doesn't make sense to canonize {@code MutableRels},
 * otherwise one node could end up with multiple parents.
 * It follows that {@code #hashCode} and {@code #equals} are less efficient
 * than their {@code RelNode} counterparts.
 * But, you don't need to copy a {@code MutableRel} in order to change it.
 * For this reason, you should use {@code MutableRel} for short-lived
 * operations, and transcribe back to {@code RelNode} when you are done.</p>
 */
public abstract class MutableRel {

  /** Equivalence that compares objects by their {@link Object#toString()}
   * method. */
  protected static final Equivalence<Object> STRING_EQUIVALENCE =
      new Equivalence<Object>() {
        @Override protected boolean doEquivalent(Object o, Object o2) {
          return o.toString().equals(o2.toString());
        }

        @Override protected int doHash(Object o) {
          return o.toString().hashCode();
        }
      };

  /** Equivalence that compares {@link Lists}s by the
   * {@link Object#toString()} of their elements. */
  @SuppressWarnings("unchecked")
  protected static final Equivalence<List<?>> PAIRWISE_STRING_EQUIVALENCE =
      (Equivalence) STRING_EQUIVALENCE.pairwise();

  public final RelOptCluster cluster;
  public final RelDataType rowType;
  protected final MutableRelType type;

  protected MutableRel parent;
  protected int ordinalInParent;

  protected MutableRel(RelOptCluster cluster,
      RelDataType rowType, MutableRelType type) {
    this.cluster = Objects.requireNonNull(cluster);
    this.rowType = Objects.requireNonNull(rowType);
    this.type = Objects.requireNonNull(type);
  }

  public MutableRel getParent() {
    return parent;
  }

  public abstract void setInput(int ordinalInParent, MutableRel input);

  public abstract List<MutableRel> getInputs();

  public abstract MutableRel clone();

  public abstract void childrenAccept(MutableRelVisitor visitor);

  /** Replaces this {@code MutableRel} in its parent with another node at the
   * same position.
   *
   * <p>Before the method, {@code child} must be an orphan (have null parent)
   * and after this method, this {@code MutableRel} is an orphan.
   *
   * @return The parent
   */
  public MutableRel replaceInParent(MutableRel child) {
    final MutableRel parent = this.parent;
    if (this != child) {
      if (parent != null) {
        parent.setInput(ordinalInParent, child);
        this.parent = null;
        this.ordinalInParent = 0;
      }
    }
    return parent;
  }

  public abstract StringBuilder digest(StringBuilder buf);

  public final String deep() {
    return new MutableRelDumper().apply(this);
  }

  @Override public final String toString() {
    return deep();
  }

  /**
   * Implementation of MutableVisitor that dumps the details
   * of a MutableRel tree.
   */
  private class MutableRelDumper extends MutableRelVisitor {
    private final StringBuilder buf = new StringBuilder();
    private int level;

    @Override public void visit(MutableRel node) {
      Spaces.append(buf, level * 2);
      if (node == null) {
        buf.append("null");
      } else {
        node.digest(buf);
        buf.append("\n");
        ++level;
        super.visit(node);
        --level;
      }
    }

    public String apply(MutableRel rel) {
      go(rel);
      return buf.toString();
    }
  }

}

// End MutableRel.java
