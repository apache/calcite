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
package org.eigenbase.rel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.eigenbase.relopt.RelImplementor;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.trace.EigenbaseTrace;

/**
 * Implementation of {@link RelImplementor}.
 */
public class RelImplementorImpl implements RelImplementor {
  protected static final Logger LOGGER =
      EigenbaseTrace.getRelImplementorTracer();

  /**
   * Maps a {@link String} to the {@link RelImplementorImpl.Frame} whose
   * {@link Frame#rel}.correlVariable == correlName.
   */
  protected final Map<String, Frame> mapCorrel2Frame =
      new HashMap<String, Frame>();

  /**
   * Maps a {@link org.eigenbase.rel.RelNode} to the unique frame whose
   * {@link RelImplementorImpl.Frame#rel} is
   * that relational expression.
   */
  protected final Map<RelNode, Frame> mapRel2Frame =
      new HashMap<RelNode, Frame>();

  protected final RexBuilder rexBuilder;

  public RelImplementorImpl(RexBuilder rexBuilder) {
    this.rexBuilder = rexBuilder;
  }

  public RexBuilder getRexBuilder() {
    return rexBuilder;
  }

  public RelDataTypeFactory getTypeFactory() {
    return rexBuilder.getTypeFactory();
  }

  public final Object visitChild(
      RelNode parent,
      int ordinal,
      RelNode child) {
    if (parent != null) {
      assert child == parent.getInputs().get(ordinal);
    }
    createFrame(parent, ordinal, child);
    return visitChildInternal(child, ordinal, null);
  }

  protected void createFrame(RelNode parent, int ordinal, RelNode child) {
    Frame frame = new Frame(child, parent, ordinal);
    mapRel2Frame.put(child, frame);
    String correl = child.getCorrelVariable();
    if (correl != null) {
      // Record that this frame is responsible for setting this
      // variable. But if another frame is already doing the job --
      // this frame's parent, which belongs to the same set -- don't
      // override it.
      if (mapCorrel2Frame.get(correl) == null) {
        mapCorrel2Frame.put(correl, frame);
      }
    }
  }

  public Object visitChildInternal(RelNode child, int ordinal, Object arg) {
    throw new UnsupportedOperationException();
  }

  protected RelNode findInputRel(
      RelNode rel, int offset) {
    return findInputRel(
        rel,
        offset,
        new int[]{0});
  }

  private RelNode findInputRel(
      RelNode rel,
      int offset,
      int[] offsets) {
    if (rel instanceof JoinRelBase) {
      // no variable here -- go deeper
      List<RelNode> inputs = rel.getInputs();
      for (RelNode input : inputs) {
        RelNode result = findInputRel(input, offset, offsets);
        if (result != null) {
          return result;
        }
      }
    } else if (offset == offsets[0]) {
      return rel;
    } else {
      offsets[0]++;
    }
    return null; // not found
  }

  /**
   * Returns a list of the relational expressions which are ancestors of the
   * current one.
   */
  public List<RelNode> getAncestorRels(RelNode rel) {
    final List<RelNode> ancestorList = new ArrayList<RelNode>();
    Frame frame = mapRel2Frame.get(rel);
    assert frame != null : "rel must be on the current implementation stack";
    while (true) {
      ancestorList.add(frame.rel);
      final RelNode parentRel = frame.parent;
      if (parentRel == null) {
        break;
      }
      frame = mapRel2Frame.get(parentRel);
      assert frame != null : "ancestor rel must have frame";
    }
    return ancestorList;
  }

  /** Information about a call from a parent relational expression
   * to implement one of its input relational expressions. */
  protected static class Frame {
    /** Parent relational expression. */
    public final RelNode parent;

    /** Relational expression that is being implemented in this frame. */
    public final RelNode rel;

    /** Ordinal of {@code rel} within {@code parent}. */
    public final int ordinal;

    Frame(RelNode child, RelNode parent, int ordinal) {
      this.rel = child;
      this.parent = parent;
      this.ordinal = ordinal;
    }
  }
}

// End RelImplementorImpl.java
