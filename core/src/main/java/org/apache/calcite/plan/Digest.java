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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * A short description of relational expression's type, inputs, and
 * other properties. The digest uniquely identifies the node; another node
 * is equivalent if and only if it has the same value.
 *
 * <p>Row type is part of the digest for the rare occasion that similar
 * expressions have different types, e.g. variants of
 * {@code Project(child=rel#1, a=null)} where a is a null INTEGER or a
 * null VARCHAR(10). Row type is represented as fieldTypes only, so {@code RelNode}
 * that differ with field names only are treated equal.
 * For instance, {@code Project(input=rel#1,empid=$0)} and {@code Project(input=rel#1,deptno=$0)}
 * are equal.
 *
 * <p>Computed by {@code org.apache.calcite.rel.AbstractRelNode#computeDigest},
 * assigned by {@link org.apache.calcite.rel.AbstractRelNode#onRegister},
 * returned by {@link org.apache.calcite.rel.AbstractRelNode#getDigest()}.
 */
public class Digest implements Comparable<Digest> {

  //~ Instance fields --------------------------------------------------------

  private final int hashCode;
  private final List<Pair<String, Object>> items;
  private final RelNode rel;

  // Used for debugging, computed lazily.
  private String digest = null;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a digest with given rel and properties.
   *
   * @param rel   The rel
   * @param items The properties, e.g. the inputs, the type, the traits and so on
   */
  private Digest(RelNode rel, List<Pair<String, Object>> items) {
    this.rel = rel;
    this.items = normalizeContents(items);
    this.hashCode = computeIdentity(rel, this.items);
  }

  /**
   * Creates a digest with given rel, the digest is computed as simple,
   * see {@link #simpleRelDigest(RelNode)}.
   */
  private Digest(RelNode rel) {
    this(rel, simpleRelDigest(rel));
  }

  /** Creates a digest with given rel and string format digest. */
  private Digest(RelNode rel, String digest) {
    this.rel = rel;
    this.items = Collections.emptyList();
    this.digest = digest;
    this.hashCode = this.digest.hashCode();
  }

  /** Returns the identity of this digest which is used to speedup hashCode and equals. */
  private static int computeIdentity(RelNode rel, List<Pair<String, Object>> contents) {
    return Objects.hash(collect(rel, contents, false));
  }

  /**
   * Collects the items used for {@link #hashCode} and {@link #equals}.
   *
   * <p>Generally, the items used for hashCode and equals should be the same. The exception
   * is the row type of the relational expression: the row type is needed because during
   * planning, new equivalent rels may be produced with changed fields nullability
   * (i.e. most of them comes from the rex simplify or constant reduction).
   * This expects to be rare case, so the hashcode is computed without row type
   * but when it conflicts, we compare with the row type involved(sans field names).
   *
   * @param rel      The rel to compute digest
   * @param contents The rel properties should be considered in digest
   * @param withType Whether to involve the row type
   */
  private static Object[] collect(
      RelNode rel,
      List<Pair<String, Object>> contents,
      boolean withType) {
    List<Object> hashCodeItems = new ArrayList<>();
    // The type name.
    hashCodeItems.add(rel.getRelTypeName());
    // The traits.
    hashCodeItems.addAll(rel.getTraitSet());
    // The hints.
    if (rel instanceof Hintable) {
      hashCodeItems.addAll(((Hintable) rel).getHints());
    }
    if (withType) {
      // The row type sans field names.
      RelDataType relType = rel.getRowType();
      if (relType.isStruct()) {
        hashCodeItems.addAll(Pair.right(relType.getFieldList()));
      } else {
        // Make a decision here because
        // some downstream projects have custom rel type which has no explicit fields.
        hashCodeItems.add(relType);
      }
    }
    // The rel node contents(e.g. the inputs or exprs).
    hashCodeItems.addAll(contents);
    return hashCodeItems.toArray();
  }

  /** Normalizes the rel node properties, currently, just to replace the
   * {@link RelNode} with a simple string format digest. **/
  private static List<Pair<String, Object>> normalizeContents(
      List<Pair<String, Object>> items) {
    List<Pair<String, Object>> normalized = new ArrayList<>();
    for (Pair<String, Object> item : items) {
      if (item.right instanceof RelNode) {
        RelNode input = (RelNode) item.right;
        normalized.add(Pair.of(item.left, simpleRelDigest(input)));
      } else {
        normalized.add(item);
      }
    }
    return normalized;
  }

  /**
   * Returns a simple string format digest.
   *
   * <p>Currently, returns composition of class name and id.
   *
   * @param rel The rel
   */
  private static String simpleRelDigest(RelNode rel) {
    return rel.getRelTypeName() + '#' + rel.getId();
  }

  @Override public String toString() {
    if (null != digest) {
      return digest;
    }
    StringBuilder sb = new StringBuilder();
    sb.append(rel.getRelTypeName());

    for (RelTrait trait : rel.getTraitSet()) {
      sb.append('.');
      sb.append(trait.toString());
    }

    sb.append('(');
    int j = 0;
    for (Pair<String, Object> item : items) {
      if (j++ > 0) {
        sb.append(',');
      }
      sb.append(item.left);
      sb.append('=');
      sb.append(item.right);
    }
    sb.append(')');
    digest = sb.toString();
    return digest;
  }

  @Override public int compareTo(@Nonnull Digest other) {
    return this.equals(other) ? 0 : this.rel.getId() - other.rel.getId();
  }

  @Override public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Digest that = (Digest) o;
    return hashCode == that.hashCode && deepEquals(that);
  }

  /**
   * The method is used to resolve hash conflict, in current 6000+ tests, there are about 8
   * tests with conflict, so we do not cache the hash code items in order to
   * reduce mem consumption.
   */
  private boolean deepEquals(Digest other) {
    Object[] thisItems = collect(this.rel, this.items, true);
    Object[] thatItems = collect(other.rel, other.items, true);
    if (thisItems.length != thatItems.length) {
      return false;
    }
    for (int i = 0; i < thisItems.length; i++) {
      if (!Objects.equals(thisItems[i], thatItems[i])) {
        return false;
      }
    }
    return true;
  }

  @Override public int hashCode() {
    return hashCode;
  }

  /**
   * Creates a digest with given rel and properties.
   */
  public static Digest create(RelNode rel, List<Pair<String, Object>> contents) {
    return new Digest(rel, contents);
  }

  /**
   * Creates a digest with given rel.
   */
  public static Digest create(RelNode rel) {
    return new Digest(rel);
  }

  /**
   * Creates a digest with given rel and string format digest
   */
  public static Digest create(RelNode rel, String digest) {
    return new Digest(rel, digest);
  }

  /**
   * Instantiates a digest with solid string format digest, this digest should only
   * be used as a initial.
   */
  public static Digest initial(RelNode rel) {
    return new Digest(rel, simpleRelDigest(rel));
  }
}
