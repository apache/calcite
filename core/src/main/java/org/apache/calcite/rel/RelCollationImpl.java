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
package org.apache.calcite.rel;

import org.apache.calcite.plan.RelMultipleTrait;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.Utilities;
import org.apache.calcite.util.Util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.UnmodifiableIterator;

import java.util.Iterator;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * Simple implementation of {@link RelCollation}.
 */
public class RelCollationImpl implements RelCollation {
  //~ Static fields/initializers ---------------------------------------------

  @Deprecated // to be removed before 2.0
  public static final RelCollation EMPTY = RelCollations.EMPTY;

  @Deprecated // to be removed before 2.0
  public static final RelCollation PRESERVE = RelCollations.PRESERVE;

  //~ Instance fields --------------------------------------------------------

  private final ImmutableList<RelFieldCollation> fieldCollations;

  //~ Constructors -----------------------------------------------------------

  protected RelCollationImpl(ImmutableList<RelFieldCollation> fieldCollations) {
    this.fieldCollations = fieldCollations;
    Preconditions.checkArgument(
        Util.isDistinct(RelCollations.ordinals(fieldCollations)),
        "fields must be distinct");
  }

  @Deprecated // to be removed before 2.0
  public static RelCollation of(RelFieldCollation... fieldCollations) {
    return RelCollations.of(fieldCollations);
  }

  @Deprecated // to be removed before 2.0
  public static RelCollation of(List<RelFieldCollation> fieldCollations) {
    return RelCollations.of(fieldCollations);
  }

  //~ Methods ----------------------------------------------------------------

  public RelTraitDef getTraitDef() {
    return RelCollationTraitDef.INSTANCE;
  }

  public List<RelFieldCollation> getFieldCollations() {
    return fieldCollations;
  }

  public int hashCode() {
    return fieldCollations.hashCode();
  }

  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof RelCollationImpl) {
      RelCollationImpl that = (RelCollationImpl) obj;
      return this.fieldCollations.equals(that.fieldCollations);
    }
    return false;
  }

  public boolean isTop() {
    return fieldCollations.isEmpty();
  }

  public int compareTo(@Nonnull RelMultipleTrait o) {
    final RelCollationImpl that = (RelCollationImpl) o;
    final UnmodifiableIterator<RelFieldCollation> iterator =
        that.fieldCollations.iterator();
    for (RelFieldCollation f : fieldCollations) {
      if (!iterator.hasNext()) {
        return 1;
      }
      final RelFieldCollation f2 = iterator.next();
      int c = Utilities.compare(f.getFieldIndex(), f2.getFieldIndex());
      if (c != 0) {
        return c;
      }
    }
    return iterator.hasNext() ? -1 : 0;
  }

  public void register(RelOptPlanner planner) {}

  public boolean satisfies(RelTrait trait) {
    return this == trait
        || trait instanceof RelCollationImpl
        && Util.startsWith(fieldCollations,
            ((RelCollationImpl) trait).fieldCollations);
  }

  /** Returns a string representation of this collation, suitably terse given
   * that it will appear in plan traces. Examples:
   * "[]", "[2]", "[0 DESC, 1]", "[0 DESC, 1 ASC NULLS LAST]". */
  public String toString() {
    Iterator<RelFieldCollation> it = fieldCollations.iterator();
    if (! it.hasNext()) {
      return "[]";
    }
    StringBuilder sb = new StringBuilder();
    sb.append('[');
    for (;;) {
      RelFieldCollation e = it.next();
      sb.append(e.getFieldIndex());
      if (e.direction != RelFieldCollation.Direction.ASCENDING
          || e.nullDirection != e.direction.defaultNullDirection()) {
        sb.append(' ').append(e.shortString());
      }
      if (!it.hasNext()) {
        return sb.append(']').toString();
      }
      sb.append(',').append(' ');
    }
  }

  @Deprecated // to be removed before 2.0
  public static List<RelCollation> createSingleton(int fieldIndex) {
    return RelCollations.createSingleton(fieldIndex);
  }

  @Deprecated // to be removed before 2.0
  public static boolean isValid(
      RelDataType rowType,
      List<RelCollation> collationList,
      boolean fail) {
    return RelCollations.isValid(rowType, collationList, fail);
  }

  @Deprecated // to be removed before 2.0
  public static boolean equal(
      List<RelCollation> collationList1,
      List<RelCollation> collationList2) {
    return RelCollations.equal(collationList1, collationList2);
  }

  @Deprecated // to be removed before 2.0
  public static List<Integer> ordinals(RelCollation collation) {
    return RelCollations.ordinals(collation);
  }
}

// End RelCollationImpl.java
