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

import java.util.*;

import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTrait;
import org.eigenbase.relopt.RelTraitDef;
import org.eigenbase.reltype.*;
import org.eigenbase.util.Util;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Simple implementation of {@link RelCollation}.
 */
public class RelCollationImpl implements RelCollation {
  //~ Static fields/initializers ---------------------------------------------

  /**
   * A collation indicating that a relation is not sorted. Ordering by no
   * columns.
   */
  public static final RelCollation EMPTY =
      RelCollationTraitDef.INSTANCE.canonize(
          new RelCollationImpl(ImmutableList.<RelFieldCollation>of()));

  /**
   * A collation that cannot be replicated by applying a sort. The only
   * implementation choice is to apply operations that preserve order.
   */
  public static final RelCollation PRESERVE =
      RelCollationTraitDef.INSTANCE.canonize(
          new RelCollationImpl(
              ImmutableList.of(new RelFieldCollation(-1))) {
            public String toString() {
              return "PRESERVE";
            }
          });

  //~ Instance fields --------------------------------------------------------

  private final ImmutableList<RelFieldCollation> fieldCollations;

  //~ Constructors -----------------------------------------------------------

  protected RelCollationImpl(ImmutableList<RelFieldCollation> fieldCollations) {
    this.fieldCollations = fieldCollations;
  }

  public static RelCollation of(RelFieldCollation... fieldCollations) {
    return new RelCollationImpl(ImmutableList.copyOf(fieldCollations));
  }

  public static RelCollation of(List<RelFieldCollation> fieldCollations) {
    return new RelCollationImpl(ImmutableList.copyOf(fieldCollations));
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

  public void register(RelOptPlanner planner) {}

  public boolean subsumes(RelTrait trait) {
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
          || e.nullDirection != RelFieldCollation.NullDirection.UNSPECIFIED) {
        sb.append(' ').append(e.shortString());
      }
      if (!it.hasNext()) {
        return sb.append(']').toString();
      }
      sb.append(',').append(' ');
    }
  }

  /**
   * Creates a list containing one collation containing one field.
   */
  public static List<RelCollation> createSingleton(int fieldIndex) {
    return ImmutableList.of(
        of(
            new RelFieldCollation(fieldIndex,
                RelFieldCollation.Direction.ASCENDING,
                RelFieldCollation.NullDirection.UNSPECIFIED)));
  }

  /**
   * Checks that a collection of collations is valid.
   *
   * @param rowType       Row type of the relational expression
   * @param collationList List of collations
   * @param fail          Whether to fail if invalid
   * @return Whether valid
   */
  public static boolean isValid(
      RelDataType rowType,
      List<RelCollation> collationList,
      boolean fail) {
    final int fieldCount = rowType.getFieldCount();
    for (RelCollation collation : collationList) {
      for (RelFieldCollation fieldCollation : collation.getFieldCollations()) {
        final int index = fieldCollation.getFieldIndex();
        if (index < 0 || index >= fieldCount) {
          assert !fail;
          return false;
        }
      }
    }
    return true;
  }

  public static boolean equal(
      List<RelCollation> collationList1,
      List<RelCollation> collationList2) {
    return collationList1.equals(collationList2);
  }

  /** Returns the indexes of the field collations in a given collation. */
  public static List<Integer> ordinals(RelCollation collation) {
    return Lists.transform(collation.getFieldCollations(),
        new Function<RelFieldCollation, Integer>() {
          public Integer apply(RelFieldCollation input) {
            return input.getFieldIndex();
          }
        });
  }
}

// End RelCollationImpl.java
