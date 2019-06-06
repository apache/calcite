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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Utilities concerning {@link org.apache.calcite.rel.RelCollation}
 * and {@link org.apache.calcite.rel.RelFieldCollation}.
 */
public class RelCollations {
  /**
   * A collation indicating that a relation is not sorted. Ordering by no
   * columns.
   */
  public static final RelCollation EMPTY =
      RelCollationTraitDef.INSTANCE.canonize(
          new RelCollationImpl(ImmutableList.of()));

  /**
   * A collation that cannot be replicated by applying a sort. The only
   * implementation choice is to apply operations that preserve order.
   */
  @Deprecated // to be removed before 2.0
  public static final RelCollation PRESERVE =
      RelCollationTraitDef.INSTANCE.canonize(
          new RelCollationImpl(
              ImmutableList.of(new RelFieldCollation(-1))) {
            public String toString() {
              return "PRESERVE";
            }
          });

  private RelCollations() {}

  public static RelCollation of(RelFieldCollation... fieldCollations) {
    return of(ImmutableList.copyOf(fieldCollations));
  }

  public static RelCollation of(List<RelFieldCollation> fieldCollations) {
    if (Util.isDistinct(ordinals(fieldCollations))) {
      return new RelCollationImpl(ImmutableList.copyOf(fieldCollations));
    }
    // Remove field collations whose field has already been seen
    final ImmutableList.Builder<RelFieldCollation> builder =
        ImmutableList.builder();
    final Set<Integer> set = new HashSet<>();
    for (RelFieldCollation fieldCollation : fieldCollations) {
      if (set.add(fieldCollation.getFieldIndex())) {
        builder.add(fieldCollation);
      }
    }
    return new RelCollationImpl(builder.build());
  }

  /**
   * Creates a collation containing one field.
   */
  public static RelCollation of(int fieldIndex) {
    return of(new RelFieldCollation(fieldIndex));
  }

  /**
   * Creates a list containing one collation containing one field.
   */
  public static List<RelCollation> createSingleton(int fieldIndex) {
    return ImmutableList.of(of(fieldIndex));
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
    return ordinals(collation.getFieldCollations());
  }

  /** Returns the indexes of the fields in a list of field collations. */
  public static List<Integer> ordinals(
      List<RelFieldCollation> fieldCollations) {
    return Lists.transform(fieldCollations, RelFieldCollation::getFieldIndex);
  }

  /** Returns whether a collation indicates that the collection is sorted on
   * a given list of keys.
   *
   * @param collation Collation
   * @param keys List of keys
   * @return Whether the collection is sorted on the given keys
   */
  public static boolean contains(RelCollation collation,
      Iterable<Integer> keys) {
    return contains(collation, Util.distinctList(keys));
  }

  private static boolean contains(RelCollation collation,
      List<Integer> keys) {
    final int n = collation.getFieldCollations().size();
    final Iterator<Integer> iterator = keys.iterator();
    for (int i = 0; i < n; i++) {
      final RelFieldCollation fieldCollation =
          collation.getFieldCollations().get(i);
      if (!iterator.hasNext()) {
        return true;
      }
      if (fieldCollation.getFieldIndex() != iterator.next()) {
        return false;
      }
    }
    return !iterator.hasNext();
  }

  /** Returns whether one of a list of collations indicates that the collection
   * is sorted on the given list of keys. */
  public static boolean contains(List<RelCollation> collations,
      ImmutableIntList keys) {
    final List<Integer> distinctKeys = Util.distinctList(keys);
    for (RelCollation collation : collations) {
      if (contains(collation, distinctKeys)) {
        return true;
      }
    }
    return false;
  }

  public static RelCollation shift(RelCollation collation, int offset) {
    if (offset == 0) {
      return collation; // save some effort
    }
    final ImmutableList.Builder<RelFieldCollation> fieldCollations =
        ImmutableList.builder();
    for (RelFieldCollation fc : collation.getFieldCollations()) {
      fieldCollations.add(fc.shift(offset));
    }
    return new RelCollationImpl(fieldCollations.build());
  }

  /** Creates a copy of this collation that changes the ordinals of input
   * fields. */
  public static RelCollation permute(RelCollation collation,
      Map<Integer, Integer> mapping) {
    return of(
        Util.transform(collation.getFieldCollations(),
            fc -> fc.withFieldIndex(mapping.get(fc.getFieldIndex()))));
  }

  /** Creates a copy of this collation that changes the ordinals of input
   * fields. */
  public static RelCollation permute(RelCollation collation,
      Mappings.TargetMapping mapping) {
    return of(
        Util.transform(collation.getFieldCollations(),
            fc -> fc.withFieldIndex(mapping.getTarget(fc.getFieldIndex()))));
  }
}

// End RelCollations.java
