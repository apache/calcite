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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A trait that consists of a list of traits, all of the same type.
 *
 * <p>It exists so that multiple traits of the same type
 * ({@link org.apache.calcite.plan.RelTraitDef}) can be stored in the same
 * {@link org.apache.calcite.plan.RelTraitSet}.
 *
 * @param <T> Member trait
 */
class RelCompositeTrait<T extends RelMultipleTrait> implements RelTrait {
  private final RelTraitDef traitDef;
  private final T[] traits;

  /** Creates a RelCompositeTrait. */
  // Must remain private. Does not copy the array.
  private RelCompositeTrait(RelTraitDef traitDef, T[] traits) {
    this.traitDef = traitDef;
    this.traits = Objects.requireNonNull(traits);
    //noinspection unchecked
    assert Ordering.natural()
        .isStrictlyOrdered(Arrays.asList((Comparable[]) traits))
        : Arrays.toString(traits);
    for (T trait : traits) {
      assert trait.getTraitDef() == this.traitDef;
    }
  }

  /** Creates a RelCompositeTrait. The constituent traits are canonized. */
  @SuppressWarnings("unchecked")
  static <T extends RelMultipleTrait> RelCompositeTrait<T> of(RelTraitDef def,
      List<T> traitList) {
    final RelCompositeTrait<T> compositeTrait;
    if (traitList.isEmpty()) {
      compositeTrait = new EmptyCompositeTrait<>(def);
    } else {
      final RelMultipleTrait[] traits =
          traitList.toArray(new RelMultipleTrait[0]);
      for (int i = 0; i < traits.length; i++) {
        traits[i] = (T) def.canonize(traits[i]);
      }
      compositeTrait = new RelCompositeTrait<>(def, (T[]) traits);
    }
    return def.canonizeComposite(compositeTrait);
  }

  public RelTraitDef getTraitDef() {
    return traitDef;
  }

  @Override public int hashCode() {
    return Arrays.hashCode(traits);
  }

  @Override public boolean equals(Object obj) {
    return this == obj
        || obj instanceof RelCompositeTrait
        && Arrays.equals(traits, ((RelCompositeTrait) obj).traits);
  }

  @Override public String toString() {
    return Arrays.toString(traits);
  }

  public boolean satisfies(RelTrait trait) {
    for (T t : traits) {
      if (t.satisfies(trait)) {
        return true;
      }
    }
    return false;
  }

  public void register(RelOptPlanner planner) {
  }

  /** Returns an immutable list of the traits in this composite trait. */
  public List<T> traitList() {
    return ImmutableList.copyOf(traits);
  }

  RelCompositeTrait<T> canonize(RelTraitDef<T> traitDef) {
    T[] newTraits = null;
    for (int i = 0; i < traits.length; i++) {
      final T trait = traits[i];
      final T trait2 = traitDef.canonize(trait);
      if (trait2 != trait) {
        if (newTraits == null) {
          newTraits = traits.clone();
        }
        newTraits[i] = trait2;
      }
    }
    if (newTraits == null) {
      return this;
    }
    assert false;
    // TODO: cache duplicate composites
    return new RelCompositeTrait<>(traitDef, newTraits);
  }

  public T trait(int i) {
    return traits[i];
  }

  public int size() {
    return traits.length;
  }

  /** Composite trait with 0 elements.
   *
   * @param <T> trait type */
  private static class EmptyCompositeTrait<T extends RelMultipleTrait>
      extends RelCompositeTrait<T> {
    private EmptyCompositeTrait(RelTraitDef traitDef) {
      //noinspection unchecked
      super(traitDef, (T[]) new RelMultipleTrait[0]);
    }

    @Override public boolean satisfies(RelTrait trait) {
      //noinspection unchecked
      return ((T) trait).isTop();
    }
  }
}

// End RelCompositeTrait.java
