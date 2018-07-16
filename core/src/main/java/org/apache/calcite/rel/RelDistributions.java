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
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.Ordering;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * Utilities concerning {@link org.apache.calcite.rel.RelDistribution}.
 */
public class RelDistributions {
  private static final ImmutableIntList EMPTY = ImmutableIntList.of();

  /** The singleton singleton distribution. */
  public static final RelDistribution SINGLETON =
      new RelDistributionImpl(RelDistribution.Type.SINGLETON, EMPTY);

  /** The singleton random distribution. */
  public static final RelDistribution RANDOM_DISTRIBUTED =
      new RelDistributionImpl(RelDistribution.Type.RANDOM_DISTRIBUTED, EMPTY);

  /** The singleton round-robin distribution. */
  public static final RelDistribution ROUND_ROBIN_DISTRIBUTED =
      new RelDistributionImpl(RelDistribution.Type.ROUND_ROBIN_DISTRIBUTED,
          EMPTY);

  /** The singleton broadcast distribution. */
  public static final RelDistribution BROADCAST_DISTRIBUTED =
      new RelDistributionImpl(RelDistribution.Type.BROADCAST_DISTRIBUTED,
          EMPTY);

  public static final RelDistribution ANY =
      new RelDistributionImpl(RelDistribution.Type.ANY, EMPTY);

  private RelDistributions() {}

  /** Creates a hash distribution. */
  public static RelDistribution hash(Collection<? extends Number> numbers) {
    ImmutableIntList list = ImmutableIntList.copyOf(numbers);
    if (numbers.size() > 1
        && !Ordering.natural().isOrdered(list)) {
      list = ImmutableIntList.copyOf(Ordering.natural().sortedCopy(list));
    }
    RelDistributionImpl trait =
        new RelDistributionImpl(RelDistribution.Type.HASH_DISTRIBUTED, list);
    return RelDistributionTraitDef.INSTANCE.canonize(trait);
  }

  /** Creates a range distribution. */
  public static RelDistribution range(Collection<? extends Number> numbers) {
    ImmutableIntList list = ImmutableIntList.copyOf(numbers);
    RelDistributionImpl trait =
        new RelDistributionImpl(RelDistribution.Type.RANGE_DISTRIBUTED, list);
    return RelDistributionTraitDef.INSTANCE.canonize(trait);
  }

  /** Implementation of {@link org.apache.calcite.rel.RelDistribution}. */
  private static class RelDistributionImpl implements RelDistribution {
    private static final Ordering<Iterable<Integer>> ORDERING =
        Ordering.<Integer>natural().lexicographical();
    private final Type type;
    private final ImmutableIntList keys;

    private RelDistributionImpl(Type type, ImmutableIntList keys) {
      this.type = Objects.requireNonNull(type);
      this.keys = ImmutableIntList.copyOf(keys);
      assert type != Type.HASH_DISTRIBUTED
          || keys.size() < 2
          || Ordering.natural().isOrdered(keys)
          : "key columns of hash distribution must be in order";
      assert type == Type.HASH_DISTRIBUTED
          || type == Type.RANDOM_DISTRIBUTED
          || keys.isEmpty();
    }

    @Override public int hashCode() {
      return Objects.hash(type, keys);
    }

    @Override public boolean equals(Object obj) {
      return this == obj
          || obj instanceof RelDistributionImpl
          && type == ((RelDistributionImpl) obj).type
          && keys.equals(((RelDistributionImpl) obj).keys);
    }

    @Override public String toString() {
      if (keys.isEmpty()) {
        return type.shortName;
      } else {
        return type.shortName + keys;
      }
    }

    @Nonnull public Type getType() {
      return type;
    }

    @Nonnull public List<Integer> getKeys() {
      return keys;
    }

    public RelDistributionTraitDef getTraitDef() {
      return RelDistributionTraitDef.INSTANCE;
    }

    public RelDistribution apply(Mappings.TargetMapping mapping) {
      if (keys.isEmpty()) {
        return this;
      }
      return getTraitDef().canonize(
          new RelDistributionImpl(type,
              ImmutableIntList.copyOf(
                  Mappings.apply((Mapping) mapping, keys))));
    }

    public boolean satisfies(RelTrait trait) {
      if (trait == this || trait == ANY) {
        return true;
      }
      if (trait instanceof RelDistributionImpl) {
        RelDistributionImpl distribution = (RelDistributionImpl) trait;
        if (type == distribution.type) {
          switch (type) {
          case HASH_DISTRIBUTED:
            // The "leading edge" property of Range does not apply to Hash.
            // Only Hash[x, y] satisfies Hash[x, y].
            return keys.equals(distribution.keys);
          case RANGE_DISTRIBUTED:
            // Range[x, y] satisfies Range[x, y, z] but not Range[x]
            return Util.startsWith(distribution.keys, keys);
          default:
            return true;
          }
        }
      }
      if (trait == RANDOM_DISTRIBUTED) {
        // RANDOM is satisfied by HASH, ROUND-ROBIN, RANDOM, RANGE;
        // we've already checked RANDOM
        return type == Type.HASH_DISTRIBUTED
            || type == Type.ROUND_ROBIN_DISTRIBUTED
            || type == Type.RANGE_DISTRIBUTED;
      }
      return false;
    }

    public void register(RelOptPlanner planner) {
    }

    @Override public boolean isTop() {
      return type == Type.ANY;
    }

    @Override public int compareTo(@Nonnull RelMultipleTrait o) {
      final RelDistribution distribution = (RelDistribution) o;
      if (type == distribution.getType()
          && (type == Type.HASH_DISTRIBUTED
              || type == Type.RANGE_DISTRIBUTED)) {
        return ORDERING.compare(getKeys(), distribution.getKeys());
      }

      return type.compareTo(distribution.getType());
    }
  }
}

// End RelDistributions.java
