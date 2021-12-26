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
import org.apache.calcite.util.mapping.Mappings;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * Description of the physical distribution of a relational expression.
 *
 * <p>TBD:</p>
 * <ul>
 *   <li>Can we shorten {@link Type#HASH_DISTRIBUTED} to HASH, etc.</li>
 *   <li>Do we need {@link RelDistributions}.DEFAULT?</li>
 *   <li>{@link RelDistributionTraitDef#convert}
 *       does not create specific physical operators as it does in Drill. Drill
 *       will need to create rules; or we could allow "converters" to be
 *       registered with the planner that are not trait-defs.
 * </ul>
 */
public interface RelDistribution extends RelMultipleTrait {
  /** Returns the type of distribution. */
  Type getType();

  /**
   * Returns the ordinals of the key columns.
   *
   * <p>Order is important for some types (RANGE); other types (HASH) consider
   * it unimportant but impose an arbitrary order; other types (BROADCAST,
   * SINGLETON) never have keys.
   */
  List<Integer> getKeys();

  /** Returns null for type which is not HASH_RANDOM while returns the number of possible instances
   *  for a record with same keys could appear on for HASH_RANDOM type. */
  default @Nullable Integer getHashRandomNumber() {
    return null;
  }

  /**
   * Applies mapping to this distribution trait.
   *
   * <p>Mapping can change the distribution trait only if it depends on distribution keys.
   *
   * <p>For example if relation is HASH distributed by keys [0, 1], after applying
   * a mapping (3, 2, 1, 0), the relation will have a distribution HASH(2,3) because
   * distribution keys changed their ordinals.
   *
   * <p>If mapping eliminates one of the distribution keys, the {@link Type#ANY}
   * distribution will be returned.
   *
   * <p>If distribution doesn't have keys (BROADCAST or SINGLETON), method will return
   * the same distribution.
   *
   * @param mapping   Mapping
   * @return distribution with mapping applied
   */
  @Override RelDistribution apply(Mappings.TargetMapping mapping);

  /** Type of distribution. */
  enum Type {
    /** There is only one instance of the stream. It sees all records. */
    SINGLETON("single"),

    /** There are multiple instances of the stream, and each instance contains
     * records whose keys hash to a particular hash value. Instances are
     * disjoint; a given record appears on exactly one stream. */
    HASH_DISTRIBUTED("hash"),

    /** This distribution type could solve data skew problem exists in hash distribution.
     *
     * <p>For hash distribution, all record with same key always appear on the same stream.
     *
     * <p>For hash random distribution, records with same key could appear on one of the N streams,
     * chosen at random.
     *
     * <p>For example, there are 128 instances of stream and the hash random number is 6. All
     * records with the same key  would appear on 6 instances of 128 instances. And which a record
     * would appear on is chosen at random.
     * <p>In the example, the type is equivalent with hash distribution if N is 1 while the type
     * is equivalent with random distribution if N is 128. */
    HASH_RANDOM_DISTRIBUTED("hash_random"),

    /** There are multiple instances of the stream, and each instance contains
     * records whose keys fall into a particular range. Instances are disjoint;
     * a given record appears on exactly one stream. */
    RANGE_DISTRIBUTED("range"),

    /** There are multiple instances of the stream, and each instance contains
     * randomly chosen records. Instances are disjoint; a given record appears
     * on exactly one stream. */
    RANDOM_DISTRIBUTED("random"),

    /** There are multiple instances of the stream, and records are assigned
     * to instances in turn. Instances are disjoint; a given record appears
     * on exactly one stream. */
    ROUND_ROBIN_DISTRIBUTED("rr"),

    /** There are multiple instances of the stream, and all records appear in
     * each instance. */
    BROADCAST_DISTRIBUTED("broadcast"),

    /** Not a valid distribution, but indicates that a consumer will accept any
     * distribution. */
    ANY("any");

    public final String shortName;

    Type(String shortName) {
      this.shortName = shortName;
    }
  }
}
