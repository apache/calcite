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
package org.apache.calcite.util;

import org.apache.calcite.util.mapping.IntPair;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Represents a mapping which reorders elements in an array.
 */
public class Permutation implements Mapping, Mappings.TargetMapping {
  //~ Instance fields --------------------------------------------------------

  private int[] targets;
  private int[] sources;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a permutation of a given size.
   *
   * <p>It is initialized to the identity permutation, such as "[0, 1, 2, 3]".
   *
   * @param size Number of elements in the permutation
   */
  public Permutation(int size) {
    targets = new int[size];
    sources = new int[size];

    // Initialize to identity.
    identity();
  }

  /**
   * Creates a permutation from an array.
   *
   * @param targets Array of targets
   * @throws IllegalArgumentException       if elements of array are not unique
   * @throws ArrayIndexOutOfBoundsException if elements of array are not
   *                                        between 0 through targets.length - 1
   *                                        inclusive
   */
  public Permutation(int[] targets) {
    this.targets = targets.clone();
    this.sources = new int[targets.length];
    Arrays.fill(sources, -1);
    for (int i = 0; i < targets.length; i++) {
      int target = targets[i];
      if (target < 0 || target >= sources.length) {
        throw new IllegalArgumentException("target out of range");
      }
      if (sources[target] != -1) {
        throw new IllegalArgumentException(
            "more than one permutation element maps to position " + target);
      }
      sources[target] = i;
    }
    assert isValid(true);
  }

  /**
   * Creates a permutation. Arrays are not copied, and are assumed to be valid
   * permutations.
   */
  private Permutation(int[] targets, int[] sources) {
    this.targets = targets;
    this.sources = sources;
    assert isValid(true);
  }

  //~ Methods ----------------------------------------------------------------

  public Object clone() {
    return new Permutation(
        targets.clone(),
        sources.clone());
  }

  /**
   * Initializes this permutation to the identity permutation.
   */
  public void identity() {
    for (int i = 0; i < targets.length; i++) {
      targets[i] = sources[i] = i;
    }
  }

  /**
   * Returns the number of elements in this permutation.
   */
  public final int size() {
    return targets.length;
  }

  public void clear() {
    throw new UnsupportedOperationException(
        "Cannot clear: permutation must always contain one mapping per element");
  }

  /**
   * Returns a string representation of this permutation.
   *
   * <p>For example, the mapping
   *
   * <table>
   * <caption>Example mapping</caption>
   * <tr>
   * <th>source</th>
   * <th>target</th>
   * </tr>
   * <tr>
   * <td>0</td>
   * <td>2</td>
   * </tr>
   * <tr>
   * <td>1</td>
   * <td>0</td>
   * </tr>
   * <tr>
   * <td>2</td>
   * <td>1</td>
   * </tr>
   * <tr>
   * <td>3</td>
   * <td>3</td>
   * </tr>
   * </table>
   *
   * <p>is represented by the string "[2, 0, 1, 3]".
   */
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append("[");
    for (int i = 0; i < targets.length; i++) {
      if (i > 0) {
        buf.append(", ");
      }
      buf.append(targets[i]);
    }
    buf.append("]");
    return buf.toString();
  }

  /**
   * Maps source position to target position.
   *
   * <p>To preserve the 1:1 nature of the permutation, the previous target of
   * source becomes the new target of the previous source.
   *
   * <p>For example, given the permutation
   *
   * <blockquote><pre>[3, 2, 0, 1]</pre></blockquote>
   *
   * <p>suppose we map position 2 to target 1. Position 2 currently has target
   * 0, and the source of position 1 is position 3. We preserve the permutation
   * property by mapping the previous source 3 to the previous target 0. The new
   * permutation is
   *
   * <blockquote><pre>[3, 2, 1, 0].</pre></blockquote>
   *
   * <p>Another example. Again starting from
   *
   * <blockquote><pre>[3, 2, 0, 1]</pre></blockquote>
   *
   * <p>suppose we map position 2 to target 3. We map the previous source 0 to
   * the previous target 0, which gives
   *
   * <blockquote><pre>[0, 2, 3, 1].</pre></blockquote>
   *
   * @param source Source position
   * @param target Target position
   * @throws ArrayIndexOutOfBoundsException if source or target is negative or
   *                                        greater than or equal to the size of
   *                                        the permuation
   */
  public void set(int source, int target) {
    set(source, target, false);
  }

  /**
   * Maps source position to target position, automatically resizing if source
   * or target is out of bounds.
   *
   * <p>To preserve the 1:1 nature of the permutation, the previous target of
   * source becomes the new target of the previous source.
   *
   * <p>For example, given the permutation
   *
   * <blockquote><pre>[3, 2, 0, 1]</pre></blockquote>
   *
   * <p>suppose we map position 2 to target 1. Position 2 currently has target
   * 0, and the source of position 1 is position 3. We preserve the permutation
   * property by mapping the previous source 3 to the previous target 0. The new
   * permutation is
   *
   * <blockquote><pre>[3, 2, 1, 0].</pre></blockquote>
   *
   * <p>Another example. Again starting from
   *
   * <blockquote><pre>[3, 2, 0, 1]</pre></blockquote>
   *
   * <p>suppose we map position 2 to target 3. We map the previous source 0 to
   * the previous target 0, which gives
   *
   * <blockquote><pre>[0, 2, 3, 1].</pre></blockquote>
   *
   * @param source      Source position
   * @param target      Target position
   * @param allowResize Whether to resize the permutation if the source or
   *                    target is greater than the current capacity
   * @throws ArrayIndexOutOfBoundsException if source or target is negative,
   *                                        or greater than or equal to the size
   *                                        of the permutation, and
   *                                        <code>allowResize</code> is false
   */
  public void set(int source, int target, boolean allowResize) {
    final int maxSourceTarget = Math.max(source, target);
    if (maxSourceTarget >= sources.length) {
      if (allowResize) {
        resize(maxSourceTarget + 1);
      } else {
        throw new ArrayIndexOutOfBoundsException(maxSourceTarget);
      }
    }
    int prevTarget = targets[source];
    assert sources[prevTarget] == source;
    int prevSource = sources[target];
    assert targets[prevSource] == target;
    setInternal(source, target);

    // To balance things up, make the previous source reference the
    // previous target. This ensures that each ordinal occurs precisely
    // once in the sources array and the targets array.
    setInternal(prevSource, prevTarget);

    // For example:
    // Before: [2, 1, 0, 3]
    // Now we set(source=1, target=0)
    //  previous target of source (1) was 1, is now 0
    //  previous source of target (0) was 2, is now 1
    //  something now has to have target 1 -- use previous source
    // After:  [2, 0, 1, 3]
  }

  /**
   * Inserts into the targets.
   *
   * <p>For example, consider the permutation</p>
   *
   * <table border="1">
   * <caption>Example permutation</caption>
   * <tr>
   * <td>source</td>
   * <td>0</td>
   * <td>1</td>
   * <td>2</td>
   * <td>3</td>
   * <td>4</td>
   * </tr>
   * <tr>
   * <td>target</td>
   * <td>3</td>
   * <td>0</td>
   * <td>4</td>
   * <td>2</td>
   * <td>1</td>
   * </tr>
   * </table>
   *
   * <p>After applying <code>insertTarget(2)</code> every target 2 or higher is
   * shifted up one.</p>
   *
   * <table border="1">
   * <caption>Mapping after applying insertTarget(2)</caption>
   * <tr>
   * <td>source</td>
   * <td>0</td>
   * <td>1</td>
   * <td>2</td>
   * <td>3</td>
   * <td>4</td>
   * <td>5</td>
   * </tr>
   * <tr>
   * <td>target</td>
   * <td>4</td>
   * <td>0</td>
   * <td>5</td>
   * <td>3</td>
   * <td>1</td>
   * <td>2</td>
   * </tr>
   * </table>
   *
   * <p>Note that the array has been extended to accommodate the new target, and
   * the previously unmapped source 5 is mapped to the unused target slot 2.</p>
   *
   * @param x Ordinal of position to add to target
   */
  public void insertTarget(int x) {
    assert isValid(true);
    resize(sources.length + 1);

    // Shuffle sources up.
    shuffleUp(sources, x);

    // Shuffle targets.
    increment(x, targets);

    assert isValid(true);
  }

  /**
   * Inserts into the sources.
   *
   * <p>Behavior is analogous to {@link #insertTarget(int)}.</p>
   *
   * @param x Ordinal of position to add to source
   */
  public void insertSource(int x) {
    assert isValid(true);
    resize(targets.length + 1);

    // Shuffle targets up.
    shuffleUp(targets, x);

    // Increment sources.
    increment(x, sources);

    assert isValid(true);
  }

  private void increment(int x, int[] zzz) {
    final int size = zzz.length;
    for (int i = 0; i < size; i++) {
      if (targets[i] == (size - 1)) {
        targets[i] = x;
      } else if (targets[i] >= x) {
        ++targets[i];
      }
    }
  }

  private void shuffleUp(final int[] zz, int x) {
    final int size = zz.length;
    int t = zz[size - 1];
    System.arraycopy(zz, x, zz, x + 1, size - 1 - x);
    zz[x] = t;
  }

  private void resize(int newSize) {
    assert isValid(true);
    final int size = targets.length;
    int[] newTargets = new int[newSize];
    System.arraycopy(targets, 0, newTargets, 0, size);
    int[] newSources = new int[newSize];
    System.arraycopy(sources, 0, newSources, 0, size);

    // Initialize the new elements to the identity mapping.
    for (int i = size; i < newSize; i++) {
      newSources[i] = i;
      newTargets[i] = i;
    }
    targets = newTargets;
    sources = newSources;
    assert isValid(true);
  }

  private void setInternal(int source, int target) {
    targets[source] = target;
    sources[target] = source;
  }

  /**
   * Returns the inverse permutation.
   */
  public Permutation inverse() {
    return new Permutation(
        sources.clone(),
        targets.clone());
  }

  /**
   * Returns whether this is the identity permutation.
   */
  public boolean isIdentity() {
    for (int i = 0; i < targets.length; i++) {
      if (targets[i] != i) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns the position that <code>source</code> is mapped to.
   */
  public int getTarget(int source) {
    try {
      return targets[source];
    } catch (ArrayIndexOutOfBoundsException e) {
      throw new Mappings.NoElementException("invalid source " + source);
    }
  }

  /**
   * Returns the position which maps to <code>target</code>.
   */
  public int getSource(int target) {
    try {
      return sources[target];
    } catch (ArrayIndexOutOfBoundsException e) {
      throw new Mappings.NoElementException("invalid target " + target);
    }
  }

  /**
   * Checks whether this permutation is valid.
   *
   *
   *
   * @param fail Whether to assert if invalid
   * @return Whether valid
   */
  private boolean isValid(boolean fail) {
    final int size = targets.length;
    if (sources.length != size) {
      assert !fail : "different lengths";
      return false;
    }

    // Every element in sources has corresponding element in targets.
    int[] occurCount = new int[size];
    for (int i = 0; i < size; i++) {
      int target = targets[i];
      if (sources[target] != i) {
        assert !fail
            : "source[" + target + "] = " + sources[target]
            + ", should be " + i;
        return false;
      }
      int source = sources[i];
      if (targets[source] != i) {
        assert !fail
            : "target[" + source + "] = " + targets[source]
            + ", should be " + i;
        return false;
      }

      // Every member should occur once.
      if (occurCount[target] != 0) {
        assert !fail : "target " + target + " occurs more than once";
        return false;
      }
      occurCount[target]++;
    }
    return true;
  }

  public int hashCode() {
    // not very efficient
    return toString().hashCode();
  }

  public boolean equals(Object obj) {
    // not very efficient
    return (obj instanceof Permutation)
        && toString().equals(obj.toString());
  }

  // implement Mapping
  public Iterator<IntPair> iterator() {
    return new Iterator<IntPair>() {
      private int i = 0;

      public boolean hasNext() {
        return i < targets.length;
      }

      public IntPair next() {
        final IntPair pair = new IntPair(i, targets[i]);
        ++i;
        return pair;
      }

      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  public int getSourceCount() {
    return targets.length;
  }

  public int getTargetCount() {
    return targets.length;
  }

  public MappingType getMappingType() {
    return MappingType.BIJECTION;
  }

  public int getTargetOpt(int source) {
    return getTarget(source);
  }

  public int getSourceOpt(int target) {
    return getSource(target);
  }

  public void setAll(Mapping mapping) {
    for (IntPair pair : mapping) {
      set(pair.source, pair.target);
    }
  }

  /**
   * Returns the product of this Permutation with a given Permutation. Does
   * not modify this Permutation or <code>permutation</code>.
   *
   * <p>For example, perm.product(perm.inverse()) yields the identity.
   */
  public Permutation product(Permutation permutation) {
    Permutation product = new Permutation(sources.length);
    for (int i = 0; i < targets.length; ++i) {
      product.set(i, permutation.getTarget(targets[i]));
    }
    return product;
  }
}

// End Permutation.java
