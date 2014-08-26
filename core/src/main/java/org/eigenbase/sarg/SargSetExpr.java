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
package org.eigenbase.sarg;

import java.util.*;

import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.util.*;

import com.google.common.collect.ImmutableList;

/**
 * SargSetExpr represents the application of a {@link SargSetOperator set
 * operator} to zero or more child {@link SargExpr sarg expressions}.
 */
public class SargSetExpr implements SargExpr {
  //~ Instance fields --------------------------------------------------------

  private final SargFactory factory;

  private final RelDataType dataType;

  private final SargSetOperator setOp;

  private final List<SargExpr> children;

  //~ Constructors -----------------------------------------------------------

  /**
   * @see SargFactory#newSetExpr
   */
  SargSetExpr(
      SargFactory factory,
      RelDataType dataType,
      SargSetOperator setOp) {
    this.factory = factory;
    this.dataType = dataType;
    this.setOp = setOp;
    children = new ArrayList<SargExpr>();
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * @return a read-only list of this expression's children (the returned
   * children themselves are modifiable)
   */
  public List<SargExpr> getChildren() {
    return ImmutableList.copyOf(children);
  }

  /**
   * Adds a child to this expression.
   *
   * @param child child to add
   */
  public void addChild(SargExpr child) {
    assert child.getDataType() == dataType;
    if (setOp == SargSetOperator.COMPLEMENT) {
      assert children.isEmpty();
    }
    children.add(child);
  }

  // implement SargExpr
  public SargFactory getFactory() {
    return factory;
  }

  // implement SargExpr
  public RelDataType getDataType() {
    return dataType;
  }

  // implement SargExpr
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(setOp);
    sb.append("(");
    for (SargExpr child : children) {
      sb.append(" ");
      sb.append(child);
    }
    sb.append(" )");
    return sb.toString();
  }

  // implement SargExpr
  public SargIntervalSequence evaluate() {
    if (setOp == SargSetOperator.COMPLEMENT) {
      assert children.size() == 1;
      SargExpr child = children.get(0);
      return child.evaluateComplemented();
    }

    List<SargIntervalSequence> list = evaluateChildren(this);

    switch (setOp) {
    case UNION:
      return evaluateUnion(list);
    case INTERSECTION:
      return evaluateIntersection(list);
    default:
      throw Util.newInternal(setOp.toString());
    }
  }

  private List<SargIntervalSequence> evaluateChildren(SargSetExpr setExpr) {
    List<SargIntervalSequence> list = new ArrayList<SargIntervalSequence>();

    for (SargExpr child : setExpr.children) {
      SargIntervalSequence newSeq = child.evaluate();
      list.add(newSeq);
    }

    return list;
  }

  // implement SargExpr
  public void collectDynamicParams(Set<RexDynamicParam> dynamicParams) {
    for (SargExpr child : children) {
      child.collectDynamicParams(dynamicParams);
    }
  }

  private SargIntervalSequence evaluateUnion(List<SargIntervalSequence> list) {
    SargIntervalSequence seq = new SargIntervalSequence();

    // Toss all entries from each sequence in the list into one big sorted
    // set.
    SortedSet<SargInterval> intervals =
        new TreeSet<SargInterval>(IntervalComparator.INSTANCE);
    for (SargIntervalSequence childSeq : list) {
      intervals.addAll(childSeq.getList());
    }

    // Now, overlapping ranges are consecutive in the set.  Merge them by
    // increasing the upper bound of the first; discard the others.  In the
    // example, [4, 6] and [5, 7) are combined to form [4, 7).  (7, 8] is
    // not merged with the new range because neither range contains the
    // value 7.
    //
    // Input:
    //          1  2  3  4  5  6  7  8  9
    // 1 [1, 3] [-----]
    // 2 [4, 6]          [-----]
    // 3 [5, 7)             [-----)
    // 4 (7, 8]                   (--]
    //
    // Output:
    // 1 [1, 3] [-----]
    // 2 [4, 7)          [--------)
    // 3 (7, 8]                   (--]
    SargInterval accumulator = null;
    for (SargInterval interval : intervals) {
      // Empty intervals should have been previously filtered out.
      assert !interval.isEmpty();

      if (accumulator == null) {
        // The very first interval:  start accumulating.
        accumulator =
            new SargInterval(
                factory,
                getDataType());
        accumulator.copyFrom(interval);
        seq.addInterval(accumulator);
        continue;
      }

      if (accumulator.contains(interval)) {
        // Just drop new interval because it's already covered
        // by accumulator.
        continue;
      }

      // Test for overlap.
      int c =
          interval.getLowerBound().compareTo(
              accumulator.getUpperBound());

      // If no overlap, test for touching instead.
      if (c > 0) {
        if (interval.getLowerBound().isTouching(
            accumulator.getUpperBound())) {
          // Force test below to pass.
          c = -1;
        }
      }

      if (c <= 0) {
        // Either touching or overlap:  grow the accumulator.
        accumulator.upperBound.copyFrom(interval.getUpperBound());
      } else {
        // Disjoint:  start accumulating a new interval
        accumulator =
            new SargInterval(
                factory,
                getDataType());
        accumulator.copyFrom(interval);
        seq.addInterval(accumulator);
      }
    }

    return seq;
  }

  private SargIntervalSequence evaluateIntersection(
      List<SargIntervalSequence> list) {
    SargIntervalSequence seq = null;

    if (list.isEmpty()) {
      // Counterintuitive but true: intersection of no sets is the
      // universal set (kinda like 2^0=1).  One way to prove this to
      // yourself is to apply DeMorgan's law.  The union of no sets is
      // certainly the empty set.  So the complement of that union is the
      // universal set.  That's equivalent to the intersection of the
      // complements of no sets, which is the intersection of no sets.
      // QED.
      seq = new SargIntervalSequence();
      seq.addInterval(
          new SargInterval(
              factory,
              getDataType()));
      return seq;
    }

    // The way we evaluate the intersection is to start with the first
    // entry as a baseline, and then keep deleting stuff from it by
    // intersecting the other entrie in turn.  Whatever makes it through
    // this filtering remains as the final result.
    for (SargIntervalSequence newSeq : list) {
      if (seq == null) {
        // first child
        seq = newSeq;
        continue;
      }
      intersectSequences(seq, newSeq);
    }

    return seq;
  }

  private void intersectSequences(
      SargIntervalSequence targetSeq,
      SargIntervalSequence sourceSeq) {
    ListIterator<SargInterval> targetIter = targetSeq.list.listIterator();
    if (!targetIter.hasNext()) {
      // No target intervals at all, so quit.
      return;
    }

    ListIterator<SargInterval> sourceIter = sourceSeq.list.listIterator();
    if (!sourceIter.hasNext()) {
      // No source intervals at all, so result is empty
      targetSeq.list.clear();
      return;
    }

    // Start working on first source and target intervals
    SargInterval target = targetIter.next();
    SargInterval source = sourceIter.next();

    // loop invariant:  both source and target are non-null on entry
    for (;;) {
      if (source.getUpperBound().compareTo(target.getLowerBound()) < 0) {
        // Source is completely below target; discard it and
        // move on to next one.
        if (!sourceIter.hasNext()) {
          // No more sources.
          break;
        }
        source = sourceIter.next();
        continue;
      }

      if (target.getUpperBound().compareTo(source.getLowerBound()) < 0) {
        // Target is completely below source; discard it and
        // move on to next one.
        targetIter.remove();
        if (!targetIter.hasNext()) {
          // All done.
          return;
        }
        target = targetIter.next();
        continue;
      }

      // Overlap case:  perform intersection of the two intervals.

      if (source.getLowerBound().compareTo(target.getLowerBound()) > 0) {
        // Source starts after target starts, so trim the target.
        target.setLower(
            source.getLowerBound().getCoordinate(),
            source.getLowerBound().getStrictness());
      }

      int c = source.getUpperBound().compareTo(target.getUpperBound());
      if (c < 0) {
        // The source ends before the target ends, so split the target
        // into two parts.  The first part will be kept for sure; the
        // second part will be compared against further source ranges.
        SargInterval newTarget =
            new SargInterval(
                factory,
                dataType);
        newTarget.setLower(
            source.getUpperBound().getCoordinate(),
            source.getUpperBound().getStrictnessComplement());

        if (target.getUpperBound().isFinite()) {
          newTarget.setUpper(
              target.getUpperBound().getCoordinate(),
              target.getUpperBound().getStrictness());
        }

        // Trim current target to exclude the part of the range
        // which will move to newTarget.
        target.setUpper(
            source.getUpperBound().getCoordinate(),
            source.getUpperBound().getStrictness());

        // Insert newTarget after target.  This makes newTarget
        // into previous().
        targetIter.add(newTarget);

        // Next time through, work on newTarget.
        // targetIter.previous() is pointing at the newTarget.
        target = targetIter.previous();

        // Now targetIter.next() is also pointing at the newTarget;
        // need to do this redundant step to get targetIter in sync
        // with target.
        target = targetIter.next();

        // Advance source.
        if (!sourceIter.hasNext()) {
          break;
        }
        source = sourceIter.next();
      } else if (c == 0) {
        // Source and target ends coincide, so advance both source and
        // target.
        if (!targetIter.hasNext()) {
          return;
        }
        target = targetIter.next();
        if (!sourceIter.hasNext()) {
          break;
        }
        source = sourceIter.next();
      } else {
        // Source ends after target ends, so advance target.
        assert c > 0;
        if (!targetIter.hasNext()) {
          return;
        }
        target = targetIter.next();
      }
    }

    // Discard any remaining targets since they didn't have corresponding
    // sources.
    for (;;) {
      targetIter.remove();
      if (!targetIter.hasNext()) {
        break;
      }
      targetIter.next();
    }
  }

  // implement SargExpr
  public SargIntervalSequence evaluateComplemented() {
    if (setOp == SargSetOperator.COMPLEMENT) {
      // Double negation is a nop
      return children.get(0).evaluate();
    }

    // Use DeMorgan's Law:  complement of union is intersection of
    // complements, and vice versa
    List<SargIntervalSequence> list = new ArrayList<SargIntervalSequence>();
    for (SargExpr child : children) {
      SargIntervalSequence newSeq = child.evaluateComplemented();
      list.add(newSeq);
    }
    switch (setOp) {
    case INTERSECTION:
      return evaluateUnion(list);
    case UNION:
      return evaluateIntersection(list);
    default:
      throw Util.newInternal(setOp.toString());
    }
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Comparator used in evaluateUnionOp. Intervals collate based on
   * {lowerBound, upperBound}.
   */
  private static class IntervalComparator implements Comparator<SargInterval> {
    public static final IntervalComparator INSTANCE = new IntervalComparator();

    private IntervalComparator() {
    }

    // implement Comparator
    public int compare(SargInterval i1, SargInterval i2) {
      int c = i1.getLowerBound().compareTo(i2.getLowerBound());
      if (c != 0) {
        return c;
      }

      return i1.getUpperBound().compareTo(i2.getUpperBound());
    }
  }
}

// End SargSetExpr.java
