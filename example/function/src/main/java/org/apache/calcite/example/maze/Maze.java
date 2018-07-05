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
package org.apache.calcite.example.maze;

import org.apache.calcite.linq4j.Enumerator;

import java.io.PrintWriter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

/** Maze generator. */
class Maze {
  private final int width;
  final int height;
  private final int[] regions;
  private final boolean[] ups;
  private final boolean[] lefts;

  static final boolean DEBUG = false;
  private final boolean horizontal = false;
  private final boolean spiral = false;

  Maze(int width, int height) {
    this.width = width;
    this.height = height;
    this.regions = new int[width * height];
    for (int i = 0; i < regions.length; i++) {
      regions[i] = i;
    }
    this.ups = new boolean[width * height + width];
    this.lefts = new boolean[width * height + 1];
  }

  private int region(int cell) {
    int region = regions[cell];
    if (region == cell) {
      return region;
    }
    return regions[cell] = region(region);
  }

  /** Prints the maze. Results are like this:
   *
   * <blockquote>
   * +--+--+--+--+--+
   * |        |     |
   * +--+  +--+--+  +
   * |     |  |     |
   * +  +--+  +--+  +
   * |              |
   * +--+--+--+--+--+
   * </blockquote>
   *
   * @param pw Print writer
   * @param space Whether to put a space in each cell; if false, prints the
   *              region number of the cell
   */
  public void print(PrintWriter pw, boolean space) {
    pw.println();
    final StringBuilder b = new StringBuilder();
    final StringBuilder b2 = new StringBuilder();
    final CellContent cellContent;
    if (space) {
      cellContent = c -> "  ";
    } else {
      cellContent = c -> {
        String s = region(c) + "";
        return s.length() == 1 ? " " + s : s;
      };
    }
    for (int y = 0; y < height; y++) {
      row(cellContent, b, b2, y);
      pw.println(b.toString());
      pw.println(b2.toString());
      b.setLength(0);
      b2.setLength(0);
    }
    for (int x = 0; x < width; x++) {
      pw.print("+--");
    }
    pw.println('+');
    pw.flush();
  }

  /** Generates a list of lines representing the maze in text form. */
  public Enumerator<String> enumerator(final Set<Integer> solutionSet) {
    final CellContent cellContent;
    if (solutionSet == null) {
      cellContent = CellContent.SPACE;
    } else {
      cellContent = c -> solutionSet.contains(c) ? "* " : "  ";
    }
    return new Enumerator<String>() {
      int i = -1;
      final StringBuilder b = new StringBuilder();
      final StringBuilder b2 = new StringBuilder();

      public String current() {
        return i % 2 == 0 ? b.toString() : b2.toString();
      }

      public boolean moveNext() {
        if (i >= height * 2) {
          return false;
        }
        ++i;
        if (i % 2 == 0) {
          b.setLength(0);
          b2.setLength(0);
          row(cellContent, b, b2, i / 2);
        }
        return true;
      }

      public void reset() {
        i = -1;
      }

      public void close() {}
    };
  }

  /** Returns a pair of strings representing a row of the maze. */
  private void row(CellContent cellContent, StringBuilder b, StringBuilder b2,
      int y) {
    final int c0 = y * width;
    for (int x = 0; x < width; x++) {
      b.append('+');
      b.append(ups[c0 + x] ? "  " : "--");
    }
    b.append('+');
    if (y == height) {
      return;
    }
    for (int x = 0; x < width; x++) {
      b2.append(lefts[c0 + x] ? ' ' : '|')
          .append(cellContent.get(c0 + x));
    }
    b2.append('|');
  }

  public Maze layout(Random random, PrintWriter pw) {
    int[] candidates =
        new int[width * height - width
            + width * height - height];
    int z = 0;
    for (int y = 0, c = 0; y < height; y++) {
      for (int x = 0; x < width; x++) {
        if (x > 0) {
          candidates[z++] = c;
        }
        ++c;
        if (y > 0) {
          candidates[z++] = c;
        }
        ++c;
      }
    }
    assert z == candidates.length;
    shuffle(random, candidates);

    for (int candidate : candidates) {
      final boolean up = (candidate & 1) != 0;
      final int c = candidate >> 1;
      if (up) {
        int region = region(c - width);

        // make sure we are not joining the same region, that is, making
        // a cycle
        if (region(c) != region) {
          ups[c] = true;
          regions[regions[c]] = region;
          regions[c] = region;
          if (DEBUG) {
            pw.println("up " + c);
          }
        } else {
          if (DEBUG) {
            pw.println("cannot remove top wall at " + c);
          }
        }
      } else {
        int region = region(c - 1);

        // make sure we are not joining the same region, that is, making
        // a cycle
        if (region(c) != region) {
          lefts[c] = true;
          regions[regions[c]] = region;
          regions[c] = region;
          if (DEBUG) {
            pw.println("left " + c);
          }
        } else {
          if (DEBUG) {
            pw.println("cannot remove left wall at " + c);
          }
        }
      }
      if (DEBUG) {
        print(pw, false);
        print(pw, true);
      }
    }
    return this;
  }

  Set<Integer> solve(int x, int y) {
    int c = y * width + x;
    final int target = regions.length - 1;
    Direction d = Direction.UP;
    final List<Integer> list = new ArrayList<>();
    final Deque<Direction> fromStack = new ArrayDeque<>();
    final Deque<Direction> directionStack = new ArrayDeque<>();
    Direction from = Direction.BACKTRACK;
    int cNext = 0;
    Direction dNext = Direction.UP;
    boolean move = false;
    for (;;) {
      switch (d) {
      case UP:
        // try to go up
        move = from != Direction.DOWN && ups[c];
        cNext = c - width;
        dNext = Direction.LEFT;
        break;
      case LEFT:
        // try to go left
        move = from != Direction.RIGHT && lefts[c];
        cNext = c - 1;
        dNext = Direction.DOWN;
        break;
      case DOWN:
        // try to go down
        move = from != Direction.UP && c + width < regions.length
            && ups[c + width];
        cNext = c + width;
        dNext = Direction.RIGHT;
        break;
      case RIGHT:
        move = from != Direction.LEFT && c % width < width - 1
            && lefts[c + 1];
        cNext = c + 1;
        dNext = Direction.BACKTRACK;
        break;
      case BACKTRACK:
        move = false;
        do {
          c = list.remove(list.size() - 1);
          dNext = directionStack.pop();
          from = fromStack.pop();
        } while (dNext == Direction.BACKTRACK);
      }
      if (move) {
        directionStack.push(dNext);
        fromStack.push(from);
        list.add(c);
        if (cNext == target) {
          list.add(cNext);
          return new LinkedHashSet<>(list);
        }
        from = d;
        d = Direction.UP;
        c = cNext;
      } else {
        d = dNext;
      }
    }
  }

  /** Direction. */
  private enum Direction {
    UP, LEFT, DOWN, RIGHT, BACKTRACK
  }

  /**
   * Randomly permutes the members of an array. Based on the Fisher-Yates
   * algorithm.
   *
   * @param random Random number generator
   * @param ints Array of integers to shuffle
   */
  private void shuffle(Random random, int[] ints) {
    for (int i = ints.length - 1; i > 0; i--) {
      int j = random.nextInt(i + 1);
      int t = ints[j];
      ints[j] = ints[i];
      ints[i] = t;
    }

    // move even walls (left) towards the start, so we end up with
    // long horizontal corridors
    if (horizontal) {
      for (int i = 2; i < ints.length; i++) {
        if (ints[i] % 2 == 0) {
          int j = random.nextInt(i);
          int t = ints[j];
          ints[j] = ints[i];
          ints[i] = t;
        }
      }
    }

    // move walls towards the edges towards the start
    if (spiral) {
      for (int z = 0; z < 5; z++) {
        for (int i = 2; i < ints.length; i++) {
          int x = ints[i] / 2 % width;
          int y = ints[i] / 2 / width;
          int xMin = Math.min(x, width - x);
          int yMin = Math.min(y, height - y);
          if (ints[i] % 2 == (xMin < yMin ? 1 : 0)) {
            int j = random.nextInt(i);
            int t = ints[j];
            ints[j] = ints[i];
            ints[i] = t;
          }
        }
      }
    }
  }

  /** Callback to get what to print in a particular cell. Must be two characters
   * long, usually two spaces. */
  interface CellContent {
    CellContent SPACE = c -> "  ";

    String get(int c);
  }
}

// End Maze.java
