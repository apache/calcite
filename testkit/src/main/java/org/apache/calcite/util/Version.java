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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

import java.util.ArrayList;
import java.util.List;

import static java.lang.Integer.parseInt;

/**
 * Version string parsed into major and minor parts.
 */
public class Version implements Comparable<Version> {
  private static final Ordering<Iterable<Integer>> ORDERING =
      Ordering.<Integer>natural().lexicographical();

  public final List<Integer> integers;
  public final String string;

  /** Private constructor. */
  private Version(List<Integer> integers, String string) {
    this.integers = ImmutableList.copyOf(integers);
    this.string = string;
  }

  /** Creates a Version by parsing a string. */
  public static Version of(String s) {
    final String[] strings = s.split("[.-]");
    List<Integer> integers = new ArrayList<>();
    for (String string : strings) {
      try {
        integers.add(parseInt(string));
      } catch (NumberFormatException e) {
        break;
      }
    }
    return new Version(integers, s);
  }

  /** Creates a Version from a sequence of integers. */
  public static Version of(int... integers) {
    return new Version(ImmutableIntList.copyOf(integers), "");
  }

  @Override public int compareTo(Version version) {
    return ORDERING.compare(this.integers, version.integers);
  }
}
