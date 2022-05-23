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
package org.apache.calcite.adapter.enumerable;

import java.util.List;

/** Class with static Helpers for MATCH_RECOGNIZE. */
public class MatchUtils {

  // Should not be instantiated
  private MatchUtils() {
    throw new IllegalStateException();
  }

  /**
   * Returns the row with the highest index whose corresponding symbol matches, null otherwise.
   * @param symbol Target Symbol
   * @param rows List of passed rows
   * @param symbols Corresponding symbols to rows
   * @return index or -1
   */
  public static <E> int lastWithSymbol(String symbol, List<E> rows, List<String> symbols,
      int startIndex) {
    for (int i = startIndex; i >= 0; i--) {
      if (symbol.equals(symbols.get(i))) {
        return i;
      }
    }
    return -1;
  }

  public static void print(int s) {
    System.out.println(s);
  }
}
