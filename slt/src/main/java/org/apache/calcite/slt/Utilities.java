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

package org.apache.calcite.slt;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class Utilities {
  private Utilities() {}

  /**
   * Just adds single quotes around a string.  No escaping is performed.
   */
  public static String singleQuote(String other) {
    return "'" + other + "'";
  }

  @Nullable
  public static String getFileExtension(String filename) {
    int i = filename.lastIndexOf('.');
    if (i > 0)
      return filename.substring(i+1);
    return null;
  }

  private static final char[] hexCode = "0123456789abcdef".toCharArray();

  public static String toHex(byte[] data) {
    StringBuilder r = new StringBuilder(data.length * 2);
    for (byte b : data) {
      r.append(hexCode[(b >> 4) & 0xF]);
      r.append(hexCode[(b & 0xF)]);
    }
    return r.toString();
  }

  public static <T, S> List<S> map(List<T> data, Function<T, S> function) {
    List<S> result = new ArrayList<>(data.size());
    for (T aData : data)
      result.add(function.apply(aData));
    return result;
  }

  public static <T, S> List<S> flatMap(List<T> data, Function<T, List<S>> function) {
    List<S> result = new ArrayList<>(data.size());
    for (T aData : data)
      result.addAll(function.apply(aData));
    return result;
  }
}
