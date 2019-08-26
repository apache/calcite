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

import java.io.StringWriter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Contains methods that call JDK methods that the
 * <a href="https://github.com/policeman-tools/forbidden-apis">forbidden
 * APIs checker</a> does not approve of.
 *
 * <p>This class is excluded from the check, so methods called via this class
 * will not fail the build.
 */
public class Unsafe {
  private Unsafe() {}

  /** Calls {@link System#exit}. */
  public static void systemExit(int status) {
    System.exit(status);
  }

  /** Calls {@link Object#notifyAll()}. */
  public static void notifyAll(Object o) {
    o.notifyAll();
  }

  /** Calls {@link Object#wait()}. */
  public static void wait(Object o) throws InterruptedException {
    o.wait();
  }

  /** Clears the contents of a {@link StringWriter}. */
  public static void clear(StringWriter sw) {
    // Included in this class because StringBuffer is banned.
    sw.getBuffer().setLength(0);
  }

  /** Helper for the SQL {@code REGEXP_REPLACE} function.
   *
   * <p>It is marked "unsafe" because it uses {@link StringBuffer};
   * Versions of {@link Matcher#appendReplacement(StringBuffer, String)}
   * and {@link Matcher#appendTail(StringBuffer)}
   * that use {@link StringBuilder} are not available until JDK 9. */
  public static String regexpReplace(String s, Pattern pattern,
      String replacement, int pos, int occurrence) {
    Bug.upgrade("when we drop JDK 8, replace StringBuffer with StringBuilder");
    final StringBuffer sb = new StringBuffer();
    final String input;
    if (pos != 1) {
      sb.append(s, 0, pos - 1);
      input = s.substring(pos - 1);
    } else {
      input = s;
    }

    int count = 0;
    Matcher matcher = pattern.matcher(input);
    while (matcher.find()) {
      if (occurrence == 0) {
        matcher.appendReplacement(sb, replacement);
      } else if (++count == occurrence) {
        matcher.appendReplacement(sb, replacement);
        break;
      }
    }
    matcher.appendTail(sb);

    return sb.toString();
  }
}

// End Unsafe.java
