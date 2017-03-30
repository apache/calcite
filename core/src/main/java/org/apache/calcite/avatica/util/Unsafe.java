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
package org.apache.calcite.avatica.util;

import java.util.Calendar;
import java.util.Locale;

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

  /** Returns a {@link java.util.Calendar} with the local time zone and root
   * locale. */
  public static Calendar localCalendar() {
    return Calendar.getInstance(Locale.ROOT);
  }
}

// End Unsafe.java
