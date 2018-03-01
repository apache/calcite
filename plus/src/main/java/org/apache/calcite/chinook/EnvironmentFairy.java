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
package org.apache.calcite.chinook;

/**
 * Fairy simulates environment around Calcite.
 *
 * <p>An example property is the user on whose behalf Calcite is running the
 * current query. Other properties can change from one query to another.
 * Properties are held in thread-locals, so it is safe to set a property then
 * read it from the same thread.
 */
public class EnvironmentFairy {

  private static final ThreadLocal<Condition> CONDITION = new ThreadLocal<Condition>() {
    @Override protected Condition initialValue() {
      return Condition.GENERAL_CONDITION;
    }
  };

  private EnvironmentFairy() {
  }

  public static void setCondition(Condition condition) {
    CONDITION.set(condition);
  }

  public static Condition getCondition() {
    return CONDITION.get();
  }

  /**
   * Who is emulated to being logged in?
   */
  public enum Condition {
    GENERAL_CONDITION, SPECIFIC_CONDITION
  }

}

// End EnvironmentFairy.java
