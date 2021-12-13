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
package org.apache.calcite.test;

/**
 * A Schema representing a foomart.test
 *
 * <p>It contains a single table with a column name same as the table name
 */
public class FoodmartTestSchema {
  public final FoodmartTestSchema.Test[] test = {
      new FoodmartTestSchema.Test("t1", "test t1"),
      new FoodmartTestSchema.Test("t2", "test t2"),
  };

  /** Test table. */
  public static class Test {
    public final String test;
    public final String description;


    public Test(String test, String description) {
      this.test = test;
      this.description = description;

    }
  }
}
