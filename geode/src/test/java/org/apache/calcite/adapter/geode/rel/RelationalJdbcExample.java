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
package org.apache.calcite.adapter.geode.rel;

/**
 * Example of using Geode via JDBC.
 *
 * <p>Before using this example, you need to populate Geode, as follows:
 *
 * <blockquote><code>
 * git clone https://github.com/vlsi/calcite-test-dataset<br>
 * cd calcite-test-dataset<br>
 * mvn install
 * </code></blockquote>
 *
 * <p>This will create a virtual machine with Geode and the "bookshop" and "zips"
 * test data sets.
 */
public class RelationalJdbcExample {

  private RelationalJdbcExample() {
  }

  public static void main(String[] args) throws Exception {
    RelationalJdbcExampleRunner exampleRunner = new RelationalJdbcExampleRunner();
    for (int i = 0; i < 2; i++) {
      exampleRunner.doExample(i);
    }
  }
}

// End RelationalJdbcExample.java
