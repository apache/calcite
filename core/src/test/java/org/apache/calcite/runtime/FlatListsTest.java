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
package org.apache.calcite.runtime;

import org.apache.calcite.runtime.FlatLists.Flat3List;
import org.apache.calcite.runtime.FlatLists.Flat4List;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests {@link FlatLists}.
 */
public class FlatListsTest {

  @Test
  public void testFlat34Equals() {
    Flat3List f3list = new Flat3List(1, 2, 3);
    Flat4List f4list = new Flat4List(1, 2, 3, 4);
    Assert.assertFalse(f3list.equals(f4list));
  }

}

// End FlatListsTest.java
