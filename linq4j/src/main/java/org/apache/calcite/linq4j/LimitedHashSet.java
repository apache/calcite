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
package org.apache.calcite.linq4j;

import java.util.Collection;
import java.util.HashSet;

/**
 * @param <E> something
 */
public class LimitedHashSet<E> extends HashSet<E> {

  private static float load = 0.9f;
  private static int limit = Math.round((1 << 21) / load) + 1;   //21 = 2.3 Mill
  private static int initCapacity = 1 << 10;

  /**
   * something
   */
  public LimitedHashSet() {
    super(initCapacity, load);
  }

  public LimitedHashSet(Collection<? extends E> c) {
    super(Math.max((int) (c.size() / load) + 1, initCapacity), load);
    super.addAll(c);
  }

  /**
   * @param e something
   * @return boolean
   */
  @Override public boolean add(E e) {
    boolean ret = super.add(e);
    if (ret && super.size() > limit) {
      throw new LimitedCollectionsException("Exceeding query row size limit: (" + limit + ").");
    }
    return ret;
  }
}
// End LimitedHashSet.java
