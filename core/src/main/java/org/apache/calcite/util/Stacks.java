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

import java.util.List;

/**
 * Utilities to make vanilla lists look like stacks.
 */
@Deprecated // to be removed before 2.0
public class Stacks {
  private Stacks() {
  }

  /**
   * Returns the most recently added element in the stack. Throws if the
   * stack is empty.
   */
  public static <T> T peek(List<T> stack) {
    return stack.get(stack.size() - 1);
  }

  /**
   * Returns the {@code n}th most recently added element in the stack.
   * Throws if the stack is empty.
   */
  public static <T> T peek(int n, List<T> stack) {
    return stack.get(stack.size() - n - 1);
  }

  /**
   * Adds an element to the stack.
   */
  public static <T> void push(List<T> stack, T element) {
    stack.add(element);
  }

  /**
   * Removes an element from the stack. Asserts of the element is not the
   * one last added; throws if the stack is empty.
   */
  public static <T> void pop(List<T> stack, T element) {
    assert stack.get(stack.size() - 1) == element;
    stack.remove(stack.size() - 1);
  }

  /**
   * Removes an element from the stack and returns it.
   *
   * <p>Throws if the stack is empty.
   */
  public static <T> T pop(List<T> stack) {
    return stack.remove(stack.size() - 1);
  }
}

// End Stacks.java
