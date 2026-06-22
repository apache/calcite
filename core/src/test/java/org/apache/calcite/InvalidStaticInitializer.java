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
package org.apache.calcite;

/**
 * An invalid class that always fail if initialized. The class may be sub-classed to cover
 * test cases where a static initializer is not allowed to be triggered. All the classes in the
 * hierarchy are called by reflection thus appear as unused.
 */
@SuppressWarnings("unused")
public class InvalidStaticInitializer {
  static {
    throwError();
  }
  private static void throwError() {
    throw new AssertionError("Static initializer must not be triggered");
  }
}
