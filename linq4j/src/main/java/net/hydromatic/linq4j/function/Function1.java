/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.linq4j.function;

/**
 * Function with one parameter.
 *
 * @param <R> Result type
 * @param <T0> Type of parameter 0
 */
public interface Function1<T0, R> extends Function<R> {
  /**
   * The identity function.
   *
   * @see Functions#identitySelector()
   */
  Function1<Object, Object> IDENTITY = new Function1<Object, Object>() {
    public Object apply(Object v0) {
      return v0;
    }
  };

  R apply(T0 a0);
}

// End Function1.java
