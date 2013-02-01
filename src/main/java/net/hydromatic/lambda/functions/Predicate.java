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
package net.hydromatic.lambda.functions;

/**
 * Predicate.
 *
 * <p>Based on {@code java.util.functions.Predicate}.</p>
 */
public interface Predicate<T> {
  boolean test(T t);

  Predicate<T> and(Predicate<? super T> p);
  // default:
  // return Predicates.and(this, p);

  Predicate<T> negate();
  // default:
  // return Predicates.negate(this);

  Predicate<T> or(Predicate<? super T> p);
  // default:
  // return Predicates.or(this, p);

  Predicate<T> xor(Predicate<? super T> p);
  // default:
  // return Predicates.xor(this, p);
}

// End Predicate.java
