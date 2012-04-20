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
 * Function with one parameter returning a native {@code boolean} value.
 *
 * @param <T1> type of parameter 1
 */
public interface Predicate1<T1> extends Function<Boolean> {
    /**
     * Predicate that always evaluates to {@code true}.
     *
     * @see Functions#truePredicate1()
     */
    Predicate1<Object> TRUE = new Predicate1<Object>() {
        public boolean apply(Object v1) {
            return true;
        }
    };

    /**
     * Predicate that always evaluates to {@code false}.
     *
     * @see Functions#falsePredicate1()
     */
    Predicate1<Object> FALSE = new Predicate1<Object>() {
        public boolean apply(Object v1) {
            return false;
        }
    };

    boolean apply(T1 v1);
}

// End Predicate1.java
