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
package net.hydromatic.optiq;

import org.eigenbase.reltype.RelDataType;

import java.util.List;

/**
 * A collection of objects in a schema that have the same name.
 *
 * <p>As well as providing the objects, the Overload interface provides a
 * method to resolve a particular call to a particular function, based on
 * the types of the arguments. A particular implementation might allow the
 * following:</p>
 *
 * <ul>
 *
 * <li>Implicit conversions. For example, supply a {@code float} argument
 * for {@code double} parameter</li>
 *
 * <li>Allow parameters to have default values. For example, you can call
 * {@code double logarithm(double value, double base = Math.E)} with either
 * one or two arguments.</li>
 *
 * <li>Derive extra type information from the arguments. For example, a call
 * to the "substring" function with a first argument of type "VARCHAR(30)"
 * yields a result of type "VARCHAR(30)).</li>
 *
 * </ul>
 *
 * <p>It is up to the implementation how to choose between the various
 * options.</p>
 *
 * <p>Note that a schema that consists only of tables (which have unique
 * names, and no parameters) will not return any overloads.</p>
 */
public interface Overload extends SchemaObject {
    /**
     * Resolves this overload to a particular function, or returns null if
     * there is no matching function.
     *
     * @param argumentTypes Parameter types
     * @return Resolved function, or null if no match
     */
    Function resolve(List<RelDataType> argumentTypes);
}

// End Overload.java
