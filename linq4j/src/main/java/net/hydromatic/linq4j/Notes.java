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
package net.hydromatic.linq4j;

/**
 * Notes about linq4j.
 *
 * <h1>Mapping between linq4j and openjdk-lambda</h1>
 *
 * <table>
 * <tr>
 * <th>linq4j</th>
 * <th>openjdk-lambda</th>
 * <th>Remarks</th>
 * </tr>
 * <tr>
 * <td>{@link net.hydromatic.linq4j.function.Predicate1 Predicate1}</td>
 * <td>{@link net.hydromatic.lambda.functions.Predicate Predicate}</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>{@link net.hydromatic.linq4j.function.Predicate2 Predicate2}</td>
 * <td>{@link net.hydromatic.lambda.functions.BiPredicate BiPredicate}</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>{@link net.hydromatic.linq4j.function.Function Function}</td>
 * <td></td>
 * <td>No equivalent in openjdk</td>
 * </tr>
 * <tr>
 * <td>{@link Enumerable}</td>
 * <td>{@link net.hydromatic.lambda.streams.Stream Stream},
 * {@link net.hydromatic.lambda.streams.MapStream MapStream}</td>
 * </tr>
 * <tr>
 * <td>{@link Grouping}&lt;K, V&gt;</td>
 * <td>{@link net.hydromatic.lambda.functions.BiValue BiValue}&lt;K, Iterable&lt;V&gt;</td>
 * </tr>
 * <tr>
 * <td>{@link Queryable}</td>
 * <td></td>
 * <td>No equivalent in openjdk</td>
 * </tr>
 * </table>
 */
final class Notes {
  private Notes() {
  }
}

// End Notes.java
