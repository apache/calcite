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

/**
 * Provides a core set of planner rules.
 *
 * <p>Consider this package to be the "standard library" of planner rules.
 * Most of the common rewrites that you would want to perform on logical
 * relational expressions, or generically on any data source, are present,
 * and have been well tested.
 *
 * <p>Of course, the library is never complete, and contributions are welcome.
 *
 * <p>Not present are rules specific to a particular data source: look in that
 * data source's adapter.
 *
 * <p>Also out of the scope of this package are rules that support a particular
 * operation, such as decorrelation or recognizing materialized views. Those are
 * defined along with the algorithm.
 *
 * <p>For
 *
 * <h2>Related packages and classes</h2>
 * <ul>
 *    <li>Package<code> <a href="../../sql/package-summary.html">
 *        org.apache.calcite.sql</a></code>
 *        is an object model for SQL expressions</li>
 *    <li>Package<code> <a href="../../rex/package-summary.html">
 *        org.apache.calcite.rex</a></code>
 *        is an object model for relational row expressions</li>
 *    <li>Package<code> <a href="../../plan/package-summary.html">
 *        org.apache.calcite.plan</a></code>
 *        provides an optimizer interface.</li>
 * </ul>
 */
@PackageMarker
package org.apache.calcite.rel.rules;

import org.apache.calcite.avatica.util.PackageMarker;

// End package-info.java
