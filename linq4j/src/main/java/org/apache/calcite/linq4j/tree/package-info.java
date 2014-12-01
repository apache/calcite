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
 * Object model for Java expressions.
 *
 * <p>This object model is used when the linq4j system is analyzing
 * queries that have been submitted using methods on the
 * {@link org.apache.calcite.linq4j.Queryable} interface. The system attempts
 * to understand the intent of the query and reorganize it for
 * efficiency; for example, it may attempt to push down filters to the
 * source SQL system.</p>
 */
package org.apache.calcite.linq4j.tree;

// End package-info.java
