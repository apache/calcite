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
package org.apache.calcite.plan;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.validate.SqlConformance;

/**
 * This is a marker interface for a callback used to convert a tree of
 * {@link RelNode relational expressions} into a plan. Calling
 * conventions typically have their own protocol for walking over a
 * tree, and correspondingly have their own implementors
 */
public interface RelImplementor {
  /** Returns the desired SQL conformance. */
  SqlConformance getConformance();
}

// End RelImplementor.java
