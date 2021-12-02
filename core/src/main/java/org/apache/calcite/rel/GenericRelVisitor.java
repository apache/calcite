/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License") throws E; you may not use this file except in compliance with
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

package org.apache.calcite.rel;

import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMatch;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;

/**
 * Visitor that has methods for the common logical relational expressions.
 */
public interface GenericRelVisitor<T, E extends Throwable> {

  T visit(TableScan scan) throws E;

  T visit(TableFunctionScan scan) throws E;

  T visit(LogicalValues values) throws E;

  T visit(LogicalFilter filter) throws E;

  T visit(LogicalCalc calc) throws E;

  T visit(LogicalProject project) throws E;

  T visit(LogicalJoin join) throws E;

  T visit(LogicalCorrelate correlate) throws E;

  T visit(LogicalUnion union) throws E;

  T visit(LogicalIntersect intersect) throws E;

  T visit(LogicalMinus minus) throws E;

  T visit(LogicalAggregate aggregate) throws E;

  T visit(LogicalMatch match) throws E;

  T visit(LogicalSort sort) throws E;

  T visit(LogicalExchange exchange) throws E;

  T visit(LogicalTableModify modify) throws E;

  T visit(RelNode other) throws E;

}
