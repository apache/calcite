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
package org.eigenbase.rel;

/**
 * Visitor that has methods for the common logical relational expressions.
 */
public interface RelShuttle {
  RelNode visit(TableAccessRelBase scan);

  RelNode visit(TableFunctionRelBase scan);

  RelNode visit(ValuesRel values);

  RelNode visit(FilterRel filter);

  RelNode visit(ProjectRel project);

  RelNode visit(JoinRel join);

  RelNode visit(CorrelatorRel correlator);

  RelNode visit(UnionRel union);

  RelNode visit(IntersectRel intersect);

  RelNode visit(MinusRel minus);

  RelNode visit(AggregateRel aggregate);

  RelNode visit(SortRel sort);

  RelNode visit(RelNode other);
}

// End RelShuttle.java
