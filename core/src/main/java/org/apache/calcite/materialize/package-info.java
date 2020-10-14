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
 * Management of materialized query results.
 *
 * <p>
 *   An actor ({@link org.apache.calcite.materialize.MaterializationActor})
 *   maintains the state of all materializations in the system
 *   and is wrapped in a service
 *   ({@link org.apache.calcite.materialize.MaterializationService}) for access from other parts of the system.
 *
 * <p>
 *   Optimizer rules allow Calcite to rewrite queries using materializations,
 *   if they are valid (that is, contain the same result as executing their
 *   defining query) and lower cost.
 *
 * <p>
 *   In future, the actor may manage the process of updating materializations,
 *   instantiating materializations from the intermediate results of queries, and
 *   recognize what materializations would be useful based on actual query load.
 *
 * fixme
 *      管理 实体化 的查询结果。
 *      MaterializationActor 管理着系统中所有 materializations 的状态。
 *      并被包装在 MaterializationService 中，可以被系统的其他部分访问。
 *
 * fixme
 *      如果查询有效且成本较低，则优化规则会使用materializations重写查询。
 *
 * fixme
 *      在未来，actor可能会管理 更新 实体化 的处理，
 *      从查询的中间结果实例化 实体化 结果，
 *      并且根据实际的查询负载、识别哪些 实体 是有用的。
 *
 */
package org.apache.calcite.materialize;
