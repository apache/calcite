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
package org.apache.calcite.schema;

import org.apache.calcite.linq4j.function.Experimental;

/**
 * A transient table is a named table that may come into existence implicitly during the
 * evaluation of a query expression or the execution of a trigger. A transient table is
 * identified by a query name if it arises during the evaluation of a query expression,
 * or by a transition table name if it arises during the execution of a trigger.
 * Such tables exist only for the duration of the executing SQL-statement containing the
 * query expression or for the duration of the executing trigger.
 */
@Experimental
public interface TransientTable extends Table {
}

// End TransientTable.java
