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
 * Parse tree for SQL DDL statements.
 *
 * <p>These are available in the extended SQL parser that is part of Calcite's
 * "server" module; the core parser in the "core" module only supports SELECT
 * and DML.
 *
 * <p>If you are writing a project that requires DDL it is likely that your
 * DDL syntax is different than ours. We recommend that you copy-paste this
 * the parser and its supporting classes into your own module, rather than try
 * to extend this one.
 */
@PackageMarker
package org.apache.calcite.sql.ddl;

import org.apache.calcite.avatica.util.PackageMarker;

// End package-info.java
