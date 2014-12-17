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
 * Calcite query provider that reads from CSV (comma-separated value) files.
 *
 * <p>A Calcite schema maps onto a directory, and each CSV file in that
 * directory appears as a table.  Full SQL operations are available on
 * those tables.</p>
 */
@PackageMarker
package org.apache.calcite.adapter.csv;

import org.apache.calcite.avatica.util.PackageMarker;

// End package-info.java
