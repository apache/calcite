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
 * Provides an implementation of relational expressions using an interpreter.
 *
 * <p>The implementation is not efficient compared to generated code, but
 * preparation time is less, and so the total prepare + execute time is
 * competitive for queries over small data sets.
 */
@PackageMarker
package org.apache.calcite.interpreter;

import org.apache.calcite.avatica.util.PackageMarker;

// End package-info.java
