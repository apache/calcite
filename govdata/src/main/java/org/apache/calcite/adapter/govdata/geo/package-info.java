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
 * Geographic data adapter for Apache Calcite.
 *
 * <p>Provides access to U.S. government geographic data including:
 * <ul>
 *   <li>Census Bureau TIGER/Line boundary files (states, counties, places, ZCTAs)</li>
 *   <li>Census Bureau demographic and economic data via API</li>
 *   <li>HUD-USPS ZIP code to Census geography crosswalk</li>
 *   <li>Census geocoding services</li>
 * </ul>
 *
 * <p>All data sources are free government services. Some require registration
 * for API keys but have no cost.
 */
package org.apache.calcite.adapter.govdata.geo;