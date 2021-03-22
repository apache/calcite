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
package org.apache.calcite.util.interval;

/**
 * DateTime interval.
 */
public enum DateTimeTypeName {
  YEAR(1, "YEAR"),
  MONTH(2, "MONTH"),
  DAY(3, "DAY"),
  HOUR(4, "HOUR"),
  MINUTE(5, "MINUTE"),
  SECOND(6, "SECOND");

  int ordinal;
  String dateTime;

  DateTimeTypeName(int ordinal, String dateTime) {
    this.ordinal = ordinal;
    this.dateTime = dateTime;
  }
}
