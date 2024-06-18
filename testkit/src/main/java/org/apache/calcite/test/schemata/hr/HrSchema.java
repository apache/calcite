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
package org.apache.calcite.test.schemata.hr;

import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.util.Smalls;

import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.Collections;

/**
 * A schema that contains two tables by reflection.
 *
 * <p>Here is the SQL to create equivalent tables in Oracle:
 *
 * <blockquote>
 * <pre>
 * CREATE TABLE "emps" (
 *   "empid" INTEGER NOT NULL,
 *   "deptno" INTEGER NOT NULL,
 *   "name" VARCHAR(10) NOT NULL,
 *   "salary" NUMBER(6, 2) NOT NULL,
 *   "commission" INTEGER);
 * INSERT INTO "emps" VALUES (100, 10, 'Bill', 10000, 1000);
 * INSERT INTO "emps" VALUES (200, 20, 'Eric', 8000, 500);
 * INSERT INTO "emps" VALUES (150, 10, 'Sebastian', 7000, null);
 * INSERT INTO "emps" VALUES (110, 10, 'Theodore', 11500, 250);
 *
 * CREATE TABLE "depts" (
 *   "deptno" INTEGER NOT NULL,
 *   "name" VARCHAR(10) NOT NULL,
 *   "employees" ARRAY OF "Employee",
 *   "location" "Location");
 * INSERT INTO "depts" VALUES (10, 'Sales', null, (-122, 38));
 * INSERT INTO "depts" VALUES (30, 'Marketing', null, (0, 52));
 * INSERT INTO "depts" VALUES (40, 'HR', null, null);
 * </pre>
 * </blockquote>
 */
public class HrSchema {
  @Override public String toString() {
    return "HrSchema";
  }

  public final Employee[] emps = {
      new Employee(100, 10, "Bill", 10000, 1000),
      new Employee(200, 20, "Eric", 8000, 500),
      new Employee(150, 10, "Sebastian", 7000, null),
      new Employee(110, 10, "Theodore", 11500, 250),
  };
  public final Department[] depts = {
      new Department(10, "Sales", Arrays.asList(emps[0], emps[2]),
          new Location(-122, 38)),
      new Department(30, "Marketing", ImmutableList.of(), new Location(0, 52)),
      new Department(40, "HR", Collections.singletonList(emps[1]), null),
  };
  public final Dependent[] dependents = {
      new Dependent(10, "Michael"),
      new Dependent(10, "Jane"),
  };
  public final Dependent[] locations = {
      new Dependent(10, "San Francisco"),
      new Dependent(20, "San Diego"),
  };

  public QueryableTable foo(int count) {
    return Smalls.generateStrings(count);
  }

  public TranslatableTable view(String s) {
    return Smalls.view(s);
  }
}
