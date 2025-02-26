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

import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.Collections;

/**
 * HR schema with more data than in {@link HrSchema}.
 */
public class HrSchemaBig {
  @Override public String toString() {
    return "HrSchema";
  }

  public final Employee[] emps = {
      new Employee(1, 10, "Bill", 10000, 1000),
      new Employee(2, 20, "Eric", 8000, 500),
      new Employee(3, 10, "Sebastian", 7000, null),
      new Employee(4, 10, "Theodore", 11500, 250),
      new Employee(5, 10, "Marjorie", 10000, 1000),
      new Employee(6, 20, "Guy", 8000, 500),
      new Employee(7, 10, "Dieudonne", 7000, null),
      new Employee(8, 10, "Haroun", 11500, 250),
      new Employee(9, 10, "Sarah", 10000, 1000),
      new Employee(10, 20, "Gabriel", 8000, 500),
      new Employee(11, 10, "Pierre", 7000, null),
      new Employee(12, 10, "Paul", 11500, 250),
      new Employee(13, 10, "Jacques", 100, 1000),
      new Employee(14, 20, "Khawla", 8000, 500),
      new Employee(15, 10, "Brielle", 7000, null),
      new Employee(16, 10, "Hyuna", 11500, 250),
      new Employee(17, 10, "Ahmed", 10000, 1000),
      new Employee(18, 20, "Lara", 8000, 500),
      new Employee(19, 10, "Capucine", 7000, null),
      new Employee(20, 10, "Michelle", 11500, 250),
      new Employee(21, 10, "Cerise", 10000, 1000),
      new Employee(22, 80, "Travis", 8000, 500),
      new Employee(23, 10, "Taylor", 7000, null),
      new Employee(24, 10, "Seohyun", 11500, 250),
      new Employee(25, 70, "Helen", 10000, 1000),
      new Employee(26, 50, "Patric", 8000, 500),
      new Employee(27, 10, "Clara", 7000, null),
      new Employee(28, 10, "Catherine", 11500, 250),
      new Employee(29, 10, "Anibal", 10000, 1000),
      new Employee(30, 30, "Ursula", 8000, 500),
      new Employee(31, 10, "Arturito", 7000, null),
      new Employee(32, 70, "Diane", 11500, 250),
      new Employee(33, 10, "Phoebe", 10000, 1000),
      new Employee(34, 20, "Maria", 8000, 500),
      new Employee(35, 10, "Edouard", 7000, null),
      new Employee(36, 110, "Isabelle", 11500, 250),
      new Employee(37, 120, "Olivier", 10000, 1000),
      new Employee(38, 20, "Yann", 8000, 500),
      new Employee(39, 60, "Ralf", 7000, null),
      new Employee(40, 60, "Emmanuel", 11500, 250),
      new Employee(41, 10, "Berenice", 10000, 1000),
      new Employee(42, 20, "Kylie", 8000, 500),
      new Employee(43, 80, "Natacha", 7000, null),
      new Employee(44, 100, "Henri", 11500, 250),
      new Employee(45, 90, "Pascal", 10000, 1000),
      new Employee(46, 90, "Sabrina", 8000, 500),
      new Employee(47, 8, "Riyad", 7000, null),
      new Employee(48, 5, "Andy", 11500, 250),
  };
  public final Department[] depts = {
      new Department(10, "Sales", Arrays.asList(emps[0], emps[2]),
          new Location(-122, 38)),
      new Department(20, "Marketing", ImmutableList.of(), new Location(0, 52)),
      new Department(30, "HR", Collections.singletonList(emps[1]), null),
      new Department(40, "Administration", Arrays.asList(emps[0], emps[2]),
          new Location(-122, 38)),
      new Department(50, "Design", ImmutableList.of(), new Location(0, 52)),
      new Department(60, "IT", Collections.singletonList(emps[1]), null),
      new Department(70, "Production", Arrays.asList(emps[0], emps[2]),
          new Location(-122, 38)),
      new Department(80, "Finance", ImmutableList.of(), new Location(0, 52)),
      new Department(90, "Accounting", Collections.singletonList(emps[1]), null),
      new Department(100, "Research", Arrays.asList(emps[0], emps[2]),
          new Location(-122, 38)),
      new Department(110, "Maintenance", ImmutableList.of(), new Location(0, 52)),
      new Department(120, "Client Support", Collections.singletonList(emps[1]), null),
  };
}
