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
package org.apache.calcite.test.schemata.pets;

import java.util.Objects;

/**
 * Pet model.
 */
public class Pet implements Comparable<Pet> {
  public final String name;
  public final int age;

  public Pet(String name, int age) {
    this.name = name;
    this.age = age;
  }

  @Override public String toString() {
    return "Pet [name: " + name + ", age: " + age + "]";
  }

  @Override public boolean equals(Object obj) {
    return obj == this
        || obj instanceof Pet
        && name.equals(((Pet) obj).name)
        && age == ((Pet) obj).age;
  }

  @Override public int hashCode() {
    return Objects.hash(name, age);
  }

  @Override public int compareTo(Pet other) {
    return Integer.compare(this.hashCode(), other.hashCode());
  }
}
