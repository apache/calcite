/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.linq4j.expressions;

import java.lang.reflect.Field;
import java.lang.reflect.Type;

/**
 * Represents a PseudoField that is implemented via java reflection Field
 */
public class ReflectedPseudoField implements PseudoField {
  private final Field field;

  public ReflectedPseudoField(Field field) {
    assert field != null : "field should not be null";
    this.field = field;
  }

  public String getName() {
    return field.getName();
  }

  public Type getType() {
    return field.getType();
  }

  public int getModifiers() {
    return field.getModifiers();
  }

  public Object get(Object o) throws IllegalAccessException {
    return field.get(o);
  }

  public Class<?> getDeclaringClass() {
    return field.getDeclaringClass();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ReflectedPseudoField that = (ReflectedPseudoField) o;

    if (!field.equals(that.field)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return field.hashCode();
  }
}
