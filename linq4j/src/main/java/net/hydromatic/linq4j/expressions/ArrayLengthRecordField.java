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

import java.lang.reflect.Array;
import java.lang.reflect.Type;

/**
 * Represents a length field of a RecordType
 */
public class ArrayLengthRecordField implements Types.RecordField {
  private final String fieldName;
  private final Class clazz;

  public ArrayLengthRecordField(String fieldName, Class clazz) {
    assert fieldName != null : "fieldName should not be null";
    assert clazz != null : "clazz should not be null";
    this.fieldName = fieldName;
    this.clazz = clazz;
  }

  public boolean nullable() {
    return false;
  }

  public String getName() {
    return fieldName;
  }

  public Type getType() {
    return int.class;
  }

  public int getModifiers() {
    return 0;
  }

  public Object get(Object o) throws IllegalAccessException {
    return Array.getLength(o);
  }

  public Type getDeclaringClass() {
    return clazz;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ArrayLengthRecordField that = (ArrayLengthRecordField) o;

    if (!clazz.equals(that.clazz)) {
      return false;
    }
    if (!fieldName.equals(that.fieldName)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = fieldName.hashCode();
    result = 31 * result + clazz.hashCode();
    return result;
  }
}
