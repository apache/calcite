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
package org.apache.calcite.runtime.rtti;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;

/** Runtime type information which contains some type parameters. */
public class GenericSqlTypeRtti extends RuntimeTypeInformation {
  final RuntimeTypeInformation[] typeArguments;

  public GenericSqlTypeRtti(RuntimeSqlTypeName typeName, RuntimeTypeInformation... typeArguments) {
    super(typeName);
    assert !typeName.isScalar() : "Generic type cannot be a scalar type " + typeName;
    this.typeArguments = typeArguments;
  }

  @Override public String getTypeString()  {
    switch (this.typeName) {
    case ARRAY:
      return "ARRAY";
    case MAP:
      return "MAP";
    case MULTISET:
      return "MULTISET";
    default:
      throw new RuntimeException("Unexpected type " + this.typeName);
    }
  }

  // This method is used to serialize the type in Java code implementations,
  // so it should produce a computation that reconstructs the type at runtime
  @Override public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("new GenericSqlTypeRtti(")
        .append(this.getTypeString());
    for (RuntimeTypeInformation arg : this.typeArguments) {
      builder.append(", ");
      builder.append(arg.toString());
    }
    builder.append(")");
    return builder.toString();
  }

  @Override public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    GenericSqlTypeRtti that = (GenericSqlTypeRtti) o;
    return typeName == that.typeName && Arrays.equals(typeArguments, that.typeArguments);
  }

  @Override public int hashCode() {
    return Arrays.hashCode(typeArguments);
  }

  public RuntimeTypeInformation getTypeArgument(int index) {
    return typeArguments[index];
  }

  public int getArgumentCount() {
    return typeArguments.length;
  }
}
