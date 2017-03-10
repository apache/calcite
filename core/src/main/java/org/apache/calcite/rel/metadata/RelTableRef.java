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
package org.apache.calcite.rel.metadata;

/**
 *
 *
 */
public class RelTableRef {

  private final String qualifiedName;
  private final int identifier;
  private final String digest;

  public RelTableRef(String qualifiedName, int identifier) {
    this.qualifiedName = qualifiedName;
    this.identifier = identifier;
    this.digest = qualifiedName + ".#" + identifier;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public boolean equals(Object obj) {
    return this == obj
        || obj instanceof RelTableRef
        && qualifiedName.equals(((RelTableRef) obj).qualifiedName)
        && identifier == ((RelTableRef) obj).identifier;
  }

  @Override public int hashCode() {
    return digest.hashCode();
  }

  public String getQualifiedName() {
    return qualifiedName;
  }

  public int getIdentifier() {
    return identifier;
  }

  @Override public String toString() {
    return digest;
  }

}

// End RelTableRef.java
