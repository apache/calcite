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
package org.eigenbase.util.mapping;

/**
 * An immutable pair of integers.
 *
 * @see Mapping#iterator()
 */
public class IntPair {
  //~ Instance fields --------------------------------------------------------

  public final int source;
  public final int target;

  //~ Constructors -----------------------------------------------------------

  public IntPair(int source, int target) {
    this.source = source;
    this.target = target;
  }

  //~ Methods ----------------------------------------------------------------

  public String toString() {
    return source + "-" + target;
  }

  public boolean equals(Object obj) {
    if (obj instanceof IntPair) {
      IntPair that = (IntPair) obj;
      return (this.source == that.source) && (this.target == that.target);
    }
    return false;
  }

  public int hashCode() {
    return this.source ^ (this.target << 4);
  }
}

// End IntPair.java
