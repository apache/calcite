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

/**
 * Represents a catch statement in a try block.
 */
public class CatchBlock {
  public final ParameterExpression parameter;
  public final Statement body;

  public CatchBlock(ParameterExpression parameter,
      Statement body) {
    this.parameter = parameter;
    this.body = body;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    CatchBlock that = (CatchBlock) o;

    if (body != null ? !body.equals(that.body) : that.body != null) {
      return false;
    }
    if (parameter != null ? !parameter.equals(that.parameter) : that
        .parameter != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = parameter != null ? parameter.hashCode() : 0;
    result = 31 * result + (body != null ? body.hashCode() : 0);
    return result;
  }
}

// End CatchBlock.java
