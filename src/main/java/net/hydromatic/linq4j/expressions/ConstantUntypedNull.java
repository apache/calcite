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
 * Represents a constant null of unknown type
 * Java allows type inference for such nulls, thus "null" cannot always be
 * replaced to (Object)null and vise versa.
 *
 * ConstantExpression(null, Object.class) is not equal to ConstantUntypedNull
 * However, optimizers might treat all the nulls equal (e.g. in case of
 * comparison).
 */
public class ConstantUntypedNull extends ConstantExpression {
  public static final ConstantExpression INSTANCE = new ConstantUntypedNull();

  private ConstantUntypedNull() {
    super(Object.class, null);
  }

  @Override
  void accept(ExpressionWriter writer, int lprec, int rprec) {
    writer.append("null");
  }

  @Override
  public boolean equals(Object o) {
    return o == INSTANCE;
  }

  @Override
  public int hashCode() {
    return ConstantUntypedNull.class.hashCode();
  }
}
