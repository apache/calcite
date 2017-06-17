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
package org.apache.calcite.linq4j.tree;

import java.lang.reflect.Type;

/**
 * <p>Statement.</p>
 */
public abstract class Statement extends AbstractNode {
  protected Statement(ExpressionType nodeType, Type type) {
    super(nodeType, type);
  }

  @Override final void accept(ExpressionWriter writer, int lprec, int rprec) {
    assert lprec == 0;
    assert rprec == 0;
    accept0(writer);
  }

  // Make return type more specific. A statement can only become a different
  // kind of statement; it can't become an expression.
  @Override public abstract Statement accept(Shuttle shuttle);
}

// End Statement.java
