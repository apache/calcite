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
package org.eigenbase.relopt;

import java.util.List;

import org.eigenbase.rel.*;
import org.eigenbase.rex.*;

/**
 * Walks over a tree of {@link RelNode relational expressions}, walking a {@link
 * RexShuttle} over every expression in that tree.
 */
public class VisitorRelVisitor extends RelVisitor {
  //~ Instance fields --------------------------------------------------------

  protected final RexVisitor<?> visitor;

  //~ Constructors -----------------------------------------------------------

  public VisitorRelVisitor(RexVisitor<?> visitor) {
    this.visitor = visitor;
  }

  //~ Methods ----------------------------------------------------------------

  public void visit(
      RelNode p,
      int ordinal,
      RelNode parent) {
    List<RexNode> childExps = p.getChildExps();
    for (RexNode childExp : childExps) {
      childExp.accept(visitor);
    }
    p.childrenAccept(this);
  }
}

// End VisitorRelVisitor.java
