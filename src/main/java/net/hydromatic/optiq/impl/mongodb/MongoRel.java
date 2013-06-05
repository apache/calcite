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
package net.hydromatic.optiq.impl.mongodb;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.Convention;
import org.eigenbase.util.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * Relational expression that uses Mongo calling convention.
 */
public interface MongoRel extends RelNode {
  void implement(Implementor implementor);

  /** Calling convention for relational operations that occur in MongoDB. */
  final Convention CONVENTION = new Convention.Impl("MONGO", MongoRel.class);

  class Implementor {
    final List<Pair<String, String>> list =
        new ArrayList<Pair<String, String>>();

    MongoTable table;

    public void add(String findOp, String aggOp) {
      list.add(Pair.of(findOp, aggOp));
    }

    public void visitChild(int ordinal, RelNode input) {
      assert ordinal == 0;
      ((MongoRel) input).implement(this);
    }
  }
}

// End MongoRel.java
