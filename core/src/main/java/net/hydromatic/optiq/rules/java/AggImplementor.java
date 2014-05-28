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
package net.hydromatic.optiq.rules.java;

import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.linq4j.expressions.Expressions;

import org.eigenbase.rel.Aggregation;

import java.lang.reflect.Type;
import java.util.List;

/** Implements an aggregate function by generating expressions to
 * initialize, add to, and get a result from, an accumulator. */
interface AggImplementor {
  Expression implementInit(RexToLixTranslator translator,
      Aggregation aggregation, Type returnType, List<Type> parameterTypes);

  Expression implementAdd(AddInfo info);

  Expression implementInitAdd(RexToLixTranslator translator,
      Aggregation aggregation, Type returnType, List<Type> parameterTypes,
      List<Expression> arguments);

  Expression implementResult(RexToLixTranslator translator,
      Aggregation aggregation, Expression accumulator);

  /** Information for a call to {@link AggImplementor#implementAdd(AddInfo)}. */
  interface AddInfo {
    RexToLixTranslator translator();
    Aggregation aggregation();
    Expression accumulator();
    List<Expression> arguments();
    Expression orderChanged();
    Expression index();
    Expression orderKeyStartIndex();
  }

  /** Helper class. */
  class Impls {
    private Impls() {}

    static AddInfo add(final RexToLixTranslator translator,
        final Aggregation aggregation, final List<Expression> arguments,
        final Expression accumulator) {
      return new AddInfo() {
        public RexToLixTranslator translator() {
          return translator;
        }

        public Aggregation aggregation() {
          return aggregation;
        }

        public Expression accumulator() {
          return accumulator;
        }

        public List<Expression> arguments() {
          return arguments;
        }

        public Expression orderChanged() {
          return RexImpTable.FALSE_EXPR;
        }

        public Expression index() {
          return Expressions.constant(0);
        }

        public Expression orderKeyStartIndex() {
          return index();
        }
      };
    }
  }
}

// End AggImplementor.java
