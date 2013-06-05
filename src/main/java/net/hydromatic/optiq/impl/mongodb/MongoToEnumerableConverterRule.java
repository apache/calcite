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

import net.hydromatic.optiq.rules.java.EnumerableConvention;

import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.convert.ConverterRule;
import org.eigenbase.relopt.ConventionTraitDef;
import org.eigenbase.relopt.RelTraitSet;

/**
 * Rule to convert a relational expression from
 * {@link MongoRel#CONVENTION} to
 * {@link EnumerableConvention#ARRAY}.
 */
public class MongoToEnumerableConverterRule extends ConverterRule {
  public static final ConverterRule INSTANCE =
      new MongoToEnumerableConverterRule();

  private MongoToEnumerableConverterRule() {
    super(RelNode.class, MongoRel.CONVENTION, EnumerableConvention.ARRAY,
        "MongoToEnumerableConverterRule");
  }

  @Override
  public RelNode convert(RelNode rel) {
    RelTraitSet newTraitSet =
        rel.getTraitSet().replace(
            ConventionTraitDef.instance, getOutConvention());
    return new MongoToEnumerableConverter(rel.getCluster(), newTraitSet, rel);
  }
}

// End MongoToEnumerableConverterRule.java
