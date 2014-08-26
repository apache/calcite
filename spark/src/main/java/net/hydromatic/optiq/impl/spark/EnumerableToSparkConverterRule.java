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
package net.hydromatic.optiq.impl.spark;

import net.hydromatic.optiq.rules.java.EnumerableConvention;

import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.convert.ConverterRule;
import org.eigenbase.relopt.RelTraitSet;

/**
 * Rule to convert a relational expression from
 * {@link net.hydromatic.optiq.impl.jdbc.JdbcConvention} to
 * {@link SparkRel#CONVENTION Spark convention}.
 */
public class EnumerableToSparkConverterRule extends ConverterRule {
  public static final EnumerableToSparkConverterRule INSTANCE =
      new EnumerableToSparkConverterRule();

  private EnumerableToSparkConverterRule() {
    super(
        RelNode.class, EnumerableConvention.INSTANCE, SparkRel.CONVENTION,
        "EnumerableToSparkConverterRule");
  }

  public RelNode convert(RelNode rel) {
    RelTraitSet newTraitSet = rel.getTraitSet().replace(getOutTrait());
    return new EnumerableToSparkConverter(
        rel.getCluster(), newTraitSet, rel);
  }
}

// End EnumerableToSparkConverterRule.java
