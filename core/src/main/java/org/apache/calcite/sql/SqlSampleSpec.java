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
package org.apache.calcite.sql;

import org.apache.calcite.sql.dialect.CalciteSqlDialect;

import java.math.BigDecimal;

import static java.util.Objects.requireNonNull;

/**
 * Specification of a SQL sample.
 *
 * <p>For example, the query
 *
 * <blockquote>
 * <pre>SELECT *
 * FROM emp TABLESAMPLE SUBSTITUTE('medium')</pre>
 * </blockquote>
 *
 * <p>declares a sample which is created using {@link #createNamed}.
 *
 * <p>A sample is not a {@link SqlNode}. To include it in a parse tree, wrap it
 * as a literal, viz:
 * {@link SqlLiteral#createSample(SqlSampleSpec, org.apache.calcite.sql.parser.SqlParserPos)}.
 */
public abstract class SqlSampleSpec {
  private static final BigDecimal ONE_HUNDRED = BigDecimal.valueOf(100);

  //~ Constructors -----------------------------------------------------------

  protected SqlSampleSpec() {
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Creates a sample which substitutes one relation for another.
   */
  public static SqlSampleSpec createNamed(String name) {
    return new SqlSubstitutionSampleSpec(name);
  }

  /**
   * Creates a table sample without repeatability.
   *
   * @param isBernoulli      true if Bernoulli style sampling is to be used;
   *                         false for implementation specific sampling
   * @param sampleRate       likelihood of a row appearing in the sample
   */
  public static SqlSampleSpec createTableSample(
      boolean isBernoulli,
      BigDecimal sampleRate) {
    return new SqlTableSampleSpec(isBernoulli, sampleRate);
  }

  @Deprecated // to be removed before 2.0
  public static SqlSampleSpec createTableSample(
      boolean isBernoulli,
      float sampleRate) {
    return createTableSample(isBernoulli, BigDecimal.valueOf(sampleRate));
  }

  /**
   * Creates a table sample with repeatability.
   *
   * @param isBernoulli      true if Bernoulli style sampling is to be used;
   *                         false for implementation specific sampling
   * @param sampleRate       likelihood of a row appearing in the sample
   * @param repeatableSeed   seed value used to reproduce the same sample
   */
  public static SqlSampleSpec createTableSample(
      boolean isBernoulli,
      BigDecimal sampleRate,
      int repeatableSeed) {
    return new SqlTableSampleSpec(isBernoulli, sampleRate, true, repeatableSeed);
  }

  @Deprecated // to be removed before 2.0
  public static SqlSampleSpec createTableSample(
      boolean isBernoulli,
      float sampleRate,
      int repeatableSeed) {
    return createTableSample(isBernoulli, BigDecimal.valueOf(sampleRate),
        repeatableSeed);
  }

  //~ Inner Classes ----------------------------------------------------------

  /** Sample specification that orders substitution. */
  public static class SqlSubstitutionSampleSpec extends SqlSampleSpec {
    private final String name;

    private SqlSubstitutionSampleSpec(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }

    @Override public String toString() {
      return "SUBSTITUTE("
          + CalciteSqlDialect.DEFAULT.quoteStringLiteral(name)
          + ")";
    }
  }

  /** Sample specification. */
  public static class SqlTableSampleSpec extends SqlSampleSpec {
    private final boolean bernoulli;
    public final BigDecimal sampleRate;
    private final boolean repeatable;
    private final int repeatableSeed;

    private SqlTableSampleSpec(boolean bernoulli, BigDecimal sampleRate) {
      this(bernoulli, sampleRate, false, 0);
    }

    private SqlTableSampleSpec(boolean bernoulli, BigDecimal sampleRate,
        boolean repeatable, int repeatableSeed) {
      this.bernoulli = bernoulli;
      this.sampleRate = requireNonNull(sampleRate, "sampleRate");
      this.repeatable = repeatable;
      this.repeatableSeed = repeatableSeed;
    }

    /**
     * Indicates Bernoulli vs. System sampling.
     */
    public boolean isBernoulli() {
      return bernoulli;
    }

    /**
     * Returns the sampling rate.
     * The range is 0.0 to 1.0.
     * 0.0 returns no rows, and 1.0 returns all rows.
     */
    @Deprecated
    public float getSamplePercentage() {
      return sampleRate.floatValue();
    }

    /**
     * Indicates whether repeatable seed should be used.
     */
    public boolean isRepeatable() {
      return repeatable;
    }

    /**
     * Seed to produce repeatable samples.
     */
    public int getRepeatableSeed() {
      return repeatableSeed;
    }

    @Override public String toString() {
      StringBuilder b = new StringBuilder();
      b.append(bernoulli ? "BERNOULLI" : "SYSTEM");
      b.append('(');
      b.append(sampleRate.multiply(ONE_HUNDRED));
      b.append(')');

      if (repeatable) {
        b.append(" REPEATABLE(");
        b.append(repeatableSeed);
        b.append(')');
      }
      return b.toString();
    }
  }
}
