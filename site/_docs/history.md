---
layout: docs
title: History
permalink: "/docs/history.html"
---

<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->

For a full list of releases, see
<a href="https://github.com/apache/calcite/releases">github</a>.
Downloads are available on the
[downloads page]({{ site.baseurl }}/downloads/).

## <a href="https://github.com/apache/calcite/releases/tag/calcite-1.21.0">1.21.0</a> / 2019-09-11
{: #v1-21-0}

This release comes two months after 1.20.0. It includes more than 100 resolved
issues, comprising a large number of new features as well as general improvements
and bug-fixes.

It is worth highlighting that Calcite now:
* supports implicit type coercion in various contexts
  (<a href="https://issues.apache.org/jira/browse/CALCITE-2302">CALCITE-2302</a>);
* allows transformations of Pig Latin scripts into algebraic plans
  (<a href="https://issues.apache.org/jira/browse/CALCITE-3122">CALCITE-3122</a>);
* provides an implementation for the main features of `MATCH_RECOGNIZE` in the
  `Enumerable` convention
  (<a href="https://issues.apache.org/jira/browse/CALCITE-1935">CALCITE-1935</a>);
* supports correlated `ANY`/`SOME`/`ALL` sub-queries
  (<a href="https://issues.apache.org/jira/browse/CALCITE-3031">CALCITE-3031</a>);
* introduces anonymous types based on `ROW`, `ARRAY`, and nested collection
  (<a href="https://issues.apache.org/jira/browse/CALCITE-3233">CALCITE-3233</a>,
  <a href="https://issues.apache.org/jira/browse/CALCITE-3231">CALCITE-3231</a>,
  <a href="https://issues.apache.org/jira/browse/CALCITE-3250">CALCITE-3250</a>);
* brings new join algorithms for the `Enumerable` convention
  (<a href="https://issues.apache.org/jira/browse/CALCITE-2979">CALCITE-2979</a>,
  <a href="https://issues.apache.org/jira/browse/CALCITE-2973">CALCITE-2973</a>,
  <a href="https://issues.apache.org/jira/browse/CALCITE-3284">CALCITE-3284</a>).

Compatibility: This release is tested
on Linux, macOS, Microsoft Windows;
using Oracle JDK 8, 9, 10, 11, 12, 13 and OpenJDK 8, 9, 10, 11, 12, 13;
Guava versions 19.0 to 27.1-jre;
Apache Druid version 0.14.0-incubating;
other software versions as specified in `pom.xml`.

#### Breaking Changes

* Core parser config.fmpp#dataTypeParserMethods should return `SqlTypeNameSpec`
  instead of `SqlIdentifier`.
* The description of converter rules has slightly changed
  (<a href="https://issues.apache.org/jira/browse/CALCITE-3115">CALCITE-3115</a>).
  In some rare cases this may lead to a `Rule description ... is not valid`
  exception. The exception can easily disappear by changing the name of the
  `Convention` which causes the problem.

#### New features

* [<a href="https://issues.apache.org/jira/browse/CALCITE-2973">CALCITE-2973</a>]
  [<a href="https://issues.apache.org/jira/browse/CALCITE-3284">CALCITE-3284</a>]
  Allow joins (hash, semi, anti) that have equi conditions to be executed using a
  hash join algorithm (Lai Zhou)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2302">CALCITE-2302</a>]
  Implicit type cast support
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3122">CALCITE-3122</a>]
  Convert Pig Latin scripts into Calcite relational algebra and Calcite SQL
  (Khai Tran)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2979">CALCITE-2979</a>]
  Add a block-based nested loop join algorithm (Khawla Mouhoubi)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3263">CALCITE-3263</a>]
  Add `MD5`, `SHA1` SQL functions (Shuming Li)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3204">CALCITE-3204</a>]
  Implement `jps` command for OS adapter (Qianjin Xu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3260">CALCITE-3260</a>]
  Add Expressions.evaluate(Node), a public API for evaluating linq4j expressions
  (Wang Yanlin)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3280">CALCITE-3280</a>]
  Add `REGEXP_REPLACE` function in Oracle, MySQL libraries (Shuming Li)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3111">CALCITE-3111</a>]
  Add `RelBuilder.correlate` method, and allow custom implementations of
  `Correlate` in `RelDecorrelator` (Juhwan Kim)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3252">CALCITE-3252</a>]
  Add `CONVERT_TIMEZONE`, `TO_DATE` and `TO_TIMESTAMP` non-standard SQL functions
  (Lindsey Meyer)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3235">CALCITE-3235</a>]
  Add `CONCAT` function for Redshift (Ryan Fu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3250">CALCITE-3250</a>]
  Support nested collection type for `SqlDataTypeSpec`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1935">CALCITE-1935</a>]
  Implement `MATCH_RECOGNIZE` (Julian Feinauer, Zhiqiang-He)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2843">CALCITE-2843</a>]
  Support PostgreSQL cast operator (`::`) (Muhammad Gelbana)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3233">CALCITE-3233</a>]
  Support `ROW` type for `SqlDataTypeSpec`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3231">CALCITE-3231</a>]
  Support `ARRAY` type for `SqlDataTypeSpec`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2624">CALCITE-2624</a>]
  Add a rule to copy a sort below a join operator (Khawla Mouhoubi)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3031">CALCITE-3031</a>]
  Support for correlated `ANY`/`SOME`/`ALL` sub-query (Vineet Garg)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2510">CALCITE-2510</a>]
  Implement `CHR` function (Sergey Tsvetkov, Chunwei Lei)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3176">CALCITE-3176</a>]
  File adapter for parsing JSON files
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3144">CALCITE-3144</a>]
  Add rule, `AggregateCaseToFilterRule`, that converts `SUM(CASE WHEN b THEN x
  END)` to `SUM(x) FILTER (WHERE b)`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2995">CALCITE-2995</a>]
  Implement `DAYNAME`，`MONTHNAME` functions; add `locale` connection property
  (xuqianjin)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2460">CALCITE-2460</a>]
  [CALCITE-2459] Add `TO_BASE64`, `FROM_BASE64` SQL functions (Wenhui Tang)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3063">CALCITE-3063</a>]
  Parse and process PostgreSQL posix regular expressions

#### Bug-fixes, API changes and minor enhancements

* [<a href="https://issues.apache.org/jira/browse/CALCITE-3321">CALCITE-3321</a>]
  Set casing rules for BigQuery SQL dialect (Lindsey Meyer)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3115">CALCITE-3115</a>]
  Cannot add `JdbcRule` instances that have different `JdbcConvention` to same
  `VolcanoPlanner`'s `RuleSet` (Wenhui Tang, Igor Guzenko)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3309">CALCITE-3309</a>]
  Refactor `generatePredicate` method from `EnumerableNestedLoopJoin`,
  `EnumerableHashJoin`, and `EnumerableBatchNestedLoopJoin` into a single location
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3310">CALCITE-3310</a>]
  Approximate and exact aggregate calls are recognized as the same during
  SQL-to-RelNode conversion
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3292">CALCITE-3292</a>]
  SqlToRelConverter#substituteSubQuery fails with NullPointerException when
  converting `SqlUpdate` (Jin Xing)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3297">CALCITE-3297</a>]
  `PigToSqlAggregateRule` should be applied on multi-set projection to produce an
  optimal plan (Igor Guzenko)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3295">CALCITE-3295</a>]
  Add aggregate call name in serialized json string for `RelNode` (Wang Yanlin)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3296">CALCITE-3296</a>]
  Decorrelator shouldn't give empty value when fetch and offset values are null
  in `Sort` rel (Juhwan Kim)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3283">CALCITE-3283</a>]
  `RelSubset` does not contain its best `RelNode` (Xiening Dai)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3210">CALCITE-3210</a>]
  JDBC adapter should generate `CAST(NULL AS type)` rather than `NULL`
  conditionally (Wang Weidong)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3220">CALCITE-3220</a>]
  JDBC adapter now transforms `TRIM` to `TRIM`, `LTRIM` or `RTRIM` when target
  is Hive (Jacky Woo)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3228">CALCITE-3228</a>]
  Error while applying rule `ProjectScanRule`: interpreter
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3223">CALCITE-3223</a>]
  Materialized view fails to match when there is non-`RexInputRef` in the
  projects (Jin Xing)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3257">CALCITE-3257</a>]
  `RelMetadataQuery` cache is not invalidated when log trace is enabled
  (Xiening Dai)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3138">CALCITE-3138</a>]
  `RelStructuredTypeFlattener` doesn't restructure `ROW` type fields (Igor Guzenko)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3251">CALCITE-3251</a>]
  `BinaryExpression` evaluate method support full numeric types in `Primitive`
  (xy2953396112)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3259">CALCITE-3259</a>]
  Align 'Property' in the serialized XML string of `RelXmlWriter` (Wang Yanlin)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3167">CALCITE-3167</a>]
  Make `equals` and `hashCode` methods final in `AbstractRelNode`, and remove
  overriding methods in `EnumerableTableScan` (Jin Xing)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3089">CALCITE-3089</a>]
  Deprecate `EquiJoin`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3267">CALCITE-3267</a>]
  Remove method `SqlDataTypeSpec#deriveType(RelDataTypefactory)`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3214">CALCITE-3214</a>]
  Add `UnionToUnionRule` for materialization matching (refine rule name) (Jin Xing)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3214">CALCITE-3214</a>]
  Add `UnionToUnionRule` for materialization matching (Jin Xing)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3249">CALCITE-3249</a>]
  `Substitution#getRexShuttle does not consider RexLiteral (Jin Xing)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3229">CALCITE-3229</a>]
  `UnsupportedOperationException` for `UPDATE` with `IN` query
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3236">CALCITE-3236</a>]
  Handle issues found in static code analysis (DonnyZone)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3238">CALCITE-3238</a>]
  Support Time Zone suffix of DateTime types for `SqlDataTypeSpec`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3159">CALCITE-3159</a>]
  Remove `DISTINCT` flag from calls to `MIN`, `MAX`, `BIT_OR`, `BIT_AND`
  aggregate functions (xuqianjin)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3237">CALCITE-3237</a>]
  `IndexOutOfBoundsException` when generating deeply nested Java code from linq4j
  (Sahith Nallapareddy)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3234">CALCITE-3234</a>]
  For boolean properties, empty string should mean "true"
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3226">CALCITE-3226</a>]
  `RelBuilder` doesn't keep the alias when `scan` from an expanded view (Jin Xing)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3198">CALCITE-3198</a>]
  Enhance `RexSimplify` to handle `(x <> a or x <> b)`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3101">CALCITE-3101</a>]
  Don't push non-equi join conditions into `Project` below `Join`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3227">CALCITE-3227</a>]
  `IndexOutOfBoundsException` when checking candidate parent match's input
  ordinal in `VolcanoRuleCall`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3177">CALCITE-3177</a>]
  Ensure correct deserialization of relational algebra
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3218">CALCITE-3218</a>]
  Syntax error while parsing `DATEADD` function (which is valid on Redshift)
  (Lindsey Meyer)
* Deprecate `RexBuilder.constantNull()`, because it produces untyped `NULL`
  literals that make planning difficult
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3191">CALCITE-3191</a>]
  In JDBC adapter for MySQL, implement `Values` by generating `SELECT` without
  `FROM`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3147">CALCITE-3147</a>]
  In JDBC adapter, accommodate the idiosyncrasies of how BigQuery (standard SQL)
  quotes character literals and identifiers
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3131">CALCITE-3131</a>]
  In `LatticeSuggester`, record whether columns are used as "dimensions" or
  "measures"
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3175">CALCITE-3175</a>]
  `AssertionError` while serializing to JSON a `RexLiteral` with `Enum` type
  (Wang Yanlin)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3225">CALCITE-3225</a>]
  `JoinToMultiJoinRule` should not match semi- or anti-LogicalJoin
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3215">CALCITE-3215</a>]
  Simplification may have not fully simplified IS `NOT NULL` expressions
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3192">CALCITE-3192</a>]
  Simplification may weaken OR conditions containing inequalities
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3211">CALCITE-3211</a>]
  List of `MutableRel` may fail to be identified by `SubstitutionVisitor` during
  matching (Jin Xing)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3207">CALCITE-3207</a>]
  Fail to convert `Join` with `LIKE` condition to SQL statement (wojustme)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2496">CALCITE-2496</a>]
  Return 0 in case of `EXTRACT(MILLI/MICRO/NANOSECOND FROM date)`
  (Sergey Nuyanzin, Chunwei Lei)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3109">CALCITE-3109</a>]
  Improvements on algebraic operators to express recursive queries (`RepeatUnion`
  and `TableSpool`)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3209">CALCITE-3209</a>]
  When calling `MutableMultiRel.setInput`, exception thrown (Jin Xing)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3195">CALCITE-3195</a>]
  Handle a UDF that throws checked exceptions in the Enumerable code generator
  (DonnyZone)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3118">CALCITE-3118</a>]
  `VolcanoRuleCall` should look at `RelSubset` rather than `RelSet` when checking
  child ordinal of a parent operand (Botong Huang)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3201">CALCITE-3201</a>]
  `SqlValidator` throws exception for SQL insert target table with virtual columns
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3182">CALCITE-3182</a>]
  Trim unused fields for plan of materialized-view before matching (Jin Xing)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3174">CALCITE-3174</a>]
  `IS NOT DISTINCT FROM` condition pushed from `Filter` to `Join` is not
  collapsed (Bohdan Kazydub)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3166">CALCITE-3166</a>]
  Make `RelBuilder` configurable
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3113">CALCITE-3113</a>]
  Equivalent `MutableAggregate`s with different row types should match with each
  other (Jin Xing)
* <a href="https://issues.apache.org/jira/browse/CALCITE-3187">CALCITE-3187</a>:
  Make decimal type inference overridable (Praveen Kumar)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3145">CALCITE-3145</a>]
  `RelBuilder.aggregate` throws `IndexOutOfBoundsException` if `groupKey` is
  non-empty and there are duplicate aggregate functions
* Change type of `SqlStdOperatorTable.GROUPING` field to public class
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3196">CALCITE-3196</a>]
  In `Frameworks`, add `interface BasePrepareAction` (a functional interface) and
  deprecate `abstract class PrepareAction`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3183">CALCITE-3183</a>]
  During field trimming, `Filter` is copied with wrong traitSet (Juhwan Kim)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3189">CALCITE-3189</a>]
  Multiple fixes for Oracle SQL dialect
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3165">CALCITE-3165</a>]
  `Project#accept`(`RexShuttle` shuttle) does not update rowType
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3188">CALCITE-3188</a>]
  `IndexOutOfBoundsException` in `ProjectFilterTransposeRule` when executing
  `SELECT COUNT`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3160">CALCITE-3160</a>]
  Failed to materialize when the aggregate function uses group key (DonnyZone)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3170">CALCITE-3170</a>]
  ANTI join on conditions push down generates wrong plan
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3169">CALCITE-3169</a>]
  decorrelateRel method should return when meeting `SEMI`/`ANTI` join in
  `RelDecorrelator`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3171">CALCITE-3171</a>]
  `SemiJoin` on conditions push down throws `IndexOutOfBoundsException`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3172">CALCITE-3172</a>]
  `RelBuilder#empty` does not keep aliases
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3121">CALCITE-3121</a>]
  `VolcanoPlanner` hangs due to sub-query with dynamic star
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3152">CALCITE-3152</a>]
  Unify throws in SQL parser
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3125">CALCITE-3125</a>]
  Remove completely `class CorrelateJoinType`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3133">CALCITE-3133</a>]
  Remove completely `class SemiJoinType`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3126">CALCITE-3126</a>]
  Remove deprecated `SemiJoin` usage completely
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3146">CALCITE-3146</a>]
  Support the detection of nested aggregations for `JdbcAggregate` in
  `SqlImplementor` (Wenhui Tang)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3155">CALCITE-3155</a>]
  Empty `LogicalValues` can not be converted to `UNION ALL` without operands which
  can not be unparsed (Musbah EL FIL)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3151">CALCITE-3151</a>]
  RexCall's Monotonicity is not considered in determining a Calc's collation
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2801">CALCITE-2801</a>]
  Check input type in `AggregateUnionAggregateRule` when remove the bottom
  `Aggregate` (Hequn Cheng)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3149">CALCITE-3149</a>]
  `RelDataType` CACHE in `RelDataTypeFactoryImpl` can't be garbage collected
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3060">CALCITE-3060</a>]
  `MutableProject` should be generated based on INVERSE_SURJECTION mapping
  (DonnyZone)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3148">CALCITE-3148</a>]
  Validator throws `IndexOutOfBoundsException` for `SqlInsert` when source and
  sink have non-equal number of fields

#### Build and test suite

* [<a href="https://issues.apache.org/jira/browse/CALCITE-3322">CALCITE-3322</a>]
  Remove duplicate test case in `RelMetadataTest`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3314">CALCITE-3314</a>]
  CVSS dependency-check-maven fails for calcite-pig, calcite-piglet,
  calcite-spark
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3315">CALCITE-3315</a>]
  Multiple failures in Druid IT tests due to implicit casts
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3307">CALCITE-3307</a>]
  `PigRelExTest`, `PigRelOpTest` and `PigScriptTest` fail on Windows
* In `SqlFunctionsTest`, replace `assertEquals` and `assertNull` with `assertThat`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3258">CALCITE-3258</a>]
  Upgrade jackson-databind from 2.9.9 to 2.9.9.3, and kafka-clients from 2.0.0
  to 2.1.1
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3222">CALCITE-3222</a>]
  Fix code style issues introduced by [CALCITE-3031] (Vineet Garg)
* More compiler fixes, and cosmetic changes
* Fix compiler warnings
* Update stale tests in DruidAdapter
* Following
  [<a href="https://issues.apache.org/jira/browse/CALCITE-2804">CALCITE-2804</a>],
  fix incorrect expected Druid query in test case
  `DruidAdapterIT#testCastToTimestamp` (Justin Szeluga)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3153">CALCITE-3153</a>]
  Improve testing in `TpcdsTest` using `assertEqual` instead of printing results
* Fix javadoc error
* Fix compilation warnings after Mongo java driver upgrade
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3179">CALCITE-3179</a>]
  Bump Jackson from 2.9.8 to 2.9.9 (Fokko Driesprong)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3157">CALCITE-3157</a>]
  Mongo java driver upgrade: 3.5.0 -> 3.10.2
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3156">CALCITE-3156</a>]
  Mongo adapter. Replace fongo with Mongo Java Server for tests
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3168">CALCITE-3168</a>]
  Add test for invalid literal of SQL parser

#### Web site and documentation

* [<a href="https://issues.apache.org/jira/browse/CALCITE-3303">CALCITE-3303</a>]
  Release Calcite 1.21.0
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3311">CALCITE-3311</a>]
  Add doc to site for implicit type coercion
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3262">CALCITE-3262</a>]
  Refine doc of `SubstitutionVisitor` (Jin Xing)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2835">CALCITE-2835</a>]
  Markdown errors on the Geode adapter page
* Site: Update Apache links on homepage to HTTPS
* Update favicon for new logo
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3136">CALCITE-3136</a>]
  Fix the default rule description of `ConverterRule` (TANG Wen-hui)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3184">CALCITE-3184</a>]
  Add the new logo to the website
* Update example announcement
* Add committer names to 1.20.0 release notes
* Add 1.20.0 release date
* Add 1.20.0 release announcement

## <a href="https://github.com/apache/calcite/releases/tag/calcite-1.20.0">1.20.0</a> / 2019-06-24
{: #v1-20-0}

This release comes three months after 1.19.0. It includes a large number of bug  fixes,
and additional SQL functions. There is now also explicit support for anti-joins.
Several new operators have been added to the algebra to allow support for recursive queries.
An adapter has also been added for [Apache Kafka](https://kafka.apache.org/).

Compatibility: This release is tested
on Linux, macOS, Microsoft Windows;
using Oracle JDK 8, 9, 10, 11, 12, 13 and OpenJDK 8, 9, 10, 11, 12, 13;
Guava versions 19.0 to 27.1-jre;
Apache Druid version 0.14.0-incubating;
other software versions as specified in `pom.xml`.


#### Breaking Changes

* Make `EnumerableMergeJoin` extend `Join` instead of `EquiJoin`
* `Correlate` use `JoinRelType` instead of `SemiJoinType`
* Rename `EnumerableThetaJoin` to `EnumerableNestedLoopJoin`
* Rename `EnumerableJoin` to `EnumerableHashJoin`
* Remove `SemiJoinFactory` from `RelBuilder`, method `semiJoin` now returns a `LogicalJoin`
  with join type `JoinRelType.SEMI` instead of a `SemiJoin`
* Rules: `SemiJoinFilterTransposeRule`, `SemiJoinJoinTransposeRule`, `SemiJoinProjectTransposeRule`
  and `SemiJoinRemoveRule` match `LogicalJoin` with join type `SEMI` instead of `SemiJoin`.
* `SemiJoin`, `EnumerableSemiJoin`, `SemiJoinType` and `CorrelateJoinType`, and methods that use them,
  are deprecated for quick removal in 1.21
* The Elasticsearch adapter no longer supports [Elasticsearch types](
    https://www.elastic.co/guide/en/elasticsearch/reference/7.0/removal-of-types.html).
  Calcite table names will reflect index names in Elasticsearch (as opposed to types).
  We recommend use of Elasticsearch 6.2 (or later) with Calcite.

#### New features

* [<a href='https://issues.apache.org/jira/browse/CALCITE-2822'>CALCITE-2822</a>] Allow `MultiJoin` rules with any project/filter (Siddharth Teotia)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2968'>CALCITE-2968</a>] New `AntiJoin` relational expression
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2721'>CALCITE-2721</a>] Support parsing record-type [DOT] member-functions
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3005'>CALCITE-3005</a>] Implement string functions: `LEFT`, `RIGHT` (xuqianjin)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2812'>CALCITE-2812</a>] Add algebraic operators to allow expressing recursive queries
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2913'>CALCITE-2913</a>] Adapter for Apache Kafka (Mingmin Xu)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3084'>CALCITE-3084</a>] Implement JDBC string functions: `ASCII`, `REPEAT`, `SPACE`, `SOUNDEX`, `DIFFERENC` (pingle wang)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2985'>CALCITE-2985</a>] Implement `JSON_STORAGE_SIZE` function (xuqianjin)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2601'>CALCITE-2601</a>] Add `REVERSE` function (pingle wang)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2712'>CALCITE-2712</a>] Add rule to remove null-generating side of a Join
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2965'>CALCITE-2965</a>] Implement string functions: `REPEAT`, `SPACE`, `SOUNDEX`, `DIFFERENCE`
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2975'>CALCITE-2975</a>] Implement `JSON_REMOVE` function (xuqianjin)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2933'>CALCITE-2933</a>] Add timestamp extract for casts from timestamp type to other types
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3011'>CALCITE-3011</a>] Support left and right outer joins with `AggregateJoinTransposeRule` (Vineet Garg)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2427'>CALCITE-2427</a>] Allow sub-queries in DML statements (Pressenna Sockalingasamy)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2914'>CALCITE-2914</a>] Add a new statistic provider, to improve how `LatticeSuggester` deduces foreign keys
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2754'>CALCITE-2754</a>] Implement `LISTAGG` function (Sergey Nuyanzin, Chunwei Lei)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-1172'>CALCITE-1172</a>] Add rule to flatten two Aggregate operators into one
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2892'>CALCITE-2892</a>] Add the `JSON_KEYS` function (xuqianjin)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-883'>CALCITE-883</a>] Support `RESPECT NULLS`, `IGNORE NULLS` option for `LEAD`, `LAG`, `FIRST_VALUE`, `LAST_VALUE`, `NTH_VALUE` functions (Chunwei Lei)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2920'>CALCITE-2920</a>] In `RelBuilder`, add `antiJoin` method (Ruben Quesada Lopez)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-1515'>CALCITE-1515</a>] In `RelBuilder`, add `functionScan` method to create `TableFunctionScan` (Chunwei Lei)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2658'>CALCITE-2658</a>] Add `ExchangeRemoveConstantKeysRule` that removes constant keys from `Exchange` or `SortExchange` (Chunwei Lei)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2729'>CALCITE-2729</a>] Introducing `WindowReduceExpressionsRule` (Chunwei Lei)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2808'>CALCITE-2808</a>] Add the `JSON_LENGTH` function (xuqianjin)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-589'>CALCITE-589</a>] Extend `unifyAggregates` method to work with Grouping Sets
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2908'>CALCITE-2908</a>] Implement SQL `LAST_DAY` function (Chunwei Lei)

#### Bug-fixes, API changes and minor enhancements

* [<a href='https://issues.apache.org/jira/browse/CALCITE-3119'>CALCITE-3119</a>] Deprecate Linq4j `CorrelateJoinType` (in favor of `JoinType`)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3087'>CALCITE-3087</a>] `AggregateOnProjectToAggregateUnifyRule` ignores Project incorrectly when its Mapping breaks ordering (DonnyZone)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2744'>CALCITE-2744</a>] Remove usage of deprecated API in `MockSqlOperatorTable`
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3123'>CALCITE-3123</a>] In `RelBuilder`, eliminate duplicate aggregate calls
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3116'>CALCITE-3116</a>] Upgrade to Avatica 1.15
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2744'>CALCITE-2744</a>] `RelDecorrelator` use wrong output map for `LogicalAggregate` decorrelate (godfreyhe and Danny Chan)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2804'>CALCITE-2804</a>] Fix casting to timestamps in Druid
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3107'>CALCITE-3107</a>] Upgrade commons-dbcp2 from 2.5.0 to 2.6.0 (Fokko Driesprong)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3106'>CALCITE-3106</a>] Upgrade commons-pool2 from 2.6.0 to 2.6.2 (Fokko Driesprong)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2944'>CALCITE-2944</a>] Deprecate Aggregate indicator and remove fields where possible
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3098'>CALCITE-3098</a>] Upgrade SQLLine to 1.8.0
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2742'>CALCITE-2742</a>] Read values of `USER` and `SYSTEM_USER` variables from `DataContext` (Siddharth Teotia, Jacques Nadeau)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3082'>CALCITE-3082</a>] Fix NPE in `SqlUtil#getSelectListItem`
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3093'>CALCITE-3093</a>] Remove JDBC connection calls from `PlannerImpl`
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3095'>CALCITE-3095</a>] Add several system properties to control enabling/disabling of rules and traits
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2696'>CALCITE-2696</a>] Improve design of join-like relational expressions
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3097'>CALCITE-3097</a>] GROUPING SETS breaks on sets of size > 1 due to precedence issues (Steven Talbot)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3022'>CALCITE-3022</a>] Babel: Various SQL parsing issues
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3047'>CALCITE-3047</a>] In JDBC adapter, expose multiple schemas of the back-end database
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3048'>CALCITE-3048</a>] Improve how JDBC adapter deduces current schema on Redshift
* Javadoc typos (Wenhui Tang, Muhammad Gelbana)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3096'>CALCITE-3096</a>] In `RelBuilder`, make alias method idempotent
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3055'>CALCITE-3055</a>] Use pair of `relNode`'s `rowType` and digest as unique key for cache in `RelOptPlanner` (KazydubB)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3077'>CALCITE-3077</a>] Rewrite `CUBE`&`ROLLUP` queries in `SparkSqlDialect` (DonnyZone)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3090'>CALCITE-3090</a>] Remove Central configuration
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2807'>CALCITE-2807</a>] Fix `IS NOT DISTINCT FROM` expression identification in `RelOptUtil#pushDownJoinConditions`()
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3050'>CALCITE-3050</a>] Integrate `SqlDialect` and `SqlParser.Config`
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3023'>CALCITE-3023</a>] Upgrade elastic search to 7.x (Takako Shimamoto)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3067'>CALCITE-3067</a>] Splunk adapter cannot parse right session keys from Splunk 7.2 (Shawn Chen)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3076'>CALCITE-3076</a>] `AggregateJoinTransposeRule` throws error for unique under aggregate keys when generating merged calls
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3068'>CALCITE-3068</a>] `testSubprogram()` does not test whether subprogram gets re-executed
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3072'>CALCITE-3072</a>] Generate right SQL for `FLOOR&SUBSTRING` functions in `SparkSqlDialect` (DonnyZone)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3074'>CALCITE-3074</a>] Move MySQL's JSON operators to `SqlLibraryOperators`
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3062'>CALCITE-3062</a>] Do not populate `provenanceMap` if not debug
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2282'>CALCITE-2282</a>] Remove sql operator table from parser
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3052'>CALCITE-3052</a>] Error while applying rule `MaterializedViewAggregateRule`(Project-Aggregate): `ArrayIndexOutOfBoundsException`
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3066'>CALCITE-3066</a>] `RelToSqlConverter` may incorrectly throw an `AssertionError` for some decimal literals
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3028'>CALCITE-3028</a>] Support FULL OUTER JOIN with `AggregateJoinTransposeRule` (Vineet Garg)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3017'>CALCITE-3017</a>] Improve null handling of `JsonValueExpressionOperator`
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2936'>CALCITE-2936</a>] Simplify EXISTS or NOT EXISTS sub-query that has "GROUP BY ()"
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2803'>CALCITE-2803</a>] `ProjectTransposeJoinRule` messes INDF expressions
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3061'>CALCITE-3061</a>] Query with WITH clause fails when alias is the same as the table with rolled up column
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3017'>CALCITE-3017</a>] Re-organize how we represent built-in operators that are not in the standard operator table
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3056'>CALCITE-3056</a>] Elasticsearch adapter. Invalid result with cast function on raw queries
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3046'>CALCITE-3046</a>] `CompileException` when inserting casted value of composited user defined type into table
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3054'>CALCITE-3054</a>] Elasticsearch adapter. Avoid scripting for simple projections
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3039'>CALCITE-3039</a>] In Interpreter, min() incorrectly returns maximum double value (dijkspicy)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3049'>CALCITE-3049</a>] When simplifying "IS NULL" and "IS NOT NULL", simplify the operand first
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3003'>CALCITE-3003</a>] `AssertionError` when GROUP BY nested field (Will Yu)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3012'>CALCITE-3012</a>] Column uniqueness metadata provider may return wrong result for `FULL OUTER JOIN` operator (Vineet Garg)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3045'>CALCITE-3045</a>] `NullPointerException` when casting null literal to composite user defined type
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3030'>CALCITE-3030</a>] `SqlParseException` when using component identifier for setting in merge statements (Danny Chan)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3029'>CALCITE-3029</a>] Java-oriented field type is wrongly forced to be NOT NULL after being converted to SQL-oriented
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2292'>CALCITE-2292</a>] Query result is wrong when table is implemented with `FilterableTable` and the sql has multiple where conditions
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2998'>CALCITE-2998</a>] `RexCopier` should support all rex types (Chunwei Lei, Alexander Shilov)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2982'>CALCITE-2982</a>] `SqlItemOperator` should throw understandable exception message for incorrect operand type (pengzhiwei)
* Revert "[<a href='https://issues.apache.org/jira/browse/CALCITE-3021'>CALCITE-3021</a>] `ArrayEqualityComparer` should use `Arrays#deepEquals`/`deepHashCode` instead of `Arrays#equals`/`hashCode` (Ruben Quesada Lopez)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3021'>CALCITE-3021</a>] `ArrayEqualityComparer` should use `Arrays#deepEquals`/`deepHashCode` instead of `Arrays#equals`/`hashCode`
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2453'>CALCITE-2453</a>] Parse list of SQL statements separated with a semicolon (Chunwei Lei, charbel yazbeck)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3004'>CALCITE-3004</a>] `RexOver` is incorrectly pushed down in `ProjectSetOpTransposeRule` and `ProjectCorrelateTransposeRule` (Chunwei Lei)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3001'>CALCITE-3001</a>] Upgrade to Apache Druid 0.14.0-incubating
* Following [<a href='https://issues.apache.org/jira/browse/CALCITE-3010'>CALCITE-3010</a>], remove redundant non-reserved keyword definitions
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2993'>CALCITE-2993</a>] `ParseException` may be thrown for legal SQL queries due to incorrect "LOOKAHEAD(1)" hints
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3010'>CALCITE-3010</a>] In SQL parser, move `JsonValueExpression` into Expression
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3009'>CALCITE-3009</a>] `DiffRepository` should ensure that XML resource file does not contain duplicate test names
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2986'>CALCITE-2986</a>] Wrong results with `= ANY` sub-query (Vineet Garg)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2962'>CALCITE-2962</a>] `RelStructuredTypeFlattener` generates wrong types for nested column when `flattenProjection` (Will Yu)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3007'>CALCITE-3007</a>] Type mismatch for `ANY` sub-query in project (Vineet Garg)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2865'>CALCITE-2865</a>] `FilterProjectTransposeRule` generates wrong `traitSet` when `copyFilter`/`Project` is true (Ruben Quesada Lopez)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2343'>CALCITE-2343</a>] `PushProjector` with OVER expression causes infinite loop (Chunwei Lei)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2994'>CALCITE-2994</a>] Least restrictive type among structs does not consider nullability
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2991'>CALCITE-2991</a>] `getMaxRowCount` should return 1 for an Aggregate with constant keys (Vineet Garg)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-1338'>CALCITE-1338</a>] `JoinProjectTransposeRule` should not pull a literal up through the null-generating side of a join (Chunwei Lei)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2977'>CALCITE-2977</a>] Exception is not thrown when there are ambiguous field in select list
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2739'>CALCITE-2739</a>] NPE is thrown if the DEFINE statement contains IN in `MATCH_RECOGNIZE`
* [<a href='https://issues.apache.org/jira/browse/CALCITE-896'>CALCITE-896</a>] Remove Aggregate if grouping columns are unique and all functions are splittable
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2456'>CALCITE-2456</a>] `VolcanoRuleCall` doesn't match unordered child operand when the operand is not the first operand. `PruneEmptyRules` `UNION` and `MINUS` with empty inputs cause infinite cycle. (Zuozhi Wang)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2847'>CALCITE-2847</a>] Optimize global LOOKAHEAD for SQL parsers
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2976'>CALCITE-2976</a>] Improve materialized view rewriting coverage with disjunctive predicates
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2954'>CALCITE-2954</a>] `SubQueryJoinRemoveRule` and `SubQueryProjectRemoveRule` passing on empty set instead of set of correlation id (Vineet Garg)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2930'>CALCITE-2930</a>] `IllegalStateException` when `FilterCorrelateRule` matches a SEMI or ANTI Correlate (Ruben Quesada Lopez)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2004'>CALCITE-2004</a>] Push join predicate down into inner relation for lateral join
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2820'>CALCITE-2820</a>] Avoid reducing certain aggregate functions when it is not necessary (Siddharth Teotia)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2928'>CALCITE-2928</a>] When resolving user-defined functions (UDFs), use the case-sensitivity of the current connection (Danny Chan)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2900'>CALCITE-2900</a>] `RelStructuredTypeFlattener` generates wrong types on nested columns (Will Yu)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2941'>CALCITE-2941</a>] `EnumerableLimitRule` on Sort with no collation creates `EnumerableLimit` with wrong `traitSet` and `cluster` (Ruben Quesada Lopez)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2909'>CALCITE-2909</a>] Optimize Enumerable `SemiJoin` with lazy computation of `innerLookup` (Ruben Quesada Lopez)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2903'>CALCITE-2903</a>] Exception thrown when decorrelating query with `TEMPORAL TABLE`
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2958'>CALCITE-2958</a>] Upgrade SQLLine to 1.7.0
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2796'>CALCITE-2796</a>] JDBC adapter fix for `ROLLUP` on MySQL 5
* In `RelFieldCollation`, add a `withX` copy method
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2953'>CALCITE-2953</a>] `LatticeTest.testTileAlgorithm2` and `LatticeTest.testTileAlgorithm3` fail intermittently
* [<a href='https://issues.apache.org/jira/browse/CALCITE-574'>CALCITE-574</a>] Remove `org.apache.calcite.util.Bug.CALCITE_461_FIXED`
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2951'>CALCITE-2951</a>] Support decorrelating a sub-query that has aggregate with grouping sets (Haisheng Yuan)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2946'>CALCITE-2946</a>] `RelBuilder` wrongly skips creation of Aggregate that prunes columns if input produces one row at most
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2942'>CALCITE-2943</a>] Materialized view rewriting logic calls `getApplicableMaterializations` each time the rule is triggered
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2942'>CALCITE-2942</a>] Materialized view rewriting logic instantiates `RelMetadataQuery` each time the rule is triggered

#### Build and test suite

* Fix test exception caused by slightly different error message from regex in JDK 13
* Following [<a href='https://issues.apache.org/jira/browse/CALCITE-2812'>CALCITE-2812</a>] Disable parallel execution of parameterized test to avoid hanging
* [<a href='https://issues.apache.org/jira/browse/CALCITE-35'>CALCITE-35</a>] More test cases to guard against providing a broken fix for parenthesized join (Muhammad Gelbana)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3034'>CALCITE-3034</a>] CSV test case description does not match it's code logic (FaxianZhao)
* Mongo adapter. Mongo checker validates only first line of the Bson query in tests
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3053'>CALCITE-3053</a>] Add a test to ensure that all functions are documented in the SQL reference
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2961'>CALCITE-2961</a>] Enable Travis to test against JDK 13

#### Web site and documentation

* [<a href='https://issues.apache.org/jira/browse/CALCITE-2952'>CALCITE-2952</a>] Document JDK 12 support
* Site: Add Danny Chan as committer
* Site: Improve contribution guidelines for JIRA
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2846'>CALCITE-2846</a>] Document Oracle-specific functions, such as `NVL` and `LTRIM`, in the SQL reference
* Site: Add new committers and PMC (Chunwei Lei, Ruben Quesada Lopez, Zhiwei Peng and Stamatis Zampetakis)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-3006'>CALCITE-3006</a>] Example code on site cannot compile (Chunwei Lei)
* Site: Add guidelines for JIRA's fix version field
* Site: Update content of "Not implemented" since JSON_LENGH has already been added
* Site: Improve documentation for MySQL-specific JSON operators
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2927'>CALCITE-2927</a>] The Javadoc and implement of `RuleQueue.computeImportance()` is inconsistent (Meng Wang)
* Update instructions for publishing site; we previously used subversion, now we use git
* Site: Add Alibaba MaxCompute to powered-by page
* Site: Add new committers (Haisheng Yuan, Hongze Zhang and Stamatis Zampetakis)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2952'>CALCITE-2952</a>] Add JDK 12 as tested to 1.19.0 history

## <a href="https://github.com/apache/calcite/releases/tag/calcite-1.19.0">1.19.0</a> / 2019-03-25
{: #v1-19-0}

This release comes three months after 1.18.0. It includes more than 80 resolved
issues, comprising of a few new features as well as general improvements
and bug-fixes. Among others, there have been significant improvements in JSON
query support.

Compatibility: This release is tested
on Linux, macOS, Microsoft Windows;
using Oracle JDK 8, 9, 10, 11, 12 and OpenJDK 8, 9, 10, 11, 12;
Guava versions 19.0 to 27.1-jre;
Druid version 0.11.0;
other software versions as specified in `pom.xml`.

#### New features

* [<a href='https://issues.apache.org/jira/browse/CALCITE-1912'>CALCITE-1912</a>]
  Support `FOR SYSTEM_TIME AS OF` in regular queries
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2786'>CALCITE-2786</a>]
  Add order by clause support for `JSON_ARRAYAGG`
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2791'>CALCITE-2791</a>]
  Add the `JSON_TYPE` function
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2864'>CALCITE-2864</a>]
  Add the `JSON_DEPTH` function
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2881'>CALCITE-2881</a>]
  Add the `JSON_PRETTY` function
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2770'>CALCITE-2770</a>]
  Add bitwise aggregate functions `BIT_AND`, `BIT_OR`
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2799'>CALCITE-2799</a>]
  Allow alias in `HAVING` clause for aggregate functions

#### Bug-fixes, API changes and minor enhancements

* [<a href='https://issues.apache.org/jira/browse/CALCITE-1513'>CALCITE-1513</a>]
  Correlated `NOT IN` query throws `AssertionError`
* [<a href='https://issues.apache.org/jira/browse/CALCITE-1726'>CALCITE-1726</a>]
  Sub-query in `FILTER` is left untransformed
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2249'>CALCITE-2249</a>]
  `AggregateJoinTransposeRule` generates non-equivalent nodes if `Aggregate`
  contains a `DISTINCT` aggregate function
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2288'>CALCITE-2288</a>]
  Type assertion error when reducing partially-constant expression
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2290'>CALCITE-2290</a>]
  Type mismatch during flattening
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2301'>CALCITE-2301</a>]
  JDBC adapter: use query timeout from the top-level statement
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2338'>CALCITE-2338</a>]
  Make simplification API more conservative
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2344'>CALCITE-2344</a>]
  Avoid inferring `$0 = null` predicate from `$0 IS NULL` when `$0` is not
  nullable
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2375'>CALCITE-2375</a>]
  `EnumerableDefaults.join_()` leaks connections
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2437'>CALCITE-2437</a>]
  `FilterMultiJoinMergeRule` doesn't combine postFilterCondition
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2454'>CALCITE-2454</a>]
  Avoid treating `Project(x=1)` and `Project(x=1)` equal when the type of `1` is
  `int` in the first rel and `long` in the second
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2463'>CALCITE-2463</a>]
  Silence ERROR logs from `CalciteException`, `SqlValidatorException`
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2464'>CALCITE-2464</a>]
  Allow to set nullability for columns of structured types
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2471'>CALCITE-2471</a>]
  `RelNode` description includes all tree when recomputed
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2554'>CALCITE-2554</a>]
  Enrich enumerable join operators with order-preserving information
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2582'>CALCITE-2582</a>]
  `FilterProjectTransposeRule` does not always simplify the new filter condition
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2599'>CALCITE-2599</a>]
  Support `ASCII(string)` in `SqlFunctions`
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2621'>CALCITE-2621</a>]
  Add rule to execute semi-joins with correlation
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2623'>CALCITE-2623</a>]
  Add specific translation for `POSITION`, `MOD` and set operators in BigQuery
  and Hive SQL dialects
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2625'>CALCITE-2625</a>]
  `ROW_NUMBER`, `RANK` generating invalid SQL
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2629'>CALCITE-2629</a>]
  Unnecessary call to `CatalogReader#getAllSchemaObjects` in `CatalogScope`
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2635'>CALCITE-2635</a>]
  `getMonotonocity` is slow on wide tables
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2674'>CALCITE-2674</a>]
  `SqlIdentifier` same name with built-in function but with escape character
  should be still resolved as an identifier
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2677'>CALCITE-2677</a>]
  Struct types with one field are not mapped correctly to Java classes
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2703'>CALCITE-2703</a>]
  Reduce code generation and class loading overhead when executing queries in
  `EnumerableConvention`
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2722'>CALCITE-2722</a>]
  `SqlImplementor.createLeftCall` method throws `StackOverflowError`
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2727'>CALCITE-2727</a>]
  Materialized view rewriting bails out incorrectly when a view does not contain
  any table reference
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2733'>CALCITE-2733</a>]
  Use `catalog` and `schema` from JDBC connect string to retrieve tables if specified
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2750'>CALCITE-2750</a>]
  `PI` operator is incorrectly identified as dynamic function
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2755'>CALCITE-2755</a>]
  Expose document `_id` field when querying ElasticSearch
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2762'>CALCITE-2762</a>]
  Quidem env variable is always false if its name is separated by dot(".")
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2778'>CALCITE-2778</a>]
  Remove `ClosableAllocation`, `ClosableAllocationOwner`,
  `CompoundClosableAllocation`
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2782'>CALCITE-2782</a>]
  Use server time zone by default if time zone is not specified in the user connection string
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2783'>CALCITE-2783</a>]
  "COALESCE(s, TRUE) = TRUE" and "(s OR s IS UNKNOWN) = TRUE" causes
  `NullPointerException`
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2785'>CALCITE-2785</a>]
  In `EnumerableAggregate`, wrong result produced If there are sorted aggregates
  and non-sorted aggregates at the same time
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2787'>CALCITE-2787</a>]
  JSON aggregate calls with different null clause get incorrectly merged while
  converting from SQL to relational algebra
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2790'>CALCITE-2790</a>]
  `AggregateJoinTransposeRule` incorrectly pushes down distinct count into join
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2797'>CALCITE-2797</a>]
  Support `APPROX_COUNT_DISTINCT` aggregate function in ElasticSearch
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2798'>CALCITE-2798</a>]
  Optimizer should remove `ORDER BY` in sub-query, provided it has no `LIMIT` or
  `OFFSET`
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2802'>CALCITE-2802</a>]
  In Druid adapter, use of range conditions like `'2010-01-01' < TIMESTAMP` leads
  to incorrect results
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2805'>CALCITE-2805</a>]
  Can't specify port with Cassandra adapter in connection string
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2806'>CALCITE-2806</a>]
  Cassandra adapter doesn't allow uppercase characters in table names
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2811'>CALCITE-2811</a>]
  Update version of Cassandra driver
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2814'>CALCITE-2814</a>]
  In ElasticSearch adapter, fix `GROUP BY` when using raw item access
  (e.g. `_MAP*` ['a.b.c'])
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2817'>CALCITE-2817</a>]
  Make `CannotPlanException` more informative
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2827'>CALCITE-2827</a>]
  Allow `CONVENTION.NONE` planning with `VolcanoPlanner`
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2838'>CALCITE-2838</a>]
  Simplification: Remove redundant `IS TRUE` checks
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2839'>CALCITE-2839</a>]
  Simplify comparisons against `BOOLEAN` literals
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2840'>CALCITE-2840</a>]
  `RexNode` simplification logic should use more specific `UnknownAs` modes
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2841'>CALCITE-2841</a>]
  Simplification: push negation into Case expression
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2842'>CALCITE-2842</a>]
  Computing `RexCall` digest containing `IN` expressions leads to exceptions
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2848'>CALCITE-2848</a>]
  Simplifying a CASE statement&#39;s first branch should ignore its safety
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2850'>CALCITE-2850</a>]
  Geode adapter: support `BOOLEAN` column as filter operand
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2852'>CALCITE-2852</a>]
  RexNode simplification does not traverse unknown functions
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2856'>CALCITE-2856</a>]
  Emulating `COMMA JOIN` as `CROSS JOIN` for `SparkSqlDialect`
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2858'>CALCITE-2858</a>]
  Improvements in JSON writer and reader for plans
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2859'>CALCITE-2859</a>]
  Centralize Calcite system properties
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2863'>CALCITE-2863</a>]
  In ElasticSearch adapter, query fails when filtering directly on `_MAP`
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2887'>CALCITE-2887</a>]
  Improve performance of `RexLiteral.toJavaString()`
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2897'>CALCITE-2897</a>]
  Reduce expensive calls to `Class.getSimpleName()`
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2899'>CALCITE-2899</a>]
  Deprecate `RelTraitPropagationVisitor` and remove its usages
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2890'>CALCITE-2890</a>]
  In ElasticSearch adapter, combine `any_value` with other aggregation functions
  failed
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2891'>CALCITE-2891</a>]
  Alias suggester failed to suggest name based on original name incrementally
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2894'>CALCITE-2894</a>]
  `RelMdPercentageOriginalRows` throws `NullPointerException` when explaining
  plan with all attributes
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2902'>CALCITE-2902</a>]
  Improve performance of `AbstractRelNode.computeDigest()`
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2929'>CALCITE-2929</a>]
  Simplification of `IS NULL` checks are incorrectly assuming that `CAST`s are
  possible
* Improve Graphviz dump in `CannotPlanException`: make boxes shorter, print
  composite traits if they were simplified
* Make `SparkHandlerImpl` singleton thread-safe
* Remove usage of `userConfig` attribute in ElasticSearch adapter
* In ElasticSearch adapter, remove dead (or unnecessary) code

#### Build and test suite

* [<a href='https://issues.apache.org/jira/browse/CALCITE-2732'>CALCITE-2732</a>]
  Upgrade PostgreSQL driver version
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2759'>CALCITE-2759</a>]
  Update `maven-remote-resources-plugin` to 1.6.0
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2765'>CALCITE-2765</a>]
  Bump Janino compiler dependency to 3.0.11
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2768'>CALCITE-2768</a>]
  `PlannerTest` ignores top-level `ORDER BY` clause (`RootRel.collation`)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2788'>CALCITE-2788</a>]
  Building error for sub-project of calcite on `maven-checkstyle-plugin`
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2779'>CALCITE-2779</a>]
  Remove references to `StringBuffer`
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2875'>CALCITE-2875</a>]
  Some misspellings in `RelOptListener`
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2895'>CALCITE-2895</a>]
  Some arguments are undocumented in constructor of `LogicalAggregate`
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2836'>CALCITE-2836</a>]
  Remove `maven-compiler-plugin` from `calcite-plus` module `pom.xml`
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2878'>CALCITE-2878</a>]
  Avoid `throw new RuntimeException(e)` in tests
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2916'>CALCITE-2916</a>]
  Upgrade jackson to 2.9.8
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2925'>CALCITE-2925</a>]
  Exclude `maven-wrapper.jar` from source distribution
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2931'>CALCITE-2931</a>]
  In Mongo adapter, compare Bson (not string) query representation in tests
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2932'>CALCITE-2932</a>]
  `DruidAdapterIT` regression after 1.17 release
* Improve messages for tests based on `CalciteAssert`
* Add JUnit category for extremely slow tests, launch them in a separate Travis job
* Fix sqlline by removing redundant slf4j dependency (`log4j-over-slf4j`) from
  `cassandra-all`

#### Web site and documentation

* Switch from `maven:alpine` to `maven` image for generating javadoc when
  building the site
* Update instructions for pushing to the git site repository
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2734'>CALCITE-2734</a>]
  Site: Update mongo documentation to reflect filename changes
* Site: Add commit message guidelines for contributors (Stamatis Zampetakis)
* Site: Add Zoltan Haindrich as committer
* Site: Elastic query example on `_MAP`
* Site: fix JSON syntax error at file adapter page (Marc Prud'hommeaux)
* Site: fix typo at the main page (Marc Prud'hommeaux)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2436'>CALCITE-2436</a>]
  Steps for building site under Windows; fix misprint in SQL Language page
* Site: News item for release 1.18
* Site: Rename MapD to OmniSci, and update logos
* Update site for new repository
* Update git URL
* Site: ElasticAdapter mention supported versions (and support schedule)
* Site: Improve documentation for ElasticSearch Adapter
* Site: Update PMC chair
* Update year in NOTICE

## <a href="https://github.com/apache/calcite/releases/tag/calcite-1.18.0">1.18.0</a> / 2018-12-21
{: #v1-18-0}

With over 200 commits from 36 contributors, this is the largest
Calcite release ever. To the SQL dialect, we added
<a href="https://issues.apache.org/jira/browse/CALCITE-2266">JSON
functions</a> and
<a href="https://issues.apache.org/jira/browse/CALCITE-2402">linear
regression functions</a>, the
<a href="https://issues.apache.org/jira/browse/CALCITE-2224">WITHIN
GROUP</a> clause for aggregate functions; there is a new
<a href="https://issues.apache.org/jira/browse/CALCITE-1870">utility
to recommend lattices based on past queries</a>,
and improvements to expression simplification, the SQL advisor,
and the Elasticsearch and Apache Geode adapters.

Compatibility: This release is tested
on Linux, macOS, Microsoft Windows;
using Oracle JDK 8, 9, 10, 11 and OpenJDK 10, 11;
Guava versions 19.0 to 27.0.1-jre;
Druid version 0.11.0;
other software versions as specified in `pom.xml`.

#### New features

* [<a href="https://issues.apache.org/jira/browse/CALCITE-2662">CALCITE-2662</a>]
  In `Planner`, allow parsing a stream (`Reader`) instead of a `String`
  (Enrico Olivelli)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2699">CALCITE-2699</a>]
  `TIMESTAMPADD` function now applies to `DATE` and `TIME` as well as `TIMESTAMP`
  (xuqianjin)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-563">CALCITE-563</a>]
  In JDBC adapter, push bindable parameters down to the underlying JDBC data
  source (Vladimir Sitnikov, Piotr Bojko)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2663">CALCITE-2663</a>]
  In DDL parser, add `CREATE` and `DROP FUNCTION` (ambition119)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2266">CALCITE-2266</a>]
  Implement SQL:2016 JSON functions: `JSON_EXISTS`, `JSON_VALUE`, `JSON_QUERY`,
  `JSON_OBJECT`, `JSON_OBJECTAGG`, `JSON_ARRAY`, `JSON_ARRAYAGG`, `x IS JSON`
  predicate (Hongze Zhang)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2224">CALCITE-2224</a>]
  Support `WITHIN GROUP` clause for aggregate functions (Hongze Zhang)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2405">CALCITE-2405</a>]
  In Babel parser, make 400 reserved keywords including `YEAR`, `SECOND`, `DESC`
  non-reserved
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1870">CALCITE-1870</a>]
  Lattice suggester
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2571">CALCITE-2571</a>]
  `TRIM` function now trims more than one character (Andrew Pilloud)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2112">CALCITE-2112</a>]
  Add Maven wrapper for Calcite (Ratandeep S. Ratti)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1026">CALCITE-1026</a>]
  Allow models in YAML format
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2402">CALCITE-2402</a>]
  Implement regression functions: `COVAR_POP`, `COVAR_SAMP`, `REGR_COUNT`,
  `REGR_SXX`, `REGR_SYY`
* SQL advisor (`SqlAdvisor`):
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2479">CALCITE-2479</a>]
    Automatically quote identifiers that look like SQL keywords
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2478">CALCITE-2478</a>]
    Purge `from_clause` when `_suggest_` token is located in one of the
    `FROM` sub-queries
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2477">CALCITE-2477</a>]
    Scalar sub-queries
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2476">CALCITE-2476</a>]
    Produce hints when sub-query with `*` is present in query
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2475">CALCITE-2475</a>]
    Support `MINUS`
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2473">CALCITE-2473</a>]
    Support `--` comments
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2434">CALCITE-2434</a>]
    Hints for nested tables and schemas
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2433">CALCITE-2433</a>]
    Configurable quoting characters
* Relational algebra builder (`RelBuilder`):
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2661">CALCITE-2661</a>]
    Add methods for creating `Exchange` and `SortExchange`
    relational expressions (Chunwei Lei)
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2654">CALCITE-2654</a>]
    Add a fluent API for building complex aggregate calls
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2441">CALCITE-2441</a>]
    `RelBuilder.scan` should expand `TranslatableTable` and views
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2647">CALCITE-2647</a>]
    Add a `groupKey` method that assumes only one grouping set
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2470">CALCITE-2470</a>]
    `project` method should combine expressions if the underlying
    node is a `Project`
* Elasticsearch adapter:
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2679">CALCITE-2679</a>]
    Implement `DISTINCT` and `GROUP BY` without aggregate functions (Siyuan Liu)
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2689">CALCITE-2689</a>]
    Allow grouping on non-textual fields like `DATE` and `NUMBER`
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2651">CALCITE-2651</a>]
    Enable scrolling for basic search queries
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2585">CALCITE-2585</a>]
    Support `NOT` operator
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2578">CALCITE-2578</a>]
    Support `ANY_VALUE` aggregate function
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2528">CALCITE-2528</a>]
    Support `Aggregate` (Andrei Sereda)
* Apache Geode adapter:
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2709">CALCITE-2709</a>]
    Allow filtering on `DATE`, `TIME`, `TIMESTAMP` fields (Sandeep Chada)
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2671">CALCITE-2671</a>]
    `GeodeFilter` now converts multiple `OR` predicates (on same attribute) into
    a single `IN SET` (Sandeep Chada)
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2498">CALCITE-2498</a>]
    Geode adapter wrongly quotes `BOOLEAN` values as strings (Andrei Sereda)

#### Bug-fixes, API changes and minor enhancements

* [<a href="https://issues.apache.org/jira/browse/CALCITE-2670">CALCITE-2670</a>]
  Combine similar JSON aggregate functions in operator table
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2468">CALCITE-2468</a>]
  Validator throws `IndexOutOfBoundsException` when trying to infer operand type
  from `STRUCT` return type (Rong Rong)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2596">CALCITE-2596</a>]
  When translating correlated variables in enumerable convention, convert
  not-null boxed primitive values to primitive (Stamatis Zampetakis)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2684">CALCITE-2684</a>]
  `RexBuilder` gives `AssertionError` when creating integer literal larger than
  2<sup>63</sup> (Ruben Quesada Lopez)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2719">CALCITE-2719</a>]
  In JDBC adapter for MySQL, fix cast to `INTEGER` and `BIGINT` (Piotr Bojko)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2713">CALCITE-2713</a>]
  JDBC adapter may generate casts on PostgreSQL for `VARCHAR` type exceeding max
  length
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2529">CALCITE-2529</a>]
  All numbers are in the same type family (Andrew Pilloud)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2701">CALCITE-2701</a>]
  Make generated `Baz` classes immutable (Stamatis Zampetakis)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2619">CALCITE-2619</a>]
  Reduce string literal creation cost by deferring and caching charset
  conversion (Ted Xu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2720">CALCITE-2720</a>]
  `RelMetadataQuery.getTableOrigin` throws `IndexOutOfBoundsException` if
  `RelNode` has no columns (Zoltan Haindrich)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2717">CALCITE-2717</a>]
  Use `Interner` instead of `LoadingCache` to cache traits, and so allow traits
  to be garbage-collected (Haisheng Yuan)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2542">CALCITE-2542</a>]
  In SQL parser, allow `.field` to follow any expression, not just tables and
  columns (Rong Rong)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2637">CALCITE-2637</a>]
  In SQL parser, allow prefix '-' between `BETWEEN` and `AND` (Qi Yu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2632">CALCITE-2632</a>]
  Ensure that `RexNode` and its sub-classes implement `hashCode` and `equals`
  methods (Zoltan Haindrich)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2494">CALCITE-2494</a>]
  `RexFieldAccess` should implement `equals` and `hashCode` methods
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2715">CALCITE-2715</a>]
  In JDBC adapter, do not generate character set in data types for MS SQL Server
  (Piotr Bojko)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2714">CALCITE-2714</a>]
  Make `BasicSqlType` immutable, and now `SqlTypeFactory.createWithNullability`
  can reuse existing type if possible (Ruben Quesada Lopez)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2687">CALCITE-2687</a>]
  `IS DISTINCT FROM` could lead to exceptions in `ReduceExpressionsRule`
  (Zoltan Haindrich)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2673">CALCITE-2673</a>]
  `SqlDialect` supports pushing of all functions by default
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2675">CALCITE-2675</a>]
  Type validation error as `ReduceExpressionsRule` fails to preserve type
  nullability (Zoltan Haindrich)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2669">CALCITE-2669</a>]
  `RelMdTableReferences` should check whether references inferred from input are
  null for `Union`/`Join` operators
* Following
  [<a href="https://issues.apache.org/jira/browse/CALCITE-2031">CALCITE-2031</a>]
  remove incorrect "Not implemented" message
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2668">CALCITE-2668</a>]
  Support for left/right outer join in `RelMdExpressionLineage`
* Fix invocation of deprecated constructor of `SqlAggFunction` (Hongze Zhang)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2652">CALCITE-2652</a>]
  `SqlNode` to SQL conversion fails if the join condition references a `BOOLEAN`
  column (Zoltan Haindrich)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2657">CALCITE-2657</a>]
  In `RexShuttle`, use `RexCall.clone` instead of `new RexCall` (Chunwei Lei)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2605">CALCITE-2605</a>]
  Support semi-join via `EnumerableCorrelate` (Ruben Quesada Lopez)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2605">CALCITE-2605</a>]
  Support left outer join via `EnumerableCorrelate`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1174">CALCITE-1174</a>]
  When generating SQL, translate `SUM0(x)` to `COALESCE(SUM(x), 0)`
* `RelBuilder.toString()`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2617">CALCITE-2617</a>]
  Add a variant of `FilterProjectTransposeRule` that can push down a `Filter`
  that contains correlated variables (Stamatis Zampetakis)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2638">CALCITE-2638</a>]
  Constant reducer should not treat as constant an `RexInputRef` that points to a
  call to a dynamic or non-deterministic function (Danny Chan)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2628">CALCITE-2628</a>]
  JDBC adapter throws `NullPointerException` while generating `GROUP BY` query
  for MySQL
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2404">CALCITE-2404</a>]
  Implement access to structured-types in enumerable runtime
  (Stamatis Zampetakis)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2622">CALCITE-2622</a>]
  `RexFieldCollation.toString()` method is not deterministic
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2611">CALCITE-2611</a>]
  Linq4j code generation failure if one side of an `OR` contains `UNKNOWN`
  (Zoltan Haindrich)
* Canonize simple cases for composite traits in trait factory
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2591">CALCITE-2591</a>]
  `EnumerableDefaults#mergeJoin` should throw error and not return incorrect
  results when inputs are not ordered (Enrico Olivelli)
* Test case for
  [<a href="https://issues.apache.org/jira/browse/CALCITE-2592">CALCITE-2592</a>]
  `EnumerableMergeJoin` is never taken
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2526">CALCITE-2526</a>]
  Add test for `OR` with nullable comparisons (pengzhiwei)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2413">CALCITE-2413</a>]
  Use raw signatures for classes with generics when producing Java code
* In Elasticsearch adapter, remove redundant null check in
  `CompoundQueryExpression`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2562">CALCITE-2562</a>]
  Remove dead code in `StandardConvertletTable#convertDatetimeMinus`
* Avoid `NullPointerException` when `FlatList` contains null elements
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2561">CALCITE-2561</a>]
  Remove dead code in `Lattice` constructor
* Apply small refactorings to Calcite codebase (Java 5, Java 7, Java 8)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2572">CALCITE-2572</a>]
  SQL standard semantics for `SUBSTRING` function (Andrew Pilloud)
* Remove dead code: `Compatible`, `CompatibleGuava11`
* Remove "Now, do something with table" from standard output when implementing
  sequences
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2444">CALCITE-2444</a>]
  Handle `IN` expressions when converting `SqlNode` to SQL (Zoltan Haindrich)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2537">CALCITE-2537</a>]
  Use litmus for `VolcanoPlanner#validate`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2546">CALCITE-2546</a>]
  Reduce precision of `Profiler`'s `surprise` and `cardinality` attributes to
  avoid floating point discrepancies (Alisha Prabhu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2563">CALCITE-2563</a>]
  Materialized view rewriting may swap columns in equivalent classes incorrectly
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2551">CALCITE-2551</a>]
  `SqlToRelConverter` gives `ClassCastException` while handling `IN` inside
  `WHERE NOT CASE` (pengzhiwei)
* Remove redundant `new` expression in constant array creation
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2474">CALCITE-2474</a>]
  SqlAdvisor: avoid NPE in lookupFromHints where FROM is empty
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2418">CALCITE-2418</a>]
  Remove `matchRecognize` field of `SqlSelect`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2514">CALCITE-2514</a>]
  Add `SqlIdentifier` conversion to `ITEM` operator for dynamic tables in
  `ExtendedExpander` (Arina Ielchiieva)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2491">CALCITE-2491</a>]
  Refactor `NameSet`, `NameMap`, and `NameMultimap`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2520">CALCITE-2520</a>]
  Make `SparkHandlerImpl#compile` silent by default, print code in
  `calcite.debug=true` mode only
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1026">CALCITE-1026</a>]
  Remove unused import
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2483">CALCITE-2483</a>]
  Druid adapter, when querying Druid segment metadata, throws when row number is
  larger than `Integer.MAX_VALUE` (Hongze Zhang)
* Support `AND`, `OR`, `COALESCE`, `IS [NOT] DISTINCT` in `RexUtil#op`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2495">CALCITE-2495</a>]
  Support encoded URLs in `org.apache.calcite.util.Source`, and use it for URL
  &rarr; File conversion in tests
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2271">CALCITE-2271</a>]
  Join of two views with window aggregates produces incorrect results or throws
  `NullPointerException`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2481">CALCITE-2481</a>]
  `NameSet` assumes lower-case characters have greater codes, which does not hold
  for certain characters
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2480">CALCITE-2480</a>]
  `NameSet.contains` wrongly returns `false` when element in set is upper-case
  and `seek` is lower-case
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2465">CALCITE-2465</a>]
  Enable use of materialized views for any planner
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2446">CALCITE-2446</a>]
  Lateral joins do not work when saved as custom views (Piotr Bojko)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2447">CALCITE-2447</a>]
  `POWER`, `ATAN2` functions fail with `NoSuchMethodException`
* Typo in `HepPlanner` trace message (Dylan)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2416">CALCITE-2416</a>]
  `AssertionError` when determining monotonicity (Alina Ipatina)
* Java 8: use `Map.computeIfAbsent` when possible
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2431">CALCITE-2431</a>]
  `SqlUtil.getAncestry` throws `AssertionError` when providing completion hints
  for sub-schema
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2430">CALCITE-2430</a>]
  `RelDataTypeImpl.getFieldList` throws `AssertionError` when SQL Advisor inspects
  non-struct field
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2429">CALCITE-2429</a>]
  `SqlValidatorImpl.lookupFieldNamespace` throws `NullPointerException` when SQL
  Advisor observes non-existing field
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2422">CALCITE-2422</a>]
  Query with unnest of column from nested sub-query fails when dynamic table is
  used
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2417">CALCITE-2417</a>]
  `RelToSqlConverter` throws `ClassCastException` with structs (Benoit Hanotte)
* Upgrades:
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2716">CALCITE-2716</a>]
    Upgrade to Avatica 1.13.0
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2711">CALCITE-2711</a>]
    Upgrade SQLLine to 1.6.0
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2570">CALCITE-2570</a>]
    Upgrade `forbiddenapis` to 2.6 for JDK 11 support
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2486">CALCITE-2486</a>]
    Upgrade Apache parent POM to version 21
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2467">CALCITE-2467</a>]
    Upgrade `owasp-dependency-check` maven plugin to 3.3.1
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2559">CALCITE-2559</a>]
    Update Checkstyle to 7.8.2
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2497">CALCITE-2497</a>]
    Update Janino version to 3.0.9
* Expression simplification (`RexSimplify`):
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2731">CALCITE-2731</a>]
    `RexProgramBuilder` makes unsafe simplifications to `CASE` expressions (Zoltan
    Haindrich)
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2730">CALCITE-2730</a>]
    `RelBuilder` incorrectly simplifies a `Filter` with duplicate conjunction to
    empty (Stamatis Zampetakis)
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2726">CALCITE-2726</a>]
    `ReduceExpressionRule` may oversimplify filter conditions containing `NULL`
    values
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2695">CALCITE-2695</a>]
    Simplify casts that are only widening nullability (Zoltan Haindrich)
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2631">CALCITE-2631</a>]
    General improvements in simplifying `CASE`
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2639">CALCITE-2639</a>]
    `FilterReduceExpressionsRule` causes `ArithmeticException` at execution time
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2620">CALCITE-2620</a>]
    Simplify `COALESCE(NULL, x)` &rarr; `x` (pengzhiwei)
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-1413">CALCITE-1413</a>]
    Enhance boolean case statement simplifications (Zoltan Haindrich)
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2615">CALCITE-2615</a>]
    When simplifying `NOT-AND-OR`, `RexSimplify` incorrectly applies predicates
    deduced for operands to the same operands (Zoltan Haindrich)
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2604">CALCITE-2604</a>]
    When simplifying an expression, say whether an `UNKNOWN` value will be
    interpreted as is, or as `TRUE` or `FALSE`
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2438">CALCITE-2438</a>]
    Fix wrong results for `IS NOT FALSE(FALSE)` (zhiwei.pzw) (Zoltan Haindrich)
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2506">CALCITE-2506</a>]
    Simplifying `COALESCE(+ nullInt, +vInt())` results in
    `AssertionError: result mismatch` (pengzhiwei)
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2580">CALCITE-2580</a>]
    Simplifying `COALESCE(NULL > NULL, TRUE)` produces wrong result filter
    expressions (pengzhiwei)
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2586">CALCITE-2586</a>]
    `CASE` with repeated branches gives `AssertionError`
    (pengzhiwei)
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2590">CALCITE-2590</a>]
    Remove redundant `CAST` when operand has exactly the same type as it is casted to
  * Implement fuzzy generator for `CASE` expressions
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2556">CALCITE-2556</a>]
    Simplify `NOT TRUE` &rarr; `FALSE`, and `NOT FALSE` &rarr; `TRUE` (pengzhiwei)
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2581">CALCITE-2581</a>]
    Avoid errors in simplifying `UNKNOWN AND NOT (UNKNOWN OR ...)` (pengzhiwei)
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2527">CALCITE-2527</a>]
    Simplify `(c IS NULL) OR (c IS ...)` might result in AssertionError: result
    mismatch (pengzhiwei)
  * Display random failure of Rex fuzzer in build logs to inspire further fixes
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2567">CALCITE-2567</a>]
    Simplify `IS NULL(NULL)` to `TRUE` (pengzhiwei)
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2555">CALCITE-2555</a>]
    RexSimplify: Simplify `x >= NULL` to `UNKNOWN` (pengzhiwei)
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2504">CALCITE-2504</a>]
    Add randomized test for better code coverage of rex node create and
    simplification
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2469">CALCITE-2469</a>]
    Simplify `(NOT x) IS NULL` &rarr; `x IS NULL` (pengzhiwei);
    also, simplify `f(x, y) IS NULL` &rarr; `x IS NULL OR y IS NULL` if `f` is a
    strong operator
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2327">CALCITE-2327</a>]
    Simplify `AND(x, y, NOT(y))` &rarr; `AND(x, null, IS NULL(y))`
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2327">CALCITE-2327</a>]
    Avoid simplification of `x AND NOT(x)` to `FALSE` for nullable `x`
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-2505">CALCITE-2505</a>]
    `AssertionError` when simplifying `IS [NOT] DISTINCT` expressions
    (Haisheng Yuan)

#### Build and test suite

* [<a href="https://issues.apache.org/jira/browse/CALCITE-2678">CALCITE-2678</a>]
  `RelBuilderTest#testRelBuilderToString` fails on Windows (Stamatis Zampetakis)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2660">CALCITE-2660</a>]
  `OsAdapterTest` now checks whether required commands are available
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2655">CALCITE-2655</a>]
  Enable Travis to test against JDK 12
* Ensure that tests are not calling `checkSimplify3` with `expected`,
  `expectedFalse`, `expectedTrue` all the same
* Geode adapter tests: Removed unnecessary `try/final` block in `RefCountPolicy`
* Add license to `TestKtTest` and add `apache-rat:check` to Travis CI
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2112">CALCITE-2112</a>]
  Add Apache license header to `maven-wrapper.properties`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2588">CALCITE-2588</a>]
  Run Geode adapter tests with an embedded instance
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2594">CALCITE-2594</a>]
  Ensure `forbiddenapis` and `maven-compiler` use the correct JDK version
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2642">CALCITE-2642</a>]
  Checkstyle complains that `maven-wrapper.properties` is missing a header
* `commons:commons-pool2` is used in tests only, so use `scope=test` for it
* Make `findbugs:jsr305` dependency optional
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2458">CALCITE-2458</a>]
  Add Kotlin as a test dependency
* Make build scripts Maven 3.3 compatible
* Fix JavaDoc warnings for Java 9+, and check JavaDoc in Travis CI
* Unwrap invocation target exception from QuidemTest#test
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2518">CALCITE-2518</a>]
  Add `failOnWarnings` to `maven-javadoc-plugin` configuration
* Silence Pig, Spark, and Elasticsearch logs in tests
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1894">CALCITE-1894</a>]
  `CsvTest.testCsvStream` failing often: add `@Ignore` since the test is known to
  fail
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2535">CALCITE-2535</a>]
  Enable `SqlTester.checkFails` (previously it was a no-op) (Hongze Zhang)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2558">CALCITE-2558</a>]
  Improve re-compilation times by skipping `parser.java` update on each build
* Increase timeout for Cassandra daemon startup for `CassandraAdapterTest`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2412">CALCITE-2412</a>]
  Add Windows CI via AppVeyor (Sergey Nuyanzin)
* Reduce `HepPlannerTest#testRuleApplyCount` complexity
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2523">CALCITE-2523</a>]
  Guard `PartiallyOrderedSetTest#testPosetBitsLarge` with
  `CalciteAssert.ENABLE_SLOW`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2521">CALCITE-2521</a>]
  Guard `RelMetadataTest#testMetadataHandlerCacheLimit` with
  `CalciteAssert.ENABLE_SLOW`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2484">CALCITE-2484</a>]
  Add `SqlValidatorDynamicTest` to `CalciteSuite`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2484">CALCITE-2484</a>]
  Move dynamic tests to a separate class like `SqlValidatorDynamicTest`, and
  avoid reuse of `MockCatalogReaderDynamic`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2522">CALCITE-2522</a>]
  Remove `e.printStackTrace()` from `CalciteAssert#returns`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2512">CALCITE-2512</a>]
  Move `StreamTest#ROW_GENERATOR` to `Table.scan().iterator` to make it not
  shared between threads (Sergey Nuyanzin)
* Skip second Checkstyle execution during Travis CI build
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2519">CALCITE-2519</a>]
  Silence ERROR logs from `CalciteException`, `SqlValidatorException` during
  tests
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1026">CALCITE-1026</a>]
  Fix `ModelTest#testYamlFileDetection` when source folder has spaces
* `MockCatalogReader` is used in testing, so cache should be disabled there to
  avoid thread conflicts and/or stale results
* [<a href="https://issues.apache.org/jira/browse/CALCITE-311">CALCITE-311</a>]
  Add a test-case for Filter after Window aggregate
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2462">CALCITE-2462</a>]
  `RexProgramTest`: replace `nullLiteral` &rarr; `nullInt`,
  `unknownLiteral` &rarr; `nullBool` for brevity
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2462">CALCITE-2462</a>]
  `RexProgramTest`: move "rex building" methods to base class
* `SqlTestFactory`: use lazy initialization of objects
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2435">CALCITE-2435</a>]
  Refactor `SqlTestFactory`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2428">CALCITE-2428</a>]
  Cassandra unit test fails to parse JDK version string (Andrei Sereda)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2419">CALCITE-2419</a>]
  Use embedded Cassandra for tests

#### Web site and documentation

* Add geospatial category to DOAP file
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2577">CALCITE-2577</a>]
  Update links on download page to HTTPS
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2574">CALCITE-2574</a>]
  Update download page to include instructions for verifying a downloaded
  artifact
* Update build status badges in `README.md`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2705">CALCITE-2705</a>]
  Site: Remove duplicate "selectivity" in list of metadata types (Alan Jin)
* Site: Add Andrei Sereda as committer
* Site: Update Julian Hyde's affiliation
* Update Michael Mior's affiliation
* Site: Add instructions for updating PRs based on the discussion in the dev
  list (Stamatis Zampetakis)
* Site: Add committer Sergey Nuyanzin
* Site: News item for release 1.17.0

## <a href="https://github.com/apache/calcite/releases/tag/calcite-1.17.0">1.17.0</a> / 2018-07-16
{: #v1-17-0}

Compatibility: This release is tested
on Linux, macOS, Microsoft Windows;
using Oracle JDK 8, 9, 10;
Guava versions 19.0 to 23.0;
Druid version 0.11.0;
other software versions as specified in `pom.xml`.

This release comes four months after 1.16.0. It includes more than 90 resolved
issues, comprising a large number of new features as well as general improvements
and bug-fixes. Among others:

Implemented <a href='https://issues.apache.org/jira/browse/CALCITE-2280'>Babel SQL parser</a>
that accepts all SQL dialects.
Allowed <a href='https://issues.apache.org/jira/browse/CALCITE-2261'>JDK 8 language level</a> for core module.
Calcite has been upgraded to use <a href='https://issues.apache.org/jira/browse/CALCITE-2365'>Avatica 1.12.0</a>

#### New features

* [<a href='https://issues.apache.org/jira/browse/CALCITE-873'>CALCITE-873</a>]
  Add a planner rule, `SortRemoveConstantKeysRule`, that removes constant keys from Sort (Atri Sharma)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2045'>CALCITE-2045</a>]
  `CREATE TYPE` (Shuyi Chen)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2216'>CALCITE-2216</a>]
  Improve extensibility of `AggregateReduceFunctionsRule` (Fabian Hueske)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2227'>CALCITE-2227</a>]
  Standards-compliant column ordering for `NATURAL JOIN` and `JOIN USING`
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2280'>CALCITE-2280</a>]
  Babel SQL parser
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2286'>CALCITE-2286</a>]
  Support timestamp type for Druid adapter
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2304'>CALCITE-2304</a>]
  In Babel parser, allow Hive-style syntax `LEFT SEMI JOIN`
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2321'>CALCITE-2321</a>]
  A union of `CHAR` columns of different lengths can now (based on a conformance setting) yield a `VARCHAR` column (Hequn Cheng)

#### Bug-fixes, API changes and minor enhancements

* [<a href='https://issues.apache.org/jira/browse/CALCITE-531'>CALCITE-531</a>]
  `LATERAL` combined with window function or table function
* [<a href='https://issues.apache.org/jira/browse/CALCITE-1167'>CALCITE-1167</a>]
  `OVERLAPS` should match even if operands are in (high, low) order
* [<a href='https://issues.apache.org/jira/browse/CALCITE-1436'>CALCITE-1436</a>]
  Support `MIN`/`MAX` functions (Muhammad Gelbana)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-1866'>CALCITE-1866</a>]
  JDBC adapter generates incorrect code when pushing `FLOOR` to MySQL (Kang Wang, Sergey Nuyanzin)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-1916'>CALCITE-1916</a>]
  Use Teradata's TPC-DS generator and run tests against TPC-DS at small scale
* [<a href='https://issues.apache.org/jira/browse/CALCITE-1949'>CALCITE-1949</a>]
  `CalciteStatement` should call `AvaticaStatement` close_(), to avoid memory leak (Kevin Risden)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2053'>CALCITE-2053</a>]
  Resolve Java user-defined functions that have `Double` and `BigDecimal` arguments (余启)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2063'>CALCITE-2063</a>]
  Add JDK 10 to `.travis.yml`
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2159'>CALCITE-2159</a>]
  Support dynamic row type in `UNNEST` (Chunhui Shi)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2164'>CALCITE-2164</a>]
  Fix alerts raised by lgtm.com (Malcolm Taylor)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2188'>CALCITE-2188</a>]
  JDBC adapter generates invalid SQL for `DATE`/`INTERVAL` arithmetic (Rahul Raj)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2201'>CALCITE-2201</a>]
  Pass `RelBuilder` into `RelDecorrelator` and `RelStructuredTypeFlattener` (Volodymyr Vysotskyi)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2205'>CALCITE-2205</a>]
  `JoinPushTransitivePredicatesRule` should not create `Filter` on top of equivalent `Filter` (Vitalii Diravka)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2206'>CALCITE-2206</a>]
  JDBC adapter incorrectly pushes windowed aggregates down to HSQLDB (Pavel Gubin)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2220'>CALCITE-2220</a>]
  `SqlToRelConverter` generates incorrect ordinal while flattening a record-valued field (Shuyi Chen)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2222'>CALCITE-2222</a>]
  Add Quarter timeunit as a valid unit to pushdown to Druid
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2225'>CALCITE-2225</a>]
  Upgrade Apache parent POM to version 19, and support OpenJDK 10
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2226'>CALCITE-2226</a>]
  Druid adapter: Substring operator converter does not handle non-constant literals correctly
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2229'>CALCITE-2229</a>]
  Allow sqlsh to be run from path, not just current directory
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2232'>CALCITE-2232</a>]
  Assertion error on `AggregatePullUpConstantsRule` while adjusting `Aggregate` indices
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2236'>CALCITE-2236</a>]
  Druid adapter: Avoid duplication of fields names during Druid query planing
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2237'>CALCITE-2237</a>]
  Upgrade Maven Surefire plugin to 2.21.0 (Kevin Risden)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2238'>CALCITE-2238</a>]
  Fix Pig and Spark adapter failures with JDK 10
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2240'>CALCITE-2240</a>]
  Extend rule to push predicates into `CASE` statement (Zoltan Haindrich)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2242'>CALCITE-2242</a>]
  Using custom `RelBuilder` for `FilterRemoveIsNotDistinctFromRule` (Vitalii Diravka)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2247'>CALCITE-2247</a>]
  Simplify `AND` and `OR` conditions using predicates (Zoltan Haindrich)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2253'>CALCITE-2253</a>]
  Fix matching predicate for `JdbcProjectRule` rule
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2255'>CALCITE-2255</a>]
  Add JDK 11 to Travis CI
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2259'>CALCITE-2259</a>]
  Allow Java 8 syntax
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2261'>CALCITE-2261</a>]
  Switch core module to JDK 8 (Enrico Olivelli)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2262'>CALCITE-2262</a>]
  Druid adapter: Allow count(*) to be pushed when other aggregate functions are present
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2264'>CALCITE-2264</a>]
  In JDBC adapter, do not push down a call to a user-defined function (UDF) (Piotr Bojko)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2265'>CALCITE-2265</a>]
  Allow comparison of ROW values (Dylan Adams)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2267'>CALCITE-2267</a>]
  Thread-safe generation of `AbstractRelNode.id` (Zhong Yu)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2275'>CALCITE-2275</a>]
  Do not push down `NOT` condition in `JOIN` (Vitalii Diravka)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2276'>CALCITE-2276</a>]
  Allow explicit `ROW` value constructor in `SELECT` clause and elsewhere (Danny Chan)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2277'>CALCITE-2277</a>]
  Skip `SemiJoin` operator in materialized view-based rewriting algorithm
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2278'>CALCITE-2278</a>]
  `AggregateJoinTransposeRule` fails to split aggregate call if input contains an aggregate call and has distinct rows (Haisheng Yuan)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2281'>CALCITE-2281</a>]
  Return type of the `TIMESTAMPADD` function has wrong precision (Sudheesh Katkam)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2287'>CALCITE-2287</a>]
  `FlatList.equals()` throws `StackOverflowError` (Zhen Wang, Zhong Yu)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2291'>CALCITE-2291</a>]
  Support Push Project past Correlate (Chunhui Shi)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2293'>CALCITE-2293</a>]
  Upgrade forbidden-apis to 2.5 (for JDK 10)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2299'>CALCITE-2299</a>]
  `TIMESTAMPADD`(`SQL_TSI_FRAC_SECOND`) should be nanoseconds (Sergey Nuyanzin)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2303'>CALCITE-2303</a>]
  In `EXTRACT` function, support `MICROSECONDS`, `MILLISECONDS`, `EPOCH`, `ISODOW`, `ISOYEAR` and `DECADE` time units (Sergey Nuyanzin)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2305'>CALCITE-2305</a>]
  JDBC adapter generates invalid casts on PostgreSQL, because PostgreSQL does not have `TINYINT` and `DOUBLE` types
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2306'>CALCITE-2306</a>]
  AssertionError in `RexLiteral.getValue3` with null literal of type `DECIMAL` (Godfrey He)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2309'>CALCITE-2309</a>]
  Dialects: Hive dialect does not support charsets in constants
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2314'>CALCITE-2314</a>]
  Verify RexNode transformations by evaluating before and after expressions against sample values
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2316'>CALCITE-2316</a>]
  Elasticsearch adapter should not convert queries to lower-case (Andrei Sereda)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2318'>CALCITE-2318</a>]
  `NumberFormatException` while starting SQLLine
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2319'>CALCITE-2319</a>]
  Set correct dimension type for druid expressions with result type boolean (nsihantmonu51)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2320'>CALCITE-2320</a>]
  Filtering UDF when converting `Filter` to `JDBCFilter` (Piotr Bojko)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2323'>CALCITE-2323</a>]
  Apply "`defaultNullCollation`" configuration parameter when translating `ORDER BY` inside `OVER` (John Fang)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2324'>CALCITE-2324</a>]
  `EXTRACT` function: `HOUR`, `MINUTE` and `SECOND` parts of a `DATE` must be zero (Sergey Nuyanzin)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2329'>CALCITE-2329</a>]
  Improve rewrite for "constant IN (sub-query)"
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2331'>CALCITE-2331</a>]
  Evaluation of predicate `(A or B) and C` fails for Elasticsearch adapter (Andrei Sereda)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2332'>CALCITE-2332</a>]
  Wrong simplification of `FLOOR(CEIL(x))` to `FLOOR(x)`
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2333'>CALCITE-2333</a>]
  Stop releasing zips
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2334'>CALCITE-2334</a>]
  Extend simplification of expressions with `CEIL` function over date types
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2341'>CALCITE-2341</a>]
  Fix `ImmutableBitSetTest` for jdk11
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2342'>CALCITE-2342</a>]
  Fix improper use of assert
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2345'>CALCITE-2345</a>]
  Running Unit tests with Fongo and integration tests with real mongo instance (Andrei Sereda)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2347'>CALCITE-2347</a>]
  Running ElasticSearch in embedded mode for unit tests of ES adapter (Andrei Sereda)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2353'>CALCITE-2353</a>]
  Allow user to override `SqlSetOption` (Andrew Pilloud)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2355'>CALCITE-2355</a>]
  Implement multiset operations (Sergey Nuyanzin)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2357'>CALCITE-2357</a>]
  Freemarker dependency override issue in fmpp maven plugin (yanghua)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2358'>CALCITE-2358</a>]
  Use null literal instead of empty string (b-slim)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2359'>CALCITE-2359</a>]
  Inconsistent results casting intervals to integers (James Duong)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2364'>CALCITE-2364</a>]
  Fix timezone issue (in test) between Mongo DB and local JVM (Andrei Sereda)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2365'>CALCITE-2365</a>]
  Upgrade avatica to 1.12
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2366'>CALCITE-2366</a>]
  Add support for `ANY_VALUE` aggregate function (Gautam Parai)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2368'>CALCITE-2368</a>]
  Fix `misc.iq` and `scalar.iq` quidem unit tests failures on Windows
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2369'>CALCITE-2369</a>]
  Fix `OsAdapterTest` failure on windows (Sergey Nuyanzin)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2370'>CALCITE-2370</a>]
  Fix failing mongo IT tests when explicit order was not specified (Andrei Sereda)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2376'>CALCITE-2376</a>]
  Unify ES2 and ES5 adapters. Migrate to low-level ES rest client as main transport (Andrei Sereda)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2379'>CALCITE-2379</a>]
  CVSS dependency-check-maven fails for calcite-spark and calcite-ubenchmark modules
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2380'>CALCITE-2380</a>]
  Javadoc generation failure in Elasticsearch2 adapter (Andrei Sereda)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2381'>CALCITE-2381</a>]
  Add information for authenticating against maven repo, GPG keys and version numbers to HOWTO
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2382'>CALCITE-2382</a>]
  Sub-query join lateral table function (pengzhiwei)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2383'>CALCITE-2383</a>]
  `NTH_VALUE` window function (Sergey Nuyanzin)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2384'>CALCITE-2384</a>]
  Performance issue in `getPulledUpPredicates` (Zoltan Haindrich)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2387'>CALCITE-2387</a>]
  Fix for `date`/`timestamp` cast expressions in Druid adapter
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2388'>CALCITE-2388</a>]
  Upgrade from `commons-dbcp` to `commons-dbcp2` version 2.4.0
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2391'>CALCITE-2391</a>]
  Aggregate query with `UNNEST` or `LATERAL` fails with `ClassCastException`
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2392'>CALCITE-2392</a>]
  Prevent columns permutation for `NATURAL JOIN` and `JOIN USING` when dynamic table is used
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2396'>CALCITE-2396</a>]
  Allow `NULL` intervals in `TIMESTAMPADD` and `DATETIME_PLUS` functions (James Duong)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2398'>CALCITE-2398</a>]
  `SqlSelect` must call into `SqlDialect` for unparse (James Duong)
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2403'>CALCITE-2403</a>]
  Upgrade quidem to 0.9
* [<a href='https://issues.apache.org/jira/browse/CALCITE-2409'>CALCITE-2409</a>]
  `SparkAdapterTest` fails on Windows when '/tmp' directory does not exist
  (Sergey Nuyanzin)

## <a href="https://github.com/apache/calcite/releases/tag/calcite-1.16.0">1.16.0</a> / 2018-03-14
{: #v1-16-0}

Compatibility: This release is tested
on Linux, macOS, Microsoft Windows;
using Oracle JDK 8, 9, 10;
Guava versions 19.0 to 23.0;
Druid version 0.11.0;
other software versions as specified in `pom.xml`.

This release comes three months after 1.15.0. It includes more than 80 resolved
issues, comprising a large number of new features as well as general improvements
and bug-fixes to Calcite core. Among others:

* Calcite has been upgraded to use
<a href="https://issues.apache.org/jira/browse/CALCITE-2182">Avatica 1.11.0</a>,
which was recently released.
* Moreover, a new adapter to
<a href="https://issues.apache.org/jira/browse/CALCITE-2059">read data from Apache Geode</a>
was added in this release. In addition, more progress has been made for the existing adapters,
e.g., the Druid adapter can generate
<a href="https://issues.apache.org/jira/browse/CALCITE-2077">`SCAN` queries rather than `SELECT` queries</a>
for more efficient execution and it can push
<a href="https://issues.apache.org/jira/browse/CALCITE-2170">more work to Druid using its new expressions capabilities</a>,
and the JDBC adapter now <a href="https://issues.apache.org/jira/browse/CALCITE-2128">supports the SQL dialect used by Jethro Data</a>.
* Finally, this release
<a href="https://issues.apache.org/jira/browse/CALCITE-2027">drops support for JDK 1.7</a> and
support for <a href="https://issues.apache.org/jira/browse/CALCITE-2191">Guava versions earlier than 19</a>.

#### New features

* [<a href="https://issues.apache.org/jira/browse/CALCITE-1265">CALCITE-1265</a>]
  In JDBC adapter, push `OFFSET` and `FETCH` to data source
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2059">CALCITE-2059</a>]
  Apache Geode adapter (Christian Tzolov)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2077">CALCITE-2077</a>]
  Druid adapter: Use `SCAN` query rather than `SELECT` query (Nishant Bangarwa)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2128">CALCITE-2128</a>]
  In JDBC adapter, add SQL dialect for Jethro Data (Jonathan Doron)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2170">CALCITE-2170</a>]
  Use Druid Expressions capabilities to improve the amount of work that can be pushed to Druid

#### Bug-fixes, API changes and minor enhancements

* [<a href="https://issues.apache.org/jira/browse/CALCITE-1054">CALCITE-1054</a>]
  NPE caused by wrong code generation for Timestamp fields
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1188">CALCITE-1188</a>]
  NullPointerException in `EXTRACT` with `WHERE ... IN` clause if field has null value
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1427">CALCITE-1427</a>]
  Code generation incorrect (does not compile) for DATE, TIME and TIMESTAMP fields
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1658">CALCITE-1658</a>]
  DateRangeRules incorrectly rewrites `EXTRACT` calls (Nishant Bangarwa)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1697">CALCITE-1697</a>]
  Update Mongo driver version to 3.5.0 (Vladimir Dolzhenko)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2002">CALCITE-2002</a>]
  `DISTINCT` applied to `VALUES` returns wrong result
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2009">CALCITE-2009</a>]
  Possible bug in interpreting `( IN ) OR ( IN )` logic
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2020">CALCITE-2020</a>]
  Upgrade org.incava java-diff
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2027">CALCITE-2027</a>]
  Drop support for Java 7 (JDK 1.7)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2034">CALCITE-2034</a>]
  `FileReaderTest` fails with path containing spaces
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2066">CALCITE-2066</a>]
  `RelOptUtil.splitJoinCondition()` could not split condition with case after applying `FilterReduceExpressionsRule` (Volodymyr Vysotskyi)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2071">CALCITE-2071</a>]
  Query with `IN` and `OR` in `WHERE` clause returns wrong result (Vineet Garg)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2072">CALCITE-2072</a>]
  Enable spatial functions by adding 'fun=spatial' to JDBC connect string
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2075">CALCITE-2075</a>]
  SparkAdapterTest UT fails
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2076">CALCITE-2076</a>]
  Upgrade to Druid 0.11.0 (Nishant Bangarwa)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2080">CALCITE-2080</a>]
  Query with `NOT IN` operator and literal throws `AssertionError`: 'Cast for just nullability not allowed' (Volodymyr Vysotskyi)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2081">CALCITE-2081</a>]
  Query with windowed aggregates under both sides of a `JOIN` throws `NullPointerException` (Zhen Wang)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2084">CALCITE-2084</a>]
  `SqlValidatorImpl.findTable()` method incorrectly handles table schema with few schema levels (Volodymyr Vysotskyi)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2088">CALCITE-2088</a>]
  Add more complex end-to-end tests in "plus" module, using Chinook data set (Piotr Bojko)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2089">CALCITE-2089</a>]
  Druid adapter: Push filter on `floor(time)` to Druid (Nishant Bangarwa)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2090">CALCITE-2090</a>]
  Extend Druid Range Rules to extract interval from Floor (Nishant Bangarwa)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2091">CALCITE-2091</a>]
  Improve DruidQuery cost function, to ensure that `EXTRACT` gets pushed as an interval if possible
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2092">CALCITE-2092</a>]
  Allow passing custom `RelBuilder` into `SqlToRelConverter`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2093">CALCITE-2093</a>]
  `OsAdapterTest` in Calcite Plus does not respect locale (Piotr Bojko)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2094">CALCITE-2094</a>]
  Druid adapter: `Count(*)` returns null instead of 0 when condition filters all rows
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2095">CALCITE-2095</a>]
  Druid adapter: Push always true and always true expressions as Expression Filters
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2096">CALCITE-2096</a>]
  Druid adapter: Remove extra `dummy_aggregator`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2097">CALCITE-2097</a>]
  Druid adapter: Push Aggregate and Filter operators containing metric columns to Druid
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2098">CALCITE-2098</a>]
  Push filters to Druid Query Scan when we have `OR` of `AND` clauses
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2099">CALCITE-2099</a>]
  Code generated for `GROUP BY` inside `UNION` does not compile (Zhen Wang)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2101">CALCITE-2101</a>]
  Druid adapter: Push count(column) using Druid filtered aggregate
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2102">CALCITE-2102</a>]
  Ignore duplicate `ORDER BY` keys, and ensure RelCollation contains no duplicates (John Fang)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2104">CALCITE-2104</a>]
  Add separate rules for `AggregateUnionAggregateRule` to reduce potential matching cost in `VolcanoPlanner` (lincoln-lil)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2105">CALCITE-2105</a>]
  `AggregateJoinTransposeRule` fails when process aggregate without group keys (jingzhang)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2107">CALCITE-2107</a>]
  Timezone not passed as part of granularity when passing `TimeExtractionFunction` to Druid (Nishant Bangarwa)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2108">CALCITE-2108</a>]
  `AggregateJoinTransposeRule` fails when process aggregateCall above `SqlSumEmptyIsZeroAggFunction` without groupKeys (jingzhang)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2110">CALCITE-2110</a>]
  `ArrayIndexOutOfBoundsException` in RexSimplify when using `ReduceExpressionsRule.JOIN_INSTANCE`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2111">CALCITE-2111</a>]
  Make HepPlanner more efficient by applying rules depth-first
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2113">CALCITE-2113</a>]
  Push column pruning to druid when Aggregate cannot be pushed (Nishant Bangarwa)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2114">CALCITE-2114</a>]
  Re-enable `DruidAggregateFilterTransposeRule`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2116">CALCITE-2116</a>]
  The digests are not same for the common sub-expressions in HepPlanner (LeoWangLZ)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2118">CALCITE-2118</a>]
  RelToSqlConverter should only generate "*" if field names match (Sam Waggoner)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2122">CALCITE-2122</a>]
  In DateRangeRules, make either `TIMESTAMP` or `DATE` literal, according to target type (Nishant Bangarwa)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2124">CALCITE-2124</a>]
  `AggregateExpandDistinctAggregatesRule` should make `SUM` nullable if there is no `GROUP BY` (Godfrey He)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2127">CALCITE-2127</a>]
  In Interpreter, allow a node to have more than one consumer
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2133">CALCITE-2133</a>]
  Allow SqlGroupedWindowFunction to specify returnTypeInference in its constructor (Shuyi Chen)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2135">CALCITE-2135</a>]
  If there is an aggregate function inside an `OVER` clause, validator should treat query as an aggregate query (Volodymyr Tkach)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2137">CALCITE-2137</a>]
  Materialized view rewriting not being triggered for some join queries
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2139">CALCITE-2139</a>]
  Upgrade checkstyle
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2143">CALCITE-2143</a>]
  RelToSqlConverter produces incorrect SQL with aggregation (Sam Waggoner)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2147">CALCITE-2147</a>]
  GroupingSets involving rollup resulting into an incorrect plan (Ravindar Munjam)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2154">CALCITE-2154</a>]
  Upgrade jackson to 2.9.4
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2156">CALCITE-2156</a>]
  In DateRangeRules, compute `FLOOR` and `CEIL` of `TIMESTAMP WITH LOCAL TIMEZONE` in local time zone (Nishant Bangarwa)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2162">CALCITE-2162</a>]
  Exception when accessing sub-field of sub-field of composite Array element (Shuyi Chen)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2178">CALCITE-2178</a>]
  Extend expression simplifier to work on datetime `CEIL`/`FLOOR` functions
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2179">CALCITE-2179</a>]
  General improvements for materialized view rewriting rule
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2180">CALCITE-2180</a>]
  Invalid code generated for negative of byte and short values
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2183">CALCITE-2183</a>]
  Implement `RelSubset.copy` method (Alessandro Solimando)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2185">CALCITE-2185</a>]
  Additional unit tests for Spark Adapter (Alessandro Solimando)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2187">CALCITE-2187</a>]
  Fix build issue caused by `CALCITE-2170`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2189">CALCITE-2189</a>]
  RelMdAllPredicates fast bail out creates mismatch with RelMdTableReferences
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2190">CALCITE-2190</a>]
  Extend SubstitutionVisitor.splitFilter to cover different order of operands
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2191">CALCITE-2191</a>]
  Drop support for Guava versions earlier than 19
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2192">CALCITE-2192</a>]
  RelBuilder wrongly skips creating an Aggregate that prunes columns, if input is unique
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2195">CALCITE-2195</a>]
  `AggregateJoinTransposeRule` fails to aggregate over unique column (Zhong Yu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2196">CALCITE-2196</a>]
  Tweak janino code generation to allow debugging (jingzhang)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2197">CALCITE-2197</a>]
  Test failures on Windows
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2200">CALCITE-2200</a>]
  Infinite loop for JoinPushTransitivePredicatesRule
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2207">CALCITE-2207</a>]
  Enforce Java version via maven-enforcer-plugin (Kevin Risden)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2213">CALCITE-2213</a>]
  Geode integration tests are failing

#### Web site and documentation

* [<a href="https://issues.apache.org/jira/browse/CALCITE-2024">CALCITE-2024</a>]
  Submit a journal paper on Calcite to VLDB Journal or ACM SIGMOD Record (Edmon Begoli)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2203">CALCITE-2203</a>]
  Calcite site redirect links to Avatica broken with jekyll-redirect-from 0.12+ (Kevin Risden)

## <a href="https://github.com/apache/calcite/releases/tag/calcite-1.15.0">1.15.0</a> / 2017-12-11
{: #v1-15-0}

Compatibility: This release is tested
on Linux, macOS, Microsoft Windows;
using Oracle JDK 7, 8, 9, 10;
Guava versions 14.0 to 23.0;
Druid version 0.10.0;
other software versions as specified in `pom.xml`.

This release comes three months after 1.14.0. It includes than 44 resolved
issues, mostly modest improvements and bug-fixes, but here are some
features of note:

* [<a href="https://issues.apache.org/jira/browse/CALCITE-707">CALCITE-707</a>]
  adds *DDL commands* to Calcite for the first time, including *CREATE and DROP
  commands for schemas, tables, foreign tables, views, and materialized views*.
  We know that DDL syntax is a matter of taste, so we added the extensions to a
  *new "server" module*, leaving the "core" parser unchanged;
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2061">CALCITE-2061</a>]
  allows *dynamic parameters* in the `LIMIT` and `OFFSET` and clauses;
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1913">CALCITE-1913</a>]
  refactors the JDBC adapter to make it easier to *plug in a new SQL dialect*;
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1616">CALCITE-1616</a>]
  adds a *data profiler*, an algorithm that efficiently analyzes large data sets
  with many columns, estimating the number of distinct values in columns and
  groups of columns, and finding functional dependencies. The improved
  statistics are used by the algorithm that designs summary tables for a
  lattice.

Calcite now supports JDK 10 and Guava 23.0. (It continues to run on
JDK 7, 8 and 9, and on versions of Guava as early as 14.0.1. The default
version of Guava remains 19.0, the latest version compatible with JDK 7
and the Cassandra adapter's dependencies.)

This is the <a href="https://issues.apache.org/jira/browse/CALCITE-2027">last
release that will support JDK 1.7</a>.

#### New features

* [<a href="https://issues.apache.org/jira/browse/CALCITE-1616">CALCITE-1616</a>]
  Data profiler
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2061">CALCITE-2061</a>]
  Dynamic parameters in `OFFSET`, `FETCH` and `LIMIT` clauses (Enrico Olivelli)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-707">CALCITE-707</a>]
  Add "server" module, with built-in support for simple DDL statements
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2041">CALCITE-2041</a>]
  When `ReduceExpressionRule` simplifies a nullable expression, allow the result
  to change type to `NOT NULL`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2058">CALCITE-2058</a>]
  Support JDK 10
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2016">CALCITE-2016</a>]
  Make item + dot operators work for array (e.g. `SELECT orders[5].color FROM t`
  (Shuyi Chen)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2035">CALCITE-2035</a>]
  Allow approximate aggregate functions, and add `APPROX_COUNT_DISTINCT`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1990">CALCITE-1990</a>]
  Make `RelDistribution` extend `RelMultipleTrait` (LeoWangLZ)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1867">CALCITE-1867</a>]
  Allow user-defined grouped window functions (Timo Walther)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2031">CALCITE-2031</a>]
  `ST_X` and `ST_Y` GIS functions
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1913">CALCITE-1913</a>]
  Pluggable SQL dialects for JDBC adapter: Replace usages of `DatabaseProduct`
  with dialect methods, and introduce a configurable `SqlDialectFactory`
  (Christian Beikov)

#### Bug-fixes, API changes and minor enhancements

* [<a href="https://issues.apache.org/jira/browse/CALCITE-2078">CALCITE-2078</a>]
  Aggregate functions in `OVER` clause (Liao Xintao)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2070">CALCITE-2070</a>]
  Git test fails when run from source distro
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1808">CALCITE-1808</a>]
  `JaninoRelMetadataProvider` loading cache might cause `OutOfMemoryError`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2069">CALCITE-2069</a>]
  `RexSimplify.removeNullabilityCast()` always removes cast for operand with
  `ANY` type (Volodymyr Vysotskyi)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2074">CALCITE-2074</a>]
  Simplification of point ranges that are open above or below yields wrong
  results
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2005">CALCITE-2005</a>]
  Test failures on Windows
* Add `ImmutableBitSet.set(int, boolean)`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2054">CALCITE-2054</a>]
  Error while validating `UPDATE` with dynamic parameter in `SET` clause (Enrico
  Olivelli)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2055">CALCITE-2055</a>]
  Check year, month, day, hour, minute and second ranges for date and time
  literals (Volodymyr Vysotskyi)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2051">CALCITE-2051</a>]
  Rules using `Aggregate` might check for simple grouping sets incorrectly
* Add parameter to `SqlCallBinding.getOperandLiteralValue(int)` to specify
  desired value type
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2039">CALCITE-2039</a>]
  `AssertionError` when pushing project to `ProjectableFilterableTable`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2050">CALCITE-2050</a>]
  Exception when pushing post-aggregates into Druid
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2043">CALCITE-2043</a>]
  Use custom `RelBuilder` implementation in some rules (Volodymyr Vysotskyi)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2044">CALCITE-2044</a>]
  Tweak cost of `BindableTableScan` to make sure `Project` is pushed through
  `Aggregate` (Luis Fernando Kauer)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2012">CALCITE-2012</a>]
  Replace `LocalInterval` by `Interval` in Druid adapter
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1984">CALCITE-1984</a>]
  Incorrect rewriting with materialized views using `DISTINCT` in aggregate
  functions
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1876">CALCITE-1876</a>]
  In CSV example, tweak cost to ensure that `Project` is pushed through
  `Aggregate` (Luis Fernando Kauer)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2037">CALCITE-2037</a>]
  Modify parser template to allow sub-projects to override `SqlStmt` syntax
  (Roman Kulyk)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2019">CALCITE-2019</a>]
  Druid's time column is NOT NULL, so push `COUNT(druid_time_column)` as if it
  were `COUNT(*)`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2034">CALCITE-2034</a>]
  `FileReaderTest` fails with path containing spaces (Marc Prud'hommeaux)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2028">CALCITE-2028</a>]
  `SubQueryRemoveRule` should create `Join`, not `Correlate`, for un-correlated
  sub-queries (Liao Xintao)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2029">CALCITE-2029</a>]
  Query with `IS DISTINCT FROM` condition in `WHERE` or `JOIN` clause fails with
  `AssertionError`, "Cast for just nullability not allowed" (Volodymyr Vysotskyi)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1998">CALCITE-1998</a>]
  Hive `ORDER BY` null values (Abbas Gadhia)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2014">CALCITE-2014</a>]
  Look for `saffron.properties` file in classpath rather than in working
  directory (Arina Ielchiieva)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1910">CALCITE-1910</a>]
  `NullPointerException` on filtered aggregators using `IN`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1762">CALCITE-1762</a>]
  Upgrade to Spark 2.X
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2008">CALCITE-2008</a>]
  Fix braces in `TRIM` signature
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2007">CALCITE-2007</a>]
  Fix `RexSimplify` behavior when literals come first
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2006">CALCITE-2006</a>]
  Push `IS NULL` and `IS NOT NULL` predicates to Druid
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1996">CALCITE-1996</a>]
  In JDBC adapter, generate correct `VALUES` syntax
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2001">CALCITE-2001</a>]
  JDBC driver should return "SYSTEM TABLE" rather than "SYSTEM_TABLE"
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1995">CALCITE-1995</a>]
  Remove terms from `Filter` if predicates indicate they are always true or
  false
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1983">CALCITE-1983</a>]
  Push `=`and `<>` operations with numeric cast on dimensions to Druid
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1960">CALCITE-1960</a>]
  `RelMdPredicates.getPredicates` is slow if there are many equivalent columns
  (Rheet Wong)
* Make Travis CI builds work (Christian Beikov)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1987">CALCITE-1987</a>]
  Implement `EXTRACT` for JDBC (Pavel Gubin)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1988">CALCITE-1988</a>]
  Various code quality issues
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1986">CALCITE-1986</a>]
  Add `RelBuilder.match` and methods for building patterns (Dian Fu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1980">CALCITE-1980</a>]
  `RelBuilder.aggregate` should rename underlying fields if `groupKey` contains
   an alias
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1946">CALCITE-1946</a>]
  JDBC adapter should generate sub-`SELECT` if dialect does not support nested
  aggregate functions (Pawel Ruchaj)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1976">CALCITE-1976</a>]
  linq4j: support List and Map literals

#### Web site and documentation

* Update PMC Chair
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2052">CALCITE-2052</a>]
  Remove SQL code style from materialized views documentation
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2036">CALCITE-2036</a>]
  Fix "next" link in [powered_by.html](powered_by.html)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2038">CALCITE-2038</a>]
  Fix incomplete sentence in tutorial
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2021">CALCITE-2021</a>]
  Document the interfaces that you can use to extend Calcite
* Javadoc fixes (Alexey Roytman)
* Add two talks, and committer Christian Beikov
* Fix URL in `FileSchemaFactory` javadoc (Marc Prud'hommeaux)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1989">CALCITE-1989</a>]
  Check dependencies for vulnerabilities each release

## <a href="https://github.com/apache/calcite/releases/tag/calcite-1.14.0">1.14.0</a> / 2017-09-06
{: #v1-14-0}

This release brings some big new features.
The `GEOMETRY` data type was added along with 35 associated functions as the start of support for Simple Feature Access.
There are also two new adapters.
Firstly, the Elasticsearch 5 adapter which now exists in parallel with the previous Elasticsearch 2 adapter.
Additionally there is now an [OS adapter]({{ site.baseurl }}/docs/os_adapter.html) which exposes operating system metrics as relational tables.
`ThetaSketch` and `HyperUnique` support has also been added to the Druid adapter.

Several minor improvements are added as well including improved `MATCH_RECOGNIZE` support, quantified comparison predicates, and `ARRAY` and `MULTISET` support for UDFs.
A full list of new features is given below.

There are also a few breaking changes.
The return type of `RANK` and other aggregate functions has been changed.
There also changes to `Aggregate` in order to improve compatibility with Apache Hive.
Finally, the `Schema#snapshot()` interface has been upgraded to allow for more flexible versioning.

Compatibility: This release is tested
on Linux, macOS, Microsoft Windows;
using Oracle JDK 1.7, 1.8, 9;
Guava versions 14.0 to 21.0;
Druid version 0.11.0;
other software versions as specified in `pom.xml`.

#### New features

* [<a href="https://issues.apache.org/jira/browse/CALCITE-1968">CALCITE-1968</a>]  OpenGIS Simple Feature Access SQL 1.2.1: add `GEOMETRY` data type and first 35 functions
  Add Spatial page, document GIS functions in SQL reference (indicating
  which ones are implemented), and add "countries" data set for testing.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1967">CALCITE-1967</a>]  Elasticsearch 5 adapter (Christian Beikov)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1911">CALCITE-1911</a>]  In `MATCH_RECOGNIZE`, support `WITHIN` sub-clause (Dian Fu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1897">CALCITE-1897</a>]  Add '%' operator as an alternative to 'MOD' (sunjincheng)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1787">CALCITE-1787</a>]  Add `ThetaSketch` and `HyperUnique` support to Calcite via rolled up columns (Zain Humayun)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1896">CALCITE-1896</a>]  OS adapter and `sqlsh`
  * Vmstat table function for sqlsh
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1864">CALCITE-1864</a>]  Allow `NULL` literal as argument
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1834">CALCITE-1834</a>]  Allow user-defined functions to have arguments that are `ARRAY` or `MULTISET` (Ankit Singhal)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1886">CALCITE-1886</a>]  Support `"LIMIT [offset,] row_count"`, per MySQL (Kaiwang Chen)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1845">CALCITE-1845</a>]  Quantified comparison predicates (SOME, ANY, ALL)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1709">CALCITE-1709</a>]  Support mixing table columns with extended columns in DML (Rajeshbabu Chintaguntla)

#### Bug-fixes, API changes and minor enhancements

* [<a href="https://issues.apache.org/jira/browse/CALCITE-1931">CALCITE-1931</a>]
  Change the return type of `RANK` and other aggregate functions.
  Various aggregate functions that used to return `INTEGER` now return other
  types: `RANK`, `DENSE_RANK`, and `NTILE` now return `BIGINT`;
  `CUME_DIST` and `PERCENT_RANK` now return `DOUBLE`.
  (**This is a breaking change**.)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1947">CALCITE-1947</a>]  Add `TIME`/`TIMESTAMP` with local time zone types to optimizer
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1972">CALCITE-1972</a>]  Create `.sha512` and `.md5` digests for release artifacts
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1941">CALCITE-1941</a>]  Refine interface `Schema#snapshot()`
  (**This is a breaking change**.)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1069">CALCITE-1069</a>]  In `Aggregate`, deprecate indicators, and allow `GROUPING` to be used as an aggregate function
  (**This is a breaking change**.)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1969">CALCITE-1969</a>]  Annotate user-defined functions as strict and semi-strict
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1945">CALCITE-1945</a>]  Make return types of `AVG`, `VARIANCE`, `STDDEV` and `COVAR` customizable via RelDataTypeSystem
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1966">CALCITE-1966</a>]  Allow normal views to act as materialization table (Christian Beikov)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1953">CALCITE-1953</a>]  Rewrite `"NOT (x IS FALSE)" to "x IS NOT FALSE"; "x IS TRUE"` would be wrong
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1943">CALCITE-1943</a>]  Add back `NavigationExpander` and `NavigationReplacer` in `SqlValidatorImpl` (Dian Fu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1963">CALCITE-1963</a>]  Upgrade checkstyle, and fix code to comply
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1944">CALCITE-1944</a>]  Window function applied to sub-query that returns dynamic star gets wrong plan (Volodymyr Vysotskyi)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1954">CALCITE-1954</a>]  Column from outer join should be null, whether or not it is aliased
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1959">CALCITE-1959</a>]  Reduce the amount of metadata and `tableName` calls in Druid (Zain Humayun)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1930">CALCITE-1930</a>]  Fix `AggregateExpandDistinctAggregatesRule` when there are multiple `AggregateCalls` referring to the same input
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1936">CALCITE-1936</a>]  Allow `ROUND()` and `TRUNCATE()` to take one operand, defaulting scale to 0
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1931">CALCITE-1931</a>]  Change the return type of RANK and other aggregate functions
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1932">CALCITE-1932</a>]  `Project.getPermutation()` should return null if not a permutation (e.g. repeated `InputRef`)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1925">CALCITE-1925</a>]  In `JaninoRelMetadataProvider`, cache null values (Ted Xu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1849">CALCITE-1849</a>]  Support `RexSubQuery` in `RelToSqlConverter`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1909">CALCITE-1909</a>]  Output `rowType` of Match should include `PARTITION BY` and `ORDER BY` columns
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1929">CALCITE-1929</a>]  Deprecate class `RelDataTypeFactory.FieldInfoBuilder`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1895">CALCITE-1895</a>]  MSSQL's SUBSTRING operator has different syntax (Chris Baynes)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1919">CALCITE-1919</a>]  `NullPointerException` when target in `ReflectiveSchema` belongs to root package (Lim Chee Hau)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1907">CALCITE-1907</a>]  Table function with 1 column gives `ClassCastException`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1841">CALCITE-1841</a>]  Create handlers for JDBC dialect-specific generated SQL (Chris Baynes)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1898">CALCITE-1898</a>]  `LIKE` must match '.' (period) literally
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1900">CALCITE-1900</a>]  Detect cyclic views and give useful error message
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1893">CALCITE-1893</a>]  Add MYSQL_5 conformance
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1883">CALCITE-1883</a>]  `HepPlanner` should force garbage collect whenever a root registered (Ted Xu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1889">CALCITE-1889</a>]  Accept compound identifiers in `SqlValidatorUtil.checkIdentifierListForDuplicates()` (Rajeshbabu Chintaguntla)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1881">CALCITE-1881</a>]  Can't distinguish overloaded user-defined functions that have DATE and TIMESTAMP arguments (余启)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1803">CALCITE-1803</a>]  Push Project that follows Aggregate down to Druid (Junxian Wu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1828">CALCITE-1828</a>]  Push the FILTER clause into Druid as a Filtered Aggregator (Zain Humayun)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1871">CALCITE-1871</a>]  Nesting `LAST` within `PREV` is not parsed correctly for `MATCH_RECOGNIZE`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1877">CALCITE-1877</a>]  Move the Pig test data files into target for the test runtime
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1815">CALCITE-1815</a>]  Switch Pig adapter to depend on avatica-core instead of full avatica
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1826">CALCITE-1826</a>]  Generate dialect-specific SQL for `FLOOR` operator when in a `GROUP BY` (Chris Baynes)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1842">CALCITE-1842</a>]  `Sort.computeSelfCost()`` calls `makeCost()`` with arguments in wrong order (Junxian Wu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1874">CALCITE-1874</a>]  In Frameworks, make `SqlToRelConverter` configurable
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1873">CALCITE-1873</a>]  In a "GROUP BY ordinal" query, validator gives invalid "Expression is not being grouped" error if column has alias
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1833">CALCITE-1833</a>]  User-defined aggregate functions with more than one parameter (hzyuemeng1)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1860">CALCITE-1860</a>]  Duplicate null predicates cause `NullPointerException` in `RexUtil` (Ruidong Li)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1859">CALCITE-1859</a>]  NPE in validate method of `VolcanoPlanner`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1818">CALCITE-1818</a>]  Handle `SqlKind.DYNAMIC` (parameters) in `SqlImplementor` (Dylan Adams)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1856">CALCITE-1856</a>]  Add option `StructKind.PEEK_FIELDS_NO_EXPAND`, similar to `PEEK_FIELDS` but is not expanded in `"SELECT *"` (Shuyi Chen)

#### Web site and documentation

* Add committer Chris Baynes
* Add DataEngConf talk
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1901">CALCITE-1901</a>]  SQL reference should say that "ONLY" is required after "FETCH ... ROWS"

## <a href="https://github.com/apache/calcite/releases/tag/calcite-1.13.0">1.13.0</a> / 2017-06-20
{: #v1-13-0}

This release comes three months after 1.12.0. It includes more than 75 resolved issues, comprising
a large number of new features as well as general improvements and bug-fixes.

First, Calcite has been upgraded to use
<a href="https://issues.apache.org/jira/browse/CALCITE-1807">Avatica 1.10.0</a>,
which was recently released.

Moreover, Calcite core includes improvements which aim at making it more powerful, stable and robust.
In addition to numerous bux-fixes, we have implemented a
<a href="https://issues.apache.org/jira/browse/CALCITE-1731">new materialized view rewriting algorithm</a>
and <a href="https://issues.apache.org/jira/browse/CALCITE-1682">new metadata providers</a> which
should prove useful for data processing systems relying on Calcite.

In this release, we have also completed the work to
<a href="https://issues.apache.org/jira/browse/CALCITE-1570">support the `MATCH_RECOGNIZE` clause</a>
used in complex-event processing (CEP).

In addition, more progress has been made for the different adapters.
For instance, the Druid adapter now relies on
<a href="https://issues.apache.org/jira/browse/CALCITE-1771">Druid 0.10.0</a> and
it can generate more efficient plans where most of the computation can be pushed to Druid,
e.g., <a href="https://issues.apache.org/jira/browse/CALCITE-1707">using extraction functions</a>.

There is one minor but potentially breaking API change in
[<a href="https://issues.apache.org/jira/browse/CALCITE-1788">CALCITE-1788</a>]
\(Simplify handling of position in the parser\), requiring changes in the parameter
lists of parser extension methods.

Compatibility: This release is tested
on Linux, macOS, Microsoft Windows;
using Oracle JDK 1.7, 1.8, 9;
Guava versions 14.0 to 21.0;
Druid version 0.10.0;
other software versions as specified in `pom.xml`.

#### New features

* [<a href="https://issues.apache.org/jira/browse/CALCITE-1570">CALCITE-1570</a>]
  Add `MATCH_RECOGNIZE` operator, for event pattern-matching
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-1647">CALCITE-1647</a>]
    Classifier and `match_number` syntax support for `MATCH_RECOGNIZE`
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-1646">CALCITE-1646</a>]
    Partition by and order by syntax support for `MATCH_RECOGNIZE` (Zhiqiang-He)
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-1645">CALCITE-1645</a>]
    Row per match syntax support for `MATCH_RECOGNIZE` (Zhiqiang-He)
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-1644">CALCITE-1644</a>]
    Subset clause syntax support for `MATCH_RECOGNIZE` (Zhiqiang-He)
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-1643">CALCITE-1643</a>]
    `AFTER MATCH` sub-clause of `MATCH_RECOGNIZE` clause (Zhiqiang-He)
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-1642">CALCITE-1642</a>]
    Support `MEASURES` clause in `MATCH_RECOGNIZE` (Zhiqiang-He)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1853">CALCITE-1853</a>]
  Push Count distinct into Druid when approximate results are acceptable (Zain Humayun)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1829">CALCITE-1829</a>]
  Add `TIME`/`TIMESTAMP`/`DATE` datatype handling to `RexImplicationChecker`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1613">CALCITE-1613</a>]
  Implement `EXTRACT` for time unit `DOW`, `DOY`; and fix `CENTURY`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1807">CALCITE-1807</a>]
  Upgrade to Avatica 1.10
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1802">CALCITE-1802</a>]
  Add post-aggregation step for Union in materialized view rewriting
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1795">CALCITE-1795</a>]
  Extend materialized view rewriting to produce rewritings using `Union` operators
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1797">CALCITE-1797</a>]
  Support view partial rewriting in aggregate materialized view rewriting
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1791">CALCITE-1791</a>]
  Support view partial rewriting in join materialized view rewriting
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1731">CALCITE-1731</a>]
  Rewriting of queries using materialized views with joins and aggregates
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1780">CALCITE-1780</a>]
  Add `required Order` and `requiresOver` parameters to the constructor of `SqlUserDefinedAggregate Function` (SunJincheng)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1306">CALCITE-1306</a>]
  Allow `GROUP BY` and `HAVING` to reference `SELECT` expressions by ordinal and alias (Rajeshbabu Chintaguntla)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1781">CALCITE-1781</a>]
  Allow expression in `CUBE` and `ROLLUP`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1771">CALCITE-1771</a>]
  Upgrade to Druid 0.10.0 (Nishant Bangarwa)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1772">CALCITE-1772</a>]
  Add a hook to allow `RelNode` expressions to be executed by JDBC driver
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1766">CALCITE-1766</a>]
  Support system functions having no args with parenthesis too (Ankit Singhal)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1760">CALCITE-1760</a>]
  Implement utility method to identify lossless casts
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1682">CALCITE-1682</a>]
  New metadata providers for expression column origin and all predicates in plan
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1753">CALCITE-1753</a>]
  Push expressions into null-generating side of a join if they are "strong" (null-preserving)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1759">CALCITE-1759</a>]
  Add SQL:2014 reserved words to parser
* [<a href="https://issues.apache.org/jira/browse/CALCITE-476">CALCITE-476</a>]
  Support distinct aggregates in window functions
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1738">CALCITE-1738</a>]
  Support `CAST` of literal values in filters pushed to Druid (Remus Rusanu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1758">CALCITE-1758</a>]
  Push to Druid `OrderBy`/`Limit` operation over time dimension and additional columns (Slim Bouguerra)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1707">CALCITE-1707</a>]
  Push `Extraction` filter on `Year`/`Month`/`Day` to druid (Slim Bouguerra)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1725">CALCITE-1725</a>]
  Push project aggregate of time extract to druid (Slim Bouguerra)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1747">CALCITE-1747</a>]
  `RelMdColumnUniqueness` for `HepRelVertex`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1749">CALCITE-1749</a>]
  Push filter conditions partially into Druid
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1730">CALCITE-1730</a>]
  Upgrade Druid to 0.9.2 (Nishant Bangarwa)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1702">CALCITE-1702</a>]
  Support extended columns in DML (Kevin Liew)

#### Bug-fixes, API changes and minor enhancements

* [<a href="https://issues.apache.org/jira/browse/CALCITE-1855">CALCITE-1855</a>]
  Fix float values in Cassandra adapter
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1848">CALCITE-1848</a>]
  Rename MySource to FileSource (Darion Yaphet)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1852">CALCITE-1852</a>]
  Fix for `UnionMergeRule` to deal correctly with `EXCEPT`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1850">CALCITE-1850</a>]
  Extend `UnionMergeRule` to deal with more than 2 branches (Pengcheng Xiong)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1805">CALCITE-1805</a>]
  Druid adapter incorrectly pushes down `COUNT(c)`; Druid only supports `COUNT(*)`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1846">CALCITE-1846</a>]
  Metadata pulled up predicates should skip non-deterministic calls (Ted Xu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1819">CALCITE-1819</a>]
  Druid Adapter does not push the boolean operator `<>` as a filter correctly (Zain Humayun)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1798">CALCITE-1798</a>]
  In JDBC adapter, generate dialect-specific SQL for `FLOOR` operator (Chris Baynes)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1812">CALCITE-1812</a>]
  Provide `RelMetadataQuery` from planner to rules and invalidate in `transformTo` (Remus Rusanu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1804">CALCITE-1804</a>]
  Cannot assign `NOT NULL` array to `NULLABLE` array (Ankit Singhal)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1810">CALCITE-1810</a>]
  Allow `NULL` for `ARRAY` constructor (Ankit Singhal)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1830">CALCITE-1830</a>]
  `ProcessBuilder` is security sensitive; move it to test suite to prevent accidents
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1816">CALCITE-1816</a>]
  `JaninoRelMetadataProvider` generated classes leak ACTIVE nodes on exception (Remus Rusanu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1690">CALCITE-1690</a>]
  Calcite timestamp literals cannot express precision above millisecond, `TIMESTAMP(3)`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1664">CALCITE-1664</a>]
  `CAST(<string> as TIMESTAMP)` adds part of sub-second fraction to the value
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1742">CALCITE-1742</a>]
  Create a read-consistent view of CalciteSchema for each statement compilation
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1800">CALCITE-1800</a>]
  JDBC adapter fails on query with `UNION` in `FROM` clause (Viktor Batytskyi, Minji Kim)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1788">CALCITE-1788</a>]
  Simplify handling of position in the parser
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1782">CALCITE-1782</a>]
  `AggregateExpandDistinctAggregatesRule` should work on `Aggregate` instead of `LogicalAggregate` (Haohui Mai)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1293">CALCITE-1293</a>]
  Bad code generated when argument to `COUNT(DISTINCT)` is a `GROUP BY` column
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1770">CALCITE-1770</a>]
  `CAST(NULL AS ...)` gives NPE (Slim Bouguerra)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1777">CALCITE-1777</a>]
  `WHERE FALSE` causes `AssertionError` (Slim Bouguerra)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1778">CALCITE-1778</a>]
  Query with `WHERE CASE` throws `AssertionError` "Cast for just nullability not allowed"
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1773">CALCITE-1773</a>]
  Add Test sql validator test for Pattern skip syntax in `MATCH_RECOGNIZE` (Zhiqiang-He)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1761">CALCITE-1761</a>]
  `TUMBLE`/`HOP`/`SESSION_START`/`END` do not resolve time field correctly
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1765">CALCITE-1765</a>]
  Druid adapter fail when the extract granularity is not supported (Slim Bouguerra)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1767">CALCITE-1767</a>]
  Fix join/aggregate rewriting rule when same table is referenced more than once
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1764">CALCITE-1764</a>]
  Adding sort ordering type for druid sort json field (Slim Bouguerra)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-715">CALCITE-715</a>]
  Add `PERIOD` type constructor and period operators (`CONTAINS`, `PRECEDES`, etc.)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1456">CALCITE-1456</a>]
  Change `SubstitutionVisitor` to use generic `RelBuilder` instead of Logical instances of the operators when possible
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1763">CALCITE-1763</a>]
  Recognize lossless casts in join/aggregate materialized view rewriting rule
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1639">CALCITE-1639</a>]
  `TIMESTAMPADD(MONTH, ...)` should return last day of month if the day overflows
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1754">CALCITE-1754</a>]
  In Csv adapter, convert `DATE` and `TIME` values to `int`, and `TIMESTAMP` values to `long` (Hongbin Ma)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1751">CALCITE-1751</a>]
  `PigRelBuilderStyleTest` test cases are flapping
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1750">CALCITE-1750</a>]
  Fix unit test failures when the path to the repository contains spaces
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1724">CALCITE-1724</a>]
  Wrong comparison for floats/double type in Druid (Slim Bouguerra)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1734">CALCITE-1734</a>]
  Select query result not parsed correctly with druid 0.9.2 (Nishant Bangarwa)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1732">CALCITE-1732</a>]
  `IndexOutOfBoundsException` when using `LATERAL TABLE` with more than one field (Godfrey He)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1722">CALCITE-1722</a>]
  Druid adapter uses un-scaled value of `DECIMAL` literals (Slim Bouguerra)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1723">CALCITE-1723</a>]
  Match `DruidProjectFilterTransposeRule` against `DruidQuery` (Nishant Bangarwa)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1714">CALCITE-1714</a>]
  Do not push group by on druid metrics fields (Slim Bouguerra)

#### Web site and documentation

* Michael Mior joins PMC
* Add 3 new committers (Zhiqiang-He, Kevin Liew, Slim Bouguerra)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1854">CALCITE-1854</a>]
  Fix value range of TINYINT in documentation (James Xu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1827">CALCITE-1827</a>]
  Document `TIMESTAMPADD`, `TIMESTAMPDIFF` functions (SunJincheng)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1796">CALCITE-1796</a>]
  Update materialized views documentation
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1566">CALCITE-1566</a>]
  Better documentation on the use of materialized views


## <a href="https://github.com/apache/calcite/releases/tag/calcite-1.12.0">1.12.0</a> / 2017-03-24
{: #v1-12-0}

[Features of note](../news/2017/03/24/release-1.12.0) this release are
JDK 9 support,
the new file/web and Apache Pig adapters,
general improvements to the Druid adapter,
more helpful error messages if you get a table or column name wrong,
improved the plans for correlated sub-queries,
support for `TUMBLE`, `HOP` and `SESSION` window functions in streaming and regular queries,
experimental support for `MATCH_RECOGNIZE` clause for complex-event processing (CEP),
several new built-in functions to comply with the ODBC/JDBC standard.

Compatibility: This release is tested
on Linux, macOS, Microsoft Windows;
using Oracle JDK 1.7, 1.8, 9;
Guava versions 14.0 to 21.0;
Druid version 0.9.1.1;
other software versions as specified in `pom.xml`.

### New features

* [<a href="https://issues.apache.org/jira/browse/CALCITE-1666">CALCITE-1666</a>]
  Support for modifiable views with extended columns (Kevin Liew)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1655">CALCITE-1655</a>]
  Druid adapter: add `IN` filter (Slim Bouguerra)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1641">CALCITE-1641</a>]
  Add parser and validator support for `MATCH_RECOGNIZE`, a new clause for
  complex-event processing (CEP) (Zhiqiang-He)
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-1686">CALCITE-1686</a>]
    Only allow `FINAL` and other functions inside `MATCH_RECOGNIZE` (Zhiqiang-He)
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-1689">CALCITE-1689</a>]
    Remove `PATTERN_DEFINE_AS` in SqlStdOperatorTable; `MATCH_RECOGNIZE` now uses
    `AS` (Zhiqiang-He)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1668">CALCITE-1668</a>]
  Simplify `1 = 1` to `TRUE`, `1 > 2` to `FALSE` (Kevin Risden)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1598">CALCITE-1598</a>]
  Pig adapter (Eli Levine)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1661">CALCITE-1661</a>]
  Druid adapter: Support aggregation functions on `DECIMAL` columns
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1615">CALCITE-1615</a>]
  Support `HOP` and `SESSION` functions in the `GROUP BY` clause
  (Julian Hyde and Haohui Mai)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1494">CALCITE-1494</a>]
  More efficient plan for correlated sub-queries, omitting value-generating
  scans where possible
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1638">CALCITE-1638</a>]
  Simplify `$x = $x` to `$x is not null`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-884">CALCITE-884</a>]
  File adapter (Henry Olson)
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-1704">CALCITE-1704</a>]
    Execute queries on CSV files using simple `sqlline` command
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-1676">CALCITE-1676</a>]
    Scan directory for .csv, .json and .gz files
  * Allow multiple Calcite columns to be derived from one HTML column,
    e.g. Location &rarr; Lat, Lon
  * Improved pattern match: added `matchSeq` to allow selection of
    <i>n</i>th match
  * Add replace patterns to cell parsing logic
  * Add handling for tables without `<TH>` elements
  * Unit tests using local files (URL tests are contingent on network
    access)
  * Ability to parse HTML, CSV and JSON from local files
  * Combine the <a href="https://github.com/HenryOlson/optiq-web">optiq-web</a>
    project with code from the
    <a href="{{ site.apiroot }}/org/apache/calcite/adapter/csv/package-summary.html">CSV adapter</a>
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1652">CALCITE-1652</a>]
  Allow `GROUPING` function to have multiple arguments, like `GROUPING_ID`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1634">CALCITE-1634</a>]
  Make `RelBuilder.distinct` no-op if input is already distinct; use it in
  `RelDecorrelator`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1635">CALCITE-1635</a>]
  Add `MinRowCount` metadata
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1628">CALCITE-1628</a>]
  Add an alternative match pattern for `SemiJoinRule`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1618">CALCITE-1618</a>]
  `SortProjectTransposeRule` should check for monotonicity preserving `CAST`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1510">CALCITE-1510</a>]
  In `INSERT`/`UPSERT` without an explicit target column list, allow fewer source
  columns than table (Kevin Liew)
  * Check for default value only when target field is null
    (Rajeshbabu Chintaguntla)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1603">CALCITE-1603</a>]
  Support `TUMBLE` window function in the `GROUP BY` clause (Julian Hyde and
  Haohui Mai)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1606">CALCITE-1606</a>]
  Add datetime scalar functions (Laurent Goujon)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1604">CALCITE-1604</a>]
  Add JDBC/ODBC scalar functions `DATABASE`, `IFNULL`, `USER` (Laurent Goujon)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1549">CALCITE-1549</a>]
  More helpful error message when schema, table or column not found
* [<a href="https://issues.apache.org/jira/browse/CALCITE-420">CALCITE-420</a>]
  Add `REPLACE` function, callable with and without JDBC escape syntax (Riccardo
  Tommasini)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1557">CALCITE-1557</a>]
  Add numeric scalar functions (Laurent Goujon)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1258">CALCITE-1258</a>]
  JDK9

#### Bug-fixes, API changes and minor enhancements

* [<a href="https://issues.apache.org/jira/browse/CALCITE-1716">CALCITE-1716</a>]
  Fix Cassandra integration tests
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1715">CALCITE-1715</a>]
  Downgrade to Guava 19.0 to fix Cassandra incompatibility
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1706">CALCITE-1706</a>]
  Disable `DruidAggregateFilterTransposeRule`, because it causes fine-grained
  aggregations to be pushed to Druid
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1695">CALCITE-1695</a>]
  Add class `RexSimplify`, providing an explicit `RexExecutor` for methods to
  simplify `RexNode`s
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1694">CALCITE-1694</a>]
  Pig adapter: Use the shaded Avatica dependency instead
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1561">CALCITE-1561</a>]
  Make `PigTest` cluster aware of data files; hopefully this will prevent
  intermittent test failures (Eli Levine)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1696">CALCITE-1696</a>]
  Support `RexLocalRef` for `EXPLAIN PLAN AS JSON`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1683">CALCITE-1683</a>]
  Druid-specific rules to transpose `Filter` with other relations
  (Nishant Bangarwa)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1684">CALCITE-1684</a>]
  Change default precision of `VARCHAR` and `VARBINARY` types from 1 to
  "unspecified" (Kevin Liew)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1691">CALCITE-1691</a>]
  `ClassCastException` in `RelOptUtil.containsNullableFields`, attempting to
  convert executor to `RexExecutorImpl`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1688">CALCITE-1688</a>]
  Infinite loop during materialization substitution if query contains `Union`,
  `Minus` or `Intersect`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1665">CALCITE-1665</a>]
  `HAVING` support in `RelToSqlConverter` (Zhiqiang He)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1673">CALCITE-1673</a>]
  In CSV adapter, query with `ORDER BY` or `GROUP BY` on `TIMESTAMP` column
  throws CompileException (Gangadhar Kairi)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1674">CALCITE-1674</a>]
  `LIKE` does not match value that contains newline (Mark Payne)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1675">CALCITE-1675</a>]
  Two-level column name cannot be resolved in `ORDER BY`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1667">CALCITE-1667</a>]
  Forbid calls to JDK APIs that use the default locale, time zone or character
  set
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1656">CALCITE-1656</a>]
  Improve cost function in `DruidQuery` to encourage early column pruning
  (Nishant Bangarwa)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1664">CALCITE-1664</a>]
  `CAST('<string>' as TIMESTAMP)` wrongly adds part of sub-second fraction to the
  value
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1659">CALCITE-1659</a>]
  Simplifying `CAST('YYYY-MM-DD hh:mm:ss.SSS' as TIMESTAMP)` should round the
  sub-second fraction (Remus Rusanu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1439">CALCITE-1439</a>]
  Handle errors during constant reduction
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1653">CALCITE-1653</a>]
  Pass an expression executor to `RexUtil.simplify` for constant reduction (Remus
  Rusanu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1601">CALCITE-1601</a>]
  `DateRangeRules` loses OR filters
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1637">CALCITE-1637</a>]
  Add mutable equivalents for all relational expressions (e.g. `MutableFilter`)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1621">CALCITE-1621</a>]
  Add a cast around the NULL literal in aggregate rules (Anton Mushin)
  * Add `RexBuilder.makeNullLiteral(RelDataType)`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1649">CALCITE-1649</a>]
  Data type mismatch in `EnumerableMergeJoin`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1636">CALCITE-1636</a>]
  JDBC adapter generates wrong SQL for self join with sub-query (Zhiqiang-He)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1633">CALCITE-1633</a>]
  In plans, output `Correlate.joinType` attribute in lower-case, same as
  `Join.joinType`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1632">CALCITE-1632</a>]
  Return type of "datetime + interval" expression
* [<a href="https://issues.apache.org/jira/browse/CALCITE-365">CALCITE-365</a>]
  `AssertionError` while translating query with `WITH` and correlated sub-query
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1623">CALCITE-1623</a>]
  Make sure `DATE`, `TIME` and `TIMESTAMP` literals have `Calendar` with GMT
  timezone
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1619">CALCITE-1619</a>]
  `CAST` is ignored by rules pushing operators into `DruidQuery`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1617">CALCITE-1617</a>]
  Druid adapter: Send timestamp literals to Druid as local time, not UTC
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1500">CALCITE-1500</a>]
  Decouple materialization and lattice substitution from `VolcanoPlanner`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1589">CALCITE-1589</a>]
  Druid adapter: `timeseries` query shows all days, even if no data
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1572">CALCITE-1572</a>]
  `JdbcSchema` throws exception when detecting nullable columns (Wu Xiang)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1610">CALCITE-1610</a>]
  `RelBuilder` sort-combining optimization treats aliases incorrectly (Jess
  Balint)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1595">CALCITE-1595</a>]
  `RelBuilder.call` throws `NullPointerException` if argument types are invalid
  (Jess Balint)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1602">CALCITE-1602</a>]
  Remove uses of deprecated APIs
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1569">CALCITE-1569</a>]
  Code generation for fields of type `java.sql.Date` (Zhen Wang)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1582">CALCITE-1582</a>]
  `RelToSqlConverter` doesn't handle cartesian join (Jess Balint)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1597">CALCITE-1597</a>]
  Obsolete `Util.newInternal`, `.pre`, `.post`, `.permAssert` and
  `Throwables.propagate`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1586">CALCITE-1586</a>]
  JDBC adapter generates wrong SQL if `UNION` has more than two inputs (Zhiqiang
  He)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1535">CALCITE-1535</a>]
  Give error if column referenced in `ORDER BY` is ambiguous (Zhen Wang)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1594">CALCITE-1594</a>]
  `ConventionTraitDef.getConversionData()` is not thread-safe
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1577">CALCITE-1577</a>]
  Druid adapter: Incorrect result - limit on timestamp disappears
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1587">CALCITE-1587</a>]
  Druid adapter: `topN` query returns approximate results
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1578">CALCITE-1578</a>]
  Druid adapter: wrong semantics of `topN` query limit with granularity
* Druid adapter: Add `enum Granularity`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1592">CALCITE-1592</a>]
  `SqlToRelConverter` throws `UnsupportedOperationException` if query has
  `NOT ... NOT IN`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1590">CALCITE-1590</a>]
  Support Guava version 21.0
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1575">CALCITE-1575</a>]
  Literals may lose precision during expression reduction
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1546">CALCITE-1546</a>]
  Wrong plan for `NOT IN` sub-queries with disjunction
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1574">CALCITE-1574</a>]
  Memory leak in maven
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1571">CALCITE-1571</a>]
  Could not resolve view with `SimpleCalciteSchema`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1558">CALCITE-1558</a>]
  `AggregateExpandDistinctAggregatesRule` gets field mapping wrong if group key
  is used in aggregate function (Zhenghua Gao)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1562">CALCITE-1562</a>]
  Update jsr305 from 1.3.9 to 3.0.1
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1563">CALCITE-1563</a>]
  In case-insensitive connection, non-existent tables use alphabetically
  preceding table
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1544">CALCITE-1544</a>]
  `AggregateJoinTransposeRule` fails to preserve row type (Kurt Young)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1543">CALCITE-1543</a>]
  Correlated scalar sub-query with multiple aggregates gives `AssertionError`
  (Kurt Young)

#### Web site and documentation

* Maryann Xue joins PMC
* Add 3 new committers (Gian Merlino, Jess Balint, Laurent Goujon)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1657">CALCITE-1657</a>]
  Release Calcite 1.12.0
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1677">CALCITE-1677</a>]
  Replace duplicate avatica docs with a redirect
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1685">CALCITE-1685</a>]
  Site: `defaultNullCollation` key listed as `materializationsEnabled`
  (Kevin Liew)
* Add MapD to <a href="{{ site.baseurl }}/docs/powered_by.html">Powered by
  Calcite</a> page (Todd Mostak)
* Diagram of logos of projects and products powered by Calcite
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1622">CALCITE-1622</a>]
  Bugs in website example code (Damjan Jovanovic)

## <a href="https://github.com/apache/calcite/releases/tag/calcite-1.11.0">1.11.0</a> / 2017-01-09
{: #v1-11-0}

Nearly three months after the previous release, there is a long list
of improvements and bug-fixes, many of them making planner rules
smarter.

Several adapters have improvements:

* The JDBC adapter can now push down DML (`INSERT`, `UPDATE`, `DELETE`),
  windowed aggregates (`OVER`), `IS NULL` and `IS NOT NULL` operators.
* The Cassandra adapter now supports authentication.
* Several key bug-fixes in the Druid adapter.

For correlated and uncorrelated sub-queries, we generate more
efficient plans (for example, in some correlated queries we no longer
require a sub-query to generate the values of the correlating
variable), can now handle multiple correlations, and have also fixed a
few correctness bugs.

New SQL syntax:

* `CROSS APPLY` and `OUTER APPLY`;
* `MINUS` as a synonym for `EXCEPT`;
* an `AS JSON` option for the `EXPLAIN` command;
* compound identifiers in the target list of `INSERT`, allowing you to
  insert into individual fields of record-valued columns (or column
  families if you are using the Apache Phoenix adapter).

A variety of new and extended built-in functions: `CONVERT`, `LTRIM`,
`RTRIM`, 3-parameter `LOCATE` and `POSITION`, `RAND`, `RAND_INTEGER`,
and `SUBSTRING` applied to binary types.

There are minor but potentially breaking API changes in
[<a href="https://issues.apache.org/jira/browse/CALCITE-1519">CALCITE-1519</a>]
\(interface `SubqueryConverter` becomes `SubQueryConverter` and some
similar changes in the case of classes and methods\) and
[<a href="https://issues.apache.org/jira/browse/CALCITE-1530">CALCITE-1530</a>]
\(rename `Shuttle` to `Visitor`, and create a new class `Visitor<R>`\).
See the cases for more details.

Compatibility: This release is tested
on Linux, macOS, Microsoft Windows;
using Oracle JDK 1.7, 1.8;
Guava versions 14.0 to 19.0;
Druid version 0.9.1.1;
other software versions as specified in `pom.xml`.

#### New features

* [<a href="https://issues.apache.org/jira/browse/CALCITE-1551">CALCITE-1551</a>]
  Preserve alias in `RelBuilder.project` (Jess Balint)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1552">CALCITE-1552</a>]
  Add `RAND` function, returning `DOUBLE` values in the range 0..1
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1414">CALCITE-1414</a>]
  Add `RAND_INTEGER` function, which returns a random integer modulo N (Julian
  Feinauer)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1540">CALCITE-1540</a>]
  Support multiple columns in `PARTITION BY` clause of window function
  (Hongbin Ma)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1534">CALCITE-1534</a>]
  Allow compound identifiers in `INSERT` target column list
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1529">CALCITE-1529</a>]
  Support `CREATE TABLE` in tests (and only in tests)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1527">CALCITE-1527</a>]
  Support DML in the JDBC adapter (Christian Tzolov)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1523">CALCITE-1523</a>]
  In `RelBuilder`, add `field` method to reference input to join by alias (Jess
  Balint)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1524">CALCITE-1524</a>]
  Add a project to the planner root so that rules know which output fields are
  used
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1425">CALCITE-1425</a>]
  Support two-level column structure in `INSERT`/`UPDATE`/`MERGE`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1472">CALCITE-1472</a>]
  Support `CROSS`/`OUTER APPLY` syntax (Jark Wu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1506">CALCITE-1506</a>]
  Push `OVER` clause to underlying SQL via JDBC adapter (Christian Tzolov)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1509">CALCITE-1509</a>]
  Allow overriding the convertlet table in CalcitePrepareImpl (Gian Merlino)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1483">CALCITE-1483</a>]
  Generate simpler logic for `NOT IN` if we can deduce that the key is never null
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1497">CALCITE-1497</a>]
  Infer `IS NOT NULL`, and project predicates
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1489">CALCITE-1489</a>]
  Add rule, `AggregateValuesRule`, that applies to an `Aggregate` on an empty
  relation (Gian Merlino)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1447">CALCITE-1447</a>]
  Implement `INTERSECT DISTINCT` by rewriting to `UNION ALL` and counting
  (Pengcheng Xiong)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1389">CALCITE-1389</a>]
  Add a rewrite rule to use materialized views with joins
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1125">CALCITE-1125</a>]
  `MINUS` as a synonym for `EXCEPT` (enabled in Oracle10 conformance)
  (Chandni Singh)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1453">CALCITE-1453</a>]
  Support `ANY` type with binary comparison and arithmetic operators
  (Jungtaek Lim)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1444">CALCITE-1444</a>]
  Add `CONVERT` function (Laurent Goujon)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1448">CALCITE-1448</a>]
  Add rules to flatten and prune `Intersect` and `Minus`;
  flatten set-operators if the top is `DISTINCT` and bottom is `ALL`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1426">CALCITE-1426</a>]
  Support customized star expansion in `Table`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1454">CALCITE-1454</a>]
  Allow custom implementations of `SqlConformance`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1417">CALCITE-1417</a>]
  In `RelBuilder`, simplify "CAST(literal TO type)" to a literal when possible
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1443">CALCITE-1443</a>]
  Add authentication support in Cassandra adapter
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1404">CALCITE-1404</a>]
  Implement `FILTER` on aggregate functions in `Interpreter`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1418">CALCITE-1418</a>]
  Implement `SUBSTRING` function for `BINARY` and `VARBINARY` (Jungtaek Lim)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1422">CALCITE-1422</a>]
  In JDBC adapter, allow `IS NULL` and `IS NOT NULL` operators in generated SQL
  join condition (Viktor Batytskyi)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1419">CALCITE-1419</a>]
  Implement JDBC functions: `LTRIM`, `RTRIM` and 3-parameter `LOCATE` and
  `POSITION` (Jungtaek Lim)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-917">CALCITE-917</a>]
  Add `AS JSON` as output option for `EXPLAIN`

#### Bug-fixes, API changes and minor enhancements

* [<a href="https://issues.apache.org/jira/browse/CALCITE-1559">CALCITE-1559</a>]
  Convert example models to stricter JSON
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1560">CALCITE-1560</a>]
  Remove `avatica` directory from `sqlline`'s class path
* Remove non-ASCII characters from Java source files
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1511">CALCITE-1511</a>]
  Decorrelation fails if query has more than one `EXISTS` in `WHERE` clause
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1555">CALCITE-1555</a>]
  Improve `RelNode` validation
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1548">CALCITE-1548</a>]
  Instantiate function objects once per query
* Add lock to `JdbcAdapterTest`, to ensure that tests that modify data run in
  series
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1519">CALCITE-1519</a>]
  Standardize on "sub-query" rather than "subquery" in class names and comments
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1493">CALCITE-1493</a>]
  Wrong plan for `NOT IN` correlated queries
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1526">CALCITE-1526</a>]
  Use `Strong` to infer whether a predicate's inputs may be null
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1530">CALCITE-1530</a>]
  Create a visitor to traverse linq4j expressions without mutating them, and
  rename `Visitor` to `Shuttle`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1507">CALCITE-1507</a>]
  `OFFSET` cannot be pushed through a `JOIN` if the non-preserved side of outer
  join is not count-preserving
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1522">CALCITE-1522</a>]
  Fix error message for `SetOp` with incompatible args (Jess Balint)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1532">CALCITE-1532</a>]
  In `HttpUtils`, don't log HTTP requests; they may contain user name, password
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1037">CALCITE-1037</a>]
  Column uniqueness is calculated incorrectly for `Correlate` (Alexey Makhmutov)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1495">CALCITE-1495</a>]
  `SemiJoinRule` should not apply to `RIGHT` and `FULL JOIN`, and should strip
  `LEFT JOIN`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1516">CALCITE-1516</a>]
  Upgrade `hydromatic-resource-maven-plugin`, and re-work `SaffronProperties`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1498">CALCITE-1498</a>]
  Avoid `LIMIT` with trivial `ORDER BY` being pushed through `JOIN` endlessly
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1501">CALCITE-1501</a>]
  `EnumerableUnion` should use array comparator when row format is `ARRAY` (Dayue
  Gao)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1502">CALCITE-1502</a>]
  `AssertionError` in `ReduceExpressionsRule` when `CASE` is used with optional
  value and literal (Serhii Harnyk)
* Cosmetic changes, and deprecate some methods
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1486">CALCITE-1486</a>]
  Invalid "Invalid literal" error for complex expression
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1488">CALCITE-1488</a>]
  `ValuesReduceRule` should ignore empty `Values`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1384">CALCITE-1384</a>]
  Extension point for `ALTER` statements (Gabriel Reid)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1484">CALCITE-1484</a>]
  Upgrade Apache parent POM to version 18
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1482">CALCITE-1482</a>]
  Fix leak on CassandraSchema creation
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1479">CALCITE-1479</a>]
  `AssertionError` in `ReduceExpressionsRule` on multi-column `IN` sub-query
  (Gian Merlino)
* Upgrade `Quidem`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1416">CALCITE-1416</a>]
  Make classes implement `AutoCloseable` where possible (Chinmay Kolhatkar)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1474">CALCITE-1474</a>]
  Upgrade `aggdesigner`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1270">CALCITE-1270</a>]
  Upgrade to `avatica-1.9`, `sqlline-1.2.0`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1461">CALCITE-1461</a>]
  Hard-coded class name in `JaninoRelMetadataProvider` breaks shading (Jark Wu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1465">CALCITE-1465</a>]
  Store constants as a derived field in `RelOptPredicateList`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1429">CALCITE-1429</a>]
  Druid adapter must send `fromNext` when requesting rows from Druid (Jiarong
  Wei)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1430">CALCITE-1430</a>]
  In Druid adapter, `pagingIdentifiers` might have more than one value (Jiarong
  Wei)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1442">CALCITE-1442</a>]
  Interval fractional second precision returns wrong value (Laurent Goujon)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1434">CALCITE-1434</a>]
  User-defined aggregate function that uses a generic interface (Arun Mahadevan)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1431">CALCITE-1431</a>]
  `RelDataTypeFactoryImpl.copyType()` did not copy `StructKind`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1424">CALCITE-1424</a>]
  Druid type is called `FLOAT`, not `DOUBLE` (Jiarong Wei)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1415">CALCITE-1415</a>]
  Add sub-query support for RelStructuredTypeFlattener

#### Web site and documentation

* Change PMC chair
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1459">CALCITE-1459</a>]
  Add Apache Apex to "Powered By" page (Chinmay Kolhatkar)

## <a href="https://github.com/apache/calcite/releases/tag/calcite-1.10.0">1.10.0</a> / 2016-10-12
{: #v1-10-0}

This release comes shortly after 1.9.0. It includes mainly bug-fixes for the core and
Druid adapter. For the latest, we fixed an
<a href="https://issues.apache.org/jira/browse/CALCITE-1403">important issue</a> that
prevented us from handling consistently time dimensions in different time zones.

Compatibility: This release is tested
on Linux, Mac OS X, Microsoft Windows;
using Oracle JDK 1.7, 1.8;
Guava versions 14.0 to 19.0;
Druid version 0.9.1.1;
other software versions as specified in `pom.xml`.

#### New feature

* [<a href="https://issues.apache.org/jira/browse/CALCITE-1374">CALCITE-1374</a>]
  Support operator `!=` as an alternative to `<>`

#### Bug-fixes, API changes and minor enhancements

* [<a href="https://issues.apache.org/jira/browse/CALCITE-1378">CALCITE-1378</a>]
  `ArrayIndexOutOfBoundsException` in sql-to-rel conversion for two-level columns
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1379">CALCITE-1379</a>]
  When expanding `STAR`, expand sub-fields in `RecordType` columns of `StructKind.PEEK_FIELDS` and `StructKind.PEEK_FIELDS_DEFAULT`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1381">CALCITE-1381</a>]
  Function quantifier should be retained in a cloned Sql call (zhengdong)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1386">CALCITE-1386</a>]
  `ITEM` operator ignores the value type of the collection, assigns to Object variable (Jungtaek Lim)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1392">CALCITE-1392</a>]
  Druid default time column not properly recognized
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1394">CALCITE-1394</a>]
  Using `CoreMatchers.containsString` causes javadoc errors
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1396">CALCITE-1396</a>]
  `isDeterministic` only explores top `RexCall`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1397">CALCITE-1397</a>]
  `ClassCastException` in `FilterReduceExpressionsRule`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1398">CALCITE-1398</a>]
  Change visibility of `RelFieldTrimmer` utility methods
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1400">CALCITE-1400</a>]
  `AggregatePullUpConstantsRule` might adjust aggregation function parameters indices wrongly
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1402">CALCITE-1402</a>]
  Druid Filter translation incorrect if input reference is in RHS of comparison
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1403">CALCITE-1403</a>]
  `DruidAdapterIT` broken
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1420">CALCITE-1420</a>]
  Allow Calcite JDBC Driver minor version to be greater than 9

#### Web site and documentation

* [<a href="https://issues.apache.org/jira/browse/CALCITE-1393">CALCITE-1393</a>]
  Exclude packages `org.apache.calcite.benchmarks.generated`, `org.openjdk.jmh` from javadoc

## <a href="https://github.com/apache/calcite/releases/tag/calcite-1.9.0">1.9.0</a> / 2016-09-22
{: #v1-9-0}

This release includes extensions and fixes for the Druid adapter. New features were
added, such as the capability to
<a href="https://issues.apache.org/jira/browse/CALCITE-1357">recognize and translate Timeseries and TopN Druid queries</a>.
Moreover, this release contains multiple bug-fixes over the initial implementation of the
adapter. It is worth mentioning that most of these fixes were contributed by Druid developers,
which demonstrates the good reception of the adapter by that community.

We have added new SQL features too, e.g.,
<a href="https://issues.apache.org/jira/browse/CALCITE-1309">support for `LATERAL TABLE`</a>.
There are multiple interesting extensions to the planner rules that should contribute to
obtain better plans, such as
<a href="https://issues.apache.org/jira/browse/CALCITE-1288">avoiding doing the same join twice</a>
in the presence of `COUNT DISTINCT`, or being able to
<a href="https://issues.apache.org/jira/browse/CALCITE-1220">simplify the expressions</a>
in the plan further. In addition, we implemented a rule to
<a href="https://issues.apache.org/jira/browse/CALCITE-1334">convert predicates on `EXTRACT` function calls into date ranges</a>.
The rule is not specific to Druid; however, in principle, it will be useful to identify
filter conditions on the time dimension of Druid data sources.

Finally, the release includes more than thirty bug-fixes, minor enhancements and internal
changes to planner rules and APIs.

Compatibility: This release is tested
on Linux, Mac OS X, Microsoft Windows;
using Oracle JDK 1.7, 1.8;
Guava versions 14.0 to 19.0;
other software versions as specified in `pom.xml`.

#### New features

* [<a href="https://issues.apache.org/jira/browse/CALCITE-1208">CALCITE-1208</a>]
  Improve two-level column structure handling
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1227">CALCITE-1227</a>]
  Add streaming CSV table (Zhen Wang)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1309">CALCITE-1309</a>]
  Support `LATERAL TABLE` (Jark Wu)

#### Druid adapter

* [<a href="https://issues.apache.org/jira/browse/CALCITE-1292">CALCITE-1292</a>]
  Druid metadata query is very slow (Michael Spector)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1324">CALCITE-1324</a>]
  Druid metadata query throws exception if there are non-standard aggregators (Martin Karlsch)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1343">CALCITE-1343</a>]
  Broken Druid query
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1348">CALCITE-1348</a>]
  In Druid adapter, adjust how `SegmentMetadataQuery` is used to detect types (Gian Merlino)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1357">CALCITE-1357</a>]
  Recognize Druid `Timeseries` and `TopN` queries in `DruidQuery`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1358">CALCITE-1358</a>]
  Push filters on time dimension to Druid

#### Planner rules

* [<a href="https://issues.apache.org/jira/browse/CALCITE-1220">CALCITE-1220</a>]
  Further extend simplify for reducing expressions
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1288">CALCITE-1288</a>]
  Avoid doing the same join twice if count(distinct) exists (Gautam Parai)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1289">CALCITE-1289</a>]
  `RexUtil.simplifyCase()` should account for nullability
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1290">CALCITE-1290</a>]
  When converting to CNF, fail if the expression size exceeds a threshold
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1334">CALCITE-1334</a>]
  Convert predicates on `EXTRACT` function calls into date ranges
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1342">CALCITE-1342</a>]
  `ProjectPusher` should use rel factories when creating new rels, e.g. project/filter
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1365">CALCITE-1365</a>]
  Introduce `UnionPullUpConstantsRule`

#### Bug-fixes, API changes and minor enhancements

* [<a href="https://issues.apache.org/jira/browse/CALCITE-30">CALCITE-30</a>]
  Implement `Statement.cancel` method
* [<a href="https://issues.apache.org/jira/browse/CALCITE-308">CALCITE-308</a>]
  Wrong result when using `DATE`+`INTERVAL` arithmetics
* [<a href="https://issues.apache.org/jira/browse/CALCITE-319">CALCITE-319</a>]
  Table aliases should follow case-sensitivity policy
* [<a href="https://issues.apache.org/jira/browse/CALCITE-528">CALCITE-528</a>]
  Creating output row type of a Join does not obey case-sensitivity flags
* [<a href="https://issues.apache.org/jira/browse/CALCITE-991">CALCITE-991</a>]
  Create separate `SqlFunctionCategory` values for table functions and macros (Julien Le Dem)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1043">CALCITE-1043</a>]
  `RexOptUtil` does not support function table other than `SqlStdOperatorTable`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1095">CALCITE-1095</a>]
  `NOT` precedence
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1148">CALCITE-1148</a>]
  Trait conversion broken for `RelTraits` other than `Convention`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1278">CALCITE-1278</a>]
  CalciteSignature's ColumnMetaData for `DELETE` should be same as `INSERT`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1283">CALCITE-1283</a>]
  Nullability incorrectly assigned in `SqlTypeFactory.leastRestrictiveSqlType()`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1284">CALCITE-1284</a>]
  Move `Quidem` tests from `JdbcTest` into their own class
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1297">CALCITE-1297</a>]
  `RelBuilder` should rename fields without creating an identity Project (Jark Wu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1302">CALCITE-1302</a>]
  Create `SqlTypeName` values for each interval range, e.g. `YEAR_MONTH`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1305">CALCITE-1305</a>]
  Case-insensitive table aliases and `GROUP BY`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1310">CALCITE-1310</a>]
  Infer type of arguments to `BETWEEN` operator (Yiming Liu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1312">CALCITE-1312</a>]
  Return type of `TIMESTAMP_ADD` applied to a `DATE` should be `TIMESTAMP` if unit is smaller than `DAY`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1313">CALCITE-1313</a>]
  Validator should derive type of expression in `ORDER BY`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1314">CALCITE-1314</a>]
  Intermittent failure in `SqlParserTest.testGenerateKeyWords`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1321">CALCITE-1321</a>]
  In-list to join optimization should have configurable in-list size (Gautam Parai)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1327">CALCITE-1327</a>]
  Nested aggregate windowed query fails (Gautam Parai)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1330">CALCITE-1330</a>]
  DB2 does not support character sets in data type
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1332">CALCITE-1332</a>]
  JDBC adapter for DB2 should always use aliases for tables: `x.y.z AS z`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1333">CALCITE-1333</a>]
  `AggFunctions` supported by `JdbcAggregate` should depend on `SqlKind`, instead of operator instance
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1336">CALCITE-1336</a>]
  Add view name to the `ViewExpander` (Julien Le Dem)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1337">CALCITE-1337</a>]
  Lazy evaluate `RexCall` digests (Ted Xu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1340">CALCITE-1340</a>]
  Window aggregates invalid error/error messages in some cases (Gautam Parai)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1344">CALCITE-1344</a>]
  Incorrect inferred precision when `BigDecimal` value is less than 1
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1346">CALCITE-1346</a>]
  Invalid nested window aggregate query with alias (Gautam Parai)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1360">CALCITE-1360</a>]
  Custom schema in file in current directory gives `NullPointerException`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1366">CALCITE-1366</a>]
  Metadata provider should not pull predicates up through `GROUP BY`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1370">CALCITE-1370</a>]
  In `SqlKind`, add `OTHER_DDL` to `DDL` enum set (Rajeshbabu Chintaguntla)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1372">CALCITE-1372</a>]
  Calcite generate wrong field names in JDBC adapter

#### Web site and documentation

* [<a href="https://issues.apache.org/jira/browse/CALCITE-1229">CALCITE-1229</a>]
  Restore API and Test API links to site
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1325">CALCITE-1325</a>]
  Druid adapter requires Guava 14.0 or higher
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1329">CALCITE-1329</a>]
  As part of release, generate a file containing multiple digests

## <a href="https://github.com/apache/calcite/releases/tag/calcite-1.8.0">1.8.0</a> / 2016-06-13
{: #v1-8-0}

This release adds adapters for
<a href="https://issues.apache.org/jira/browse/CALCITE-1253">Elasticsearch</a> and
<a href="https://issues.apache.org/jira/browse/CALCITE-1121">Druid</a>.
It is also now easier to
<a href="https://issues.apache.org/jira/browse/CALCITE-1259">make a JDBC connection based upon a single adapter</a>.

There are several new SQL features: `UNNEST` with
<a href="https://issues.apache.org/jira/browse/CALCITE-855">multiple arguments</a>,
<a href="https://issues.apache.org/jira/browse/CALCITE-1250">MAP arguments</a>
and <a href="https://issues.apache.org/jira/browse/CALCITE-1225">with a JOIN</a>;
a <a href="https://issues.apache.org/jira/browse/CALCITE-1168">DESCRIBE</a> statement;
and a <a href="https://issues.apache.org/jira/browse/CALCITE-1115">TRANSLATE</a>
function like the one in Oracle and PostgreSQL.
We also added support for
<a href="https://issues.apache.org/jira/browse/CALCITE-1120">SELECT without FROM</a>
(equivalent to the `VALUES` clause, and widely used in MySQL and PostgreSQL),
and added a
[conformance]({{ site.baseurl }}/docs/adapter.html#jdbc-connect-string-parameters)
parameter to allow you to selectively enable this and other SQL features.

And a couple of dozen bug-fixes and enhancements to planner rules and APIs.

Compatibility: This release is tested
on Linux, Mac OS X, Microsoft Windows;
using Oracle JDK 1.7, 1.8;
Guava versions 14.0 to 19.0;
other software versions as specified in `pom.xml`.

#### New features

* [<a href="https://issues.apache.org/jira/browse/CALCITE-1177">CALCITE-1177</a>]
  Extend list of supported time units in `EXTRACT`, `CEIL` and `FLOOR` functions
  (Venki Korukanti)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1259">CALCITE-1259</a>]
  Allow connecting to a single schema without writing a model
* [<a href="https://issues.apache.org/jira/browse/CALCITE-750">CALCITE-750</a>]
  Support aggregates within windowed aggregates (Gautam Parai)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1250">CALCITE-1250</a>]
  `UNNEST` applied to `MAP` data type (Johannes Schulte)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1253">CALCITE-1253</a>]
  Elasticsearch adapter (Subhobrata Dey)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1228">CALCITE-1228</a>]
  Bind parameters in `INSERT`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1120">CALCITE-1120</a>]
  `SELECT` without `FROM` (Jimmy Xiang)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-855">CALCITE-855</a>]
  `UNNEST` with multiple arguments
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1225">CALCITE-1225</a>]
  `UNNEST` with `JOIN`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1115">CALCITE-1115</a>]
  Add `TRANSLATE` function with 3 parameters, like the one in Oracle (Javanshir
  Yelchiyev)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1168">CALCITE-1168</a>]
  Add `DESCRIBE` statement (Arina Ielchiieva)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1121">CALCITE-1121</a>]
  Druid adapter
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-1276">CALCITE-1276</a>]
    In Druid adapter, deduce tables and columns if not specified
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1207">CALCITE-1207</a>]
  Allow numeric connection properties, and 'K', 'M', 'G' suffixes

#### Planner rules

* [<a href="https://issues.apache.org/jira/browse/CALCITE-1235">CALCITE-1235</a>]
  Fully push down `LIMIT` + `OFFSET` in Cassandra
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1216">CALCITE-1216</a>]
  Rule to convert `Filter`-on-`Scan` to materialized view (Amogh Margoor)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1200">CALCITE-1200</a>]
  Extend `RelOptUtil.splitJoinCondition` to handle `IS NOT DISTINCT FROM`
  (Venki Korukanti)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1211">CALCITE-1211</a>]
  Allow free use of `CassandraSort` for `LIMIT`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1210">CALCITE-1210</a>]
  Allow UUID filtering in Cassandra
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1182">CALCITE-1182</a>]
  Add `ProjectRemoveRule` to pre-processing program of materialization
  substitution

#### Bug-fixes, API changes and minor enhancements

* [<a href="https://issues.apache.org/jira/browse/CALCITE-1281">CALCITE-1281</a>]
  Druid adapter wrongly returns all numeric values as `int` or `float`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1279">CALCITE-1279</a>]
  Druid "select" query gives `ClassCastException`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1277">CALCITE-1277</a>]
  Rat fails on source distribution due to `git.properties`
* Update KEYS
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1252">CALCITE-1252</a>]
  Handle `ANY` type in `RexBuilder.ensureType` and `TypeFactory.leastRestrictive`
  (Mehand Baid, Minji Kim)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1151">CALCITE-1151</a>]
  Fix `SqlSetOption` to correctly handle `SqlOperator.createCall`
  (Sudheesh Katkam, Minji Kim)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1106">CALCITE-1106</a>]
  Expose constructor for `ProjectJoinTransposeRule` (Minji Kim)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1107">CALCITE-1107</a>]
  Make `SqlSumEmptyIsZeroAggFunction` constructor public (Minji Kim)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1269">CALCITE-1269</a>]
  Replace `IntList` with Guava `Ints` class
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1239">CALCITE-1239</a>]
  Upgrade to avatica-1.8.0
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1266">CALCITE-1266</a>]
  `RelBuilder.field` gets offsets wrong
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1264">CALCITE-1264</a>]
  `Litmus` argument interpolation (Chris Baynes)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1245">CALCITE-1245</a>]
  Allow `RelBuilder.scan` to take qualified table name (Chris Baynes)
* Move code from `Enumerables` to `EnumerableDefaults`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1246">CALCITE-1246</a>]
  Cleanup duplicate variables in `JoinPushThroughJoinRule` (Yi Xinglu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1241">CALCITE-1241</a>]
  Add a Freemarker variable for adding non reserved keyword list to `Parser.jj`
  template (Venki Korukanti)
  * Create a table in `SqlParserTest` of reserved keywords from various versions
    of the SQL standard
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1150">CALCITE-1150</a>]
  Add dynamic record type and dynamic star for schema-on-read table
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1238">CALCITE-1238</a>]
  Unparsing a query with `LIMIT` but no `ORDER BY` gives invalid SQL (Emmanuel
  Bastien)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1230">CALCITE-1230</a>]
  Add SQLSTATE reference data as `enum SqlState` in Avatica, and
  deprecate `SqlStateCodes` in Calcite
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1199">CALCITE-1199</a>]
  Incorrect trimming of `CHAR` when performing cast to `VARCHAR`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1219">CALCITE-1219</a>]
  Add method `SqlOperatorBinding.isOperandLiteral()` (Hsuan-Yi Chu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1222">CALCITE-1222</a>]
  `DatabaseMetaData.getColumnLabel` returns null when query has `ORDER BY`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1215">CALCITE-1215</a>]
  Fix missing override in `CassandraTable`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1212">CALCITE-1212</a>]
  Fix NPE on some Cassandra projects
* Remove trailing spaces from all source files
* Move HTTP utilities from Splunk adapter to core
* Test case for
  [<a href="https://issues.apache.org/jira/browse/PHOENIX-2767">PHOENIX-2767</a>],
  non-constant in `IN`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1166">CALCITE-1166</a>]
  Disallow sub-classes of `RelOptRuleOperand`
* Remove all calls to deprecated methods
* Add class `ConsList`
* More of [<a href="https://issues.apache.org/jira/browse/CALCITE-999">CALCITE-999</a>]
  Clean up maven POM files
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1204">CALCITE-1204</a>]
  Fix invalid Javadoc and suppress checkstyle "errors"
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1170">CALCITE-1170</a>]
  Allow `SqlSetOperator` to be overridden, as a regular `SqlOperator` can
  (Hsuan-Yi Chu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-746">CALCITE-746</a>]
  Allow apache-rat to be run outside of release process

#### Web site and documentation

* [<a href="https://issues.apache.org/jira/browse/CALCITE-1273">CALCITE-1273</a>]
  Following
  [<a href="https://issues.apache.org/jira/browse/CALCITE-306">CALCITE-306</a>],
  update references to `EnumerableTableAccessRel` to `EnumerableTableScan`
* Fix typo in SQL (Yi Xinglu)
* Site: add committer, and post slides/video from Polyalgebra talk
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1203">CALCITE-1203</a>]
  Update to github-pages-67
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1202">CALCITE-1202</a>]
  Lock version of Jekyll for bundler
* Site: add upcoming talks, and fix link to Apache phonebook

## <a href="https://github.com/apache/calcite/releases/tag/calcite-1.7.0">1.7.0</a> / 2016-03-22
{: #v1-7-0}

This is the first Apache Calcite release since
[Avatica became an independent project]({{ site.avaticaBaseurl }}/news/2016/03/03/separate-project/).
Calcite now depends on [Avatica]({{ site.avaticaBaseurl }}/) in the
same way as it does other libraries, via a Maven dependency. To see
Avatica-related changes, see the
[release notes for Avatica 1.7.1]({{ site.avaticaBaseurl }}/docs/history.html#v1-7-1).

We have [added](https://issues.apache.org/jira/browse/CALCITE-1080)
an [adapter]({{ site.baseurl }}/docs/adapter.html) for
[Apache Cassandra](https://cassandra.apache.org/).
You can map a Cassandra keyspace into Calcite as a schema, Cassandra
CQL tables as tables, and execute SQL queries on them, which Calcite
converts into [CQL](https://cassandra.apache.org/doc/cql/CQL.html).
Cassandra can define and maintain materialized views but the adapter
goes further: it can transparently rewrite a query to use a
materialized view even if the view is not mentioned in the query.

This release adds an
[Oracle-compatibility mode](https://issues.apache.org/jira/browse/CALCITE-1066).
If you add `fun=oracle` to your JDBC connect string, you get all of
the standard operators and functions plus Oracle-specific functions
`DECODE`, `NVL`, `LTRIM`, `RTRIM`, `GREATEST` and `LEAST`. We look
forward to adding more functions, and compatibility modes for other
databases, in future releases.

We've replaced our use of JUL (`java.util.logging`)
with [SLF4J](https://slf4j.org/). SLF4J provides an API which Calcite can use
independent of the logging implementation. This ultimately provides additional
flexibility to users, allowing them to configure Calcite's logging within their
own chosen logging framework. This work was done in
[[CALCITE-669](https://issues.apache.org/jira/browse/CALCITE-669)].

For users experienced with configuring JUL in Calcite previously, there are some
differences as some the JUL logging levels do not exist in SLF4J: `FINE`,
`FINER`, and `FINEST`, specifically. To deal with this, `FINE` was mapped
to SLF4J's `DEBUG` level, while `FINER` and `FINEST` were mapped to SLF4J's `TRACE`.

Compatibility: This release is tested
on Linux, Mac OS X, Microsoft Windows;
using Oracle JDK 1.7, 1.8;
Guava versions 12.0.1 to 19.0;
other software versions as specified in `pom.xml`.

#### New features

* [<a href="https://issues.apache.org/jira/browse/CALCITE-1124">CALCITE-1124</a>]
  Add `TIMESTAMPADD`, `TIMESTAMPDIFF` functions (Arina Ielchiieva)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1066">CALCITE-1066</a>]
  Add Oracle function table, and functions `DECODE`, `NVL`, `LTRIM`, `RTRIM`,
  `GREATEST`, `LEAST`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1080">CALCITE-1080</a>]
  Cassandra adapter (Michael Mior)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1062">CALCITE-1062</a>]
  In validation, lookup a (possibly overloaded) operator from an operator
  table (Hsuan-Yi Chu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-551">CALCITE-551</a>]
  Sub-query inside aggregate function

#### Planner rules

* [<a href="https://issues.apache.org/jira/browse/CALCITE-1158">CALCITE-1158</a>]
  Make `AggregateRemoveRule` extensible
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1116">CALCITE-1116</a>]
  Extend `simplify` for reducing expressions
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1104">CALCITE-1104</a>]
  Materialized views in Cassandra (Michael Mior)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1130">CALCITE-1130</a>]
  Add support for operators `IS NULL` and `IS NOT NULL` in
  `RexImplicationChecker` (Amogh Margoor)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1129">CALCITE-1129</a>]
  Extend `JoinUnionTransposeRule` to match `Union` instead of `LogicalUnion`
  (Vasia Kalavri)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1109">CALCITE-1109</a>]
  Fix up condition when pushing `Filter` through `Aggregate` (Amogh Margoor)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1100">CALCITE-1100</a>]
  If constant reduction no-ops, don't create a new `RelNode` (Hsuan-Yi Chu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1076">CALCITE-1076</a>]
  Update `RelMdDistribution` to match other metadata APIs (Ted Xu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1056">CALCITE-1056</a>]
  In `RelBuilder`, simplify predicates, and optimize away `WHERE FALSE`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1059">CALCITE-1059</a>]
  Not valid to convert `Aggregate` on empty to empty if its `GROUP BY` key is empty

#### Bug-fixes, API changes and minor enhancements

* [<a href="https://issues.apache.org/jira/browse/CALCITE-1147">CALCITE-1147</a>]
  Allow multiple providers for the same kind of metadata
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1153">CALCITE-1153</a>]
  Invalid cast created during SQL Join in Oracle (Chris Atkinson)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1146">CALCITE-1146</a>]
  Wrong path in CSV example model (wanglan)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1156">CALCITE-1156</a>]
  Increase Jetty version to 9.2.15.v20160210
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1064">CALCITE-1064</a>]
  Address problematic `maven-remote-resources-plugin`
* In `TimeUnit` add `WEEK`, `QUARTER`, `MICROSECOND` values, and change type of
  `multiplier`
* Deprecate `SqlLiteral.SqlSymbol`; `SqlSymbol` can now wrap any enum
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1078">CALCITE-1078</a>]
  Detach avatica from the core calcite Maven project
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-1077">CALCITE-1077</a>]
    Switch Calcite to the released Avatica 1.7.1
  * Update `groupId` when Calcite POMs reference Avatica modules
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-1137">CALCITE-1137</a>]
    Exclude Avatica from Calcite source release
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1111">CALCITE-1111</a>]
  Upgrade Guava, and test on a range of Guava versions
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1054">CALCITE-1054</a>]
  Wrong code generation for `TIMESTAMP` values that may be `NULL`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-604">CALCITE-604</a>]
  Tune metadata by generating a dispatcher at runtime
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1063">CALCITE-1063</a>]
  Flat lists for 4, 5, 6 elements
* Add Orinoco schema (streaming retail data), accessible from Quidem scripts
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1097">CALCITE-1097</a>]
  Exception when executing query with too many aggregation columns (chenzhifa)
* Add tests for leap days
* [<a href="https://issues.apache.org/jira/browse/CALCITE-553">CALCITE-553</a>]
  In maven, enable compiler profiles by default
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1031">CALCITE-1031</a>]
  In prepared statement, `CsvScannableTable.scan` is called twice
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1046">CALCITE-1046</a>]
  Matchers for testing SQL query results
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1083">CALCITE-1083</a>]
  `SqlNode.equalsDeep` has O(n ^ 2) performance
* [<a href="https://issues.apache.org/jira/browse/CALCITE-998">CALCITE-998</a>]
  Exception when calling `STDDEV_SAMP`, `STDDEV_POP` (Matthew Shaer)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1071">CALCITE-1071</a>]
  Improve hash functions
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1072">CALCITE-1072</a>]
  CSV adapter incorrectly parses `TIMESTAMP` values after noon (Chris Albright)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-669">CALCITE-669</a>]
  Mass removal of Java Logging for SLF4J
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1068">CALCITE-1068</a>]
  Deprecate `Stacks`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1067">CALCITE-1067</a>]
  Test failures due to clashing temporary table names
* [<a href="https://issues.apache.org/jira/browse/CALCITE-864">CALCITE-864</a>]
  Correlation variable has incorrect row type if it is populated by right side
  of a Join
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1021">CALCITE-1021</a>]
  Upgrade Jackson
* [<a href="https://issues.apache.org/jira/browse/CALCITE-999">CALCITE-999</a>]
  Clean up maven POM files

#### Web site and documentation

* [<a href="https://issues.apache.org/jira/browse/CALCITE-1112">CALCITE-1112</a>]
  "Powered by Calcite" page
* Add SQL-Gremlin to Adapters page
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1090">CALCITE-1090</a>]
  Revise Streaming SQL specification
* Appoint Josh Elser to PMC
* Add "Streaming SQL" talk
* [<a href="https://issues.apache.org/jira/browse/CALCITE-623">CALCITE-623</a>]
  Add a small blurb to the site about Jenkins for CI
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1070">CALCITE-1070</a>]
  Upgrade to new Apache logo
* Document how to announce a release
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1074">CALCITE-1074</a>]
  Delete old releases from mirroring system

## <a href="https://github.com/apache/calcite/releases/tag/calcite-1.6.0">1.6.0</a> / 2016-01-22
{: #v1-6-0}

As usual in this release, there are new SQL features, improvements to
planning rules and Avatica, and lots of bug-fixes. We'll spotlight a
couple of features make it easier to handle complex queries.

[<a href="https://issues.apache.org/jira/browse/CALCITE-816">CALCITE-816</a>]
allows you to represent sub-queries (`EXISTS`, `IN` and scalar) as
`RexSubQuery`, a kind of expression in the relational algebra. Until
now, the sql-to-rel converter was burdened with expanding sub-queries,
and people creating relational algebra directly (or via RelBuilder)
could only create 'flat' relational expressions. Now we have planner
rules to expand and de-correlate sub-queries.

Metadata is the fuel that powers query planning. It includes
traditional query-planning statistics such as cost and row-count
estimates, but also information such as which columns form unique
keys, unique and what predicates are known to apply to a relational
expression's output rows. From the predicates we can deduce which
columns are constant, and following
[<a href="https://issues.apache.org/jira/browse/CALCITE-1023">CALCITE-1023</a>]
we can now remove constant columns from `GROUP BY` keys.

Metadata is often computed recursively, and it is hard to safely and
efficiently calculate metadata on a graph of `RelNode`s that is large,
frequently cyclic, and constantly changing.
[<a href="https://issues.apache.org/jira/browse/CALCITE-794">CALCITE-794</a>]
introduces a context to each metadata call. That context can detect
cyclic metadata calls and produce a safe answer to the metadata
request. It will also allow us to add finer-grained caching and
further tune the metadata layer.

Compatibility: This release is tested
on Linux, Mac OS X, Microsoft Windows;
using Oracle JDK 1.7, 1.8;
other software versions as specified in `pom.xml`.

#### New features

* [<a href="https://issues.apache.org/jira/browse/CALCITE-816">CALCITE-816</a>]
  Represent sub-query as a `RexNode`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-854">CALCITE-854</a>]
  Implement `UNNEST ... WITH ORDINALITY`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1003">CALCITE-1003</a>]
  Utility to convert `RelNode` to SQL (Amogh Margoor)
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-1010">CALCITE-1010</a>]
    `FETCH/LIMIT` and `OFFSET` in RelToSqlConverter (Amogh Margoor)
  * Move code from `JdbcImplementor` and `JdbcRules` to new class
    `SqlImplementor`
  * Deduce dialect's null collation from `DatabaseMetaData`
  * Fix `RelToSqlConverterTest` on Windows
* Following
  [<a href="https://issues.apache.org/jira/browse/CALCITE-897">CALCITE-897</a>],
  empty string for `boolean` properties means true
* [<a href="https://issues.apache.org/jira/browse/CALCITE-992">CALCITE-992</a>]
  Validate and resolve sequence reference as a `Table` object
* [<a href="https://issues.apache.org/jira/browse/CALCITE-968">CALCITE-968</a>]
  Stream-to-relation and stream-to-stream joins (Milinda Pathirage)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1041">CALCITE-1041</a>]
  User-defined function that returns `DATE` or `TIMESTAMP` value
* [<a href="https://issues.apache.org/jira/browse/CALCITE-986">CALCITE-986</a>]
  User-defined function with `DATE` or `TIMESTAMP` parameters
* [<a href="https://issues.apache.org/jira/browse/CALCITE-958">CALCITE-958</a>]
  Overloaded Table Functions with named arguments (Julien Le Dem)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-970">CALCITE-970</a>]
  If `NULLS FIRST`/`NULLS LAST` not specified, sort `NULL` values high

#### Avatica features and bug-fixes

* [<a href="https://issues.apache.org/jira/browse/CALCITE-1040">CALCITE-1040</a>]
  Differentiate better between arrays and scalars in protobuf
* [<a href="https://issues.apache.org/jira/browse/CALCITE-934">CALCITE-934</a>]
  Use an OS-assigned ephemeral port for `CalciteRemoteDriverTest`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-767">CALCITE-767</a>]
  Create Avatica RPC endpoints for commit and rollback commands
* [<a href="https://issues.apache.org/jira/browse/CALCITE-983">CALCITE-983</a>]
  Handle nulls in `ErrorResponse`'s protobuf representation better
* [<a href="https://issues.apache.org/jira/browse/CALCITE-989">CALCITE-989</a>]
  Add server's address in each response
* Fix some bugs found by static analysis
* Make all `equals` and `hashCode` methods uniform
* [<a href="https://issues.apache.org/jira/browse/CALCITE-962">CALCITE-962</a>]
  Propagate the cause, not just the cause's message, from `JdbcMeta`

#### Planner rules

* [<a href="https://issues.apache.org/jira/browse/CALCITE-1057">CALCITE-1057</a>]
  Add `RelMetadataProvider` parameter to standard planner `Program`s
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1055">CALCITE-1055</a>]
  `SubQueryRemoveRule` should create `Correlate`, not `Join`, for correlated
  sub-queries
* [<a href="https://issues.apache.org/jira/browse/CALCITE-978">CALCITE-978</a>]
  Enable customizing constant folding rule behavior when a `Filter` simplifies
  to false (Jason Altekruse)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-977">CALCITE-977</a>]
  Make the constant expression `Executor` configurable in `FrameworkConfig`
  (Jason Altekruse)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1058">CALCITE-1058</a>]
  Add method `RelBuilder.empty`, and rewrite LIMIT 0 and WHERE FALSE to it
* [<a href="https://issues.apache.org/jira/browse/CALCITE-996">CALCITE-996</a>]
  Simplify predicate when we create a `Filter` operator
* Simplify `RexProgram`, in particular `(NOT CASE ... END) IS TRUE`, which
  occurs in when `NOT IN` is expanded
* Fix variant of
  [<a href="https://issues.apache.org/jira/browse/CALCITE-923">CALCITE-923</a>]
  that occurs in `RelOptRulesTest.testPushFilterPastProject`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1023">CALCITE-1023</a>]
  and
  [<a href="https://issues.apache.org/jira/browse/CALCITE-1038">CALCITE-1038</a>]
  Planner rule that removes `Aggregate` keys that are constant
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1018">CALCITE-1018</a>]
  `SortJoinTransposeRule` not firing due to `getMaxRowCount(RelSubset)` returning
  null
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1019">CALCITE-1019</a>]
  `RelMdUtil.checkInputForCollationAndLimit()` was wrong with `alreadySorted`
  check
* Not safe to use '=' for predicates on constant expressions that might be null
* [<a href="https://issues.apache.org/jira/browse/CALCITE-993">CALCITE-993</a>]
  Pull up all constant expressions, not just literals, as predicates
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1005">CALCITE-1005</a>]
  Handle null in `getMaxRowCount` for `Aggregate` (Mike Hinchey)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-995">CALCITE-995</a>]
  Sort transpose rules might fall in an infinite loop
* [<a href="https://issues.apache.org/jira/browse/CALCITE-987">CALCITE-987</a>]
  Pushing `LIMIT 0` results in an infinite loop (Pengcheng Xiong)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-988">CALCITE-988</a>]
  `FilterToProjectUnifyRule.invert(MutableRel, MutableRel, MutableProject)`
  works incorrectly
* [<a href="https://issues.apache.org/jira/browse/CALCITE-969">CALCITE-969</a>]
  Composite `EnumerableSort` with `DESC` wrongly sorts `NULL` values low
* [<a href="https://issues.apache.org/jira/browse/CALCITE-959">CALCITE-959</a>]
  Add description to `SortProjectTransposeRule`'s constructor

#### Bug-fixes, API changes and minor enhancements

* [<a href="https://issues.apache.org/jira/browse/CALCITE-1060">CALCITE-1060</a>]
  Fix test deadlock by initializing `DriverManager` before registering `AlternatingDriver`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1047">CALCITE-1047</a>]
  `ChunkList.clear` throws `AssertionError`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1053">CALCITE-1053</a>]
  CPU spin, `ReflectiveRelMetadataProvider.apply` waiting for `HashMap.get`
* Upgrade toolbox, to fix line length issue on Windows
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1051">CALCITE-1051</a>]
  Underflow exception due to scaling IN clause literals (Frankie Bollaert)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-975">CALCITE-975</a>]
  Allow Planner to return validated row type together with SqlNode
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1020">CALCITE-1020</a>]
  Add `MILLISECOND` in `TimeUnit` (Pengcheng Xiong)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-794">CALCITE-794</a>]
  Detect cycles when computing statistics
  (**This is a breaking change**.)
* Tune algorithm that deduces the return type of `AND` expression
* [<a href="https://issues.apache.org/jira/browse/CALCITE-842">CALCITE-842</a>]
  Decorrelator gets field offsets confused if fields have been trimmed
* Fix `NullPointerException` in `SqlJoin.toString()`
* Add `ImmutableBitSet.rebuild()`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-915">CALCITE-915</a>]
  Tests now unset `ThreadLocal` values on exit
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1036">CALCITE-1036</a>]
  `DiffRepository` should not insert new resources at the end of the repository
* [<a href="https://issues.apache.org/jira/browse/CALCITE-955">CALCITE-955</a>]
  `Litmus` (continuation-passing style for methods that check invariants)
* `RelBuilder.project` now does nothing if asked to project the identity with
  the same field names
* Deprecate some `Util` methods, and upgrade last Maven modules to JDK 1.7
* Document `RelOptPredicateList`
* Add `ImmutableNullableList.copyOf(Iterable)`
* Fix "endPosTable already set" error from `javac`
* Add benchmark of `Parser.create(sql).parseQuery()`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1042">CALCITE-1042</a>]
  Ensure that `FILTER` is `BOOLEAN NOT NULL`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1039">CALCITE-1039</a>]
  Assign a `SqlKind` value for each built-in aggregate function
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1030">CALCITE-1030</a>]
  JSON `ModelHandler` calling `SchemaPlus.setCacheEnabled()` causes
  `UnsupportedOperationException` when using `SimpleCalciteSchema`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1028">CALCITE-1028</a>]
  Move populate materializations after sql-to-rel conversion
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1034">CALCITE-1034</a>]
  Use a custom checker for code style rules that Checkstyle cannot express
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1032">CALCITE-1032</a>]
  Verify javadoc of private methods
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1015">CALCITE-1015</a>]
  `OFFSET 0` causes `AssertionError` (Zhen Wang)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1024">CALCITE-1024</a>]
  In a planner test, if a rule should have no effect, state that explicitly
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1016">CALCITE-1016</a>]
  `GROUP BY *constant*` on empty relation should return 0 rows
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1022">CALCITE-1022</a>]
  Rename `.oq` Quidem files to `.iq`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-980">CALCITE-980</a>]
  Fix `AND` and `OR` implementation in `Enumerable` convention
* [<a href="https://issues.apache.org/jira/browse/CALCITE-459">CALCITE-459</a>]
  When parsing SQL, allow single line comment on last line (Zhen Wang)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1009">CALCITE-1009</a>]
  `SelfPopulatingList` is not thread-safe
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1008">CALCITE-1008</a>]
  Replace `Closeable` with `AutoCloseable`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1001">CALCITE-1001</a>]
  Upgrade to quidem-0.7
* [<a href="https://issues.apache.org/jira/browse/CALCITE-990">CALCITE-990</a>]
  In `VolcanoPlanner`, populate `RelOptRuleCall.nodeInputs` for operands of type
  "any"
* [<a href="https://issues.apache.org/jira/browse/CALCITE-966">CALCITE-966</a>]
  `VolcanoPlanner` now clears `ruleNames` in order to avoid rule name
  conflicting error
* Factor user-defined function tests from `JdbcTest` to `UdfTest`, and classes
  into `Smalls`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-974">CALCITE-974</a>]
  Exception while validating `DELETE` (Yuri Au Yong)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-964">CALCITE-964</a>]
  Rename `timezone` connection property to `timeZone`

#### Web site and documentation

* Avatica
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-1033">CALCITE-1033</a>]
    Introduce Avatica protobuf documentation
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-1029">CALCITE-1029</a>]
     Add "purpose" descriptions to Avatica JSON docs
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-984">CALCITE-984</a>]
    Massive cleanup of Avatica JSON docs
* [<a href="https://issues.apache.org/jira/browse/CALCITE-861">CALCITE-861</a>]
  Be explicit that `mvn test` needs to be invoked
* [<a href="https://issues.apache.org/jira/browse/CALCITE-997">CALCITE-997</a>]
  Document keywords
* [<a href="https://issues.apache.org/jira/browse/CALCITE-979">CALCITE-979</a>]
  Broken links in web site
* [<a href="https://issues.apache.org/jira/browse/CALCITE-961">CALCITE-961</a>]
  Web site: Add downloads and Apache navigation links
* [<a href="https://issues.apache.org/jira/browse/CALCITE-960">CALCITE-960</a>]
  Download links for pgp, md5, `KEYS` files, and direct from mirrors
* Remove embedded date-stamps from javadoc; add javadoc for test classes
* [<a href="https://issues.apache.org/jira/browse/CALCITE-965">CALCITE-965</a>]
  Link to downloads page from each release news item

## <a href="https://github.com/apache/calcite/releases/tag/calcite-1.5.0">1.5.0</a> / 2015-11-06
{: #v1-5-0}

Our first release as a top-level Apache project!

Avatica has undergone major improvements,
including a new RPC layer that uses
[protocol buffers](https://developers.google.com/protocol-buffers/),
support for DML statements, better support for bind variables and
unique identifiers for connections and statements.

There are lots of improvements to planner rules, and the logic
that replaces relational expressions with equivalent materializations.

We continue to find more uses for
[RelBuilder]({{ site.baseurl }}/docs/algebra.html).
We now recommend that you use `RelBuilder` whenever you create
relational expressions within a planner rule; the rule can then be
re-used to create different sub-classes of relational expression, and
the builder will perform simple optimizations automatically.

Using `RelBuilder` we built Piglet,
a subset of the classic Hadoop language
[Pig](https://pig.apache.org/).
Pig is particularly interesting because it makes heavy use of nested
multi-sets.  You can follow this example to implement your own query
language, and immediately taking advantage of Calcite's back-ends and
optimizer rules. It's all just algebra, after all!

#### New features

* [<a href="https://issues.apache.org/jira/browse/CALCITE-911">CALCITE-911</a>]
  Add a variant of `CalciteSchema` that does not cache sub-objects
* [<a href="https://issues.apache.org/jira/browse/CALCITE-845">CALCITE-845</a>]
  Derive `SUM`’s return type by a customizable policy (Maryann Xue)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-916">CALCITE-916</a>]
  Support table function that implements `ScannableTable`
  * Example table function that generates mazes and their solutions
* [<a href="https://issues.apache.org/jira/browse/CALCITE-941">CALCITE-941</a>]
  Named, optional and `DEFAULT` arguments to function calls;
  support named arguments when calling table functions and table macros
* [<a href="https://issues.apache.org/jira/browse/CALCITE-910">CALCITE-910</a>]
  Improve handling of `ARRAY`, `MULTISET`, `STRUCT` types
* [<a href="https://issues.apache.org/jira/browse/CALCITE-879">CALCITE-879</a>]
  `COLLECT` aggregate function
* [<a href="https://issues.apache.org/jira/browse/CALCITE-546">CALCITE-546</a>]
  Allow table, column and field called '*'
* [<a href="https://issues.apache.org/jira/browse/CALCITE-893">CALCITE-893</a>]
  Theta join in JDBC adapter
* Linq4j: Implement `EnumerableDefaults` methods (MiNG)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-823">CALCITE-823</a>]
  Add `ALTER ... RESET` statement (Sudheesh Katkam)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-881">CALCITE-881</a>]
  Allow schema.table.column references in `GROUP BY`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-852">CALCITE-852</a>]
  DDL statements
* [<a href="https://issues.apache.org/jira/browse/CALCITE-851">CALCITE-851</a>]
  Add original SQL string as a field in the parser
* [<a href="https://issues.apache.org/jira/browse/CALCITE-819">CALCITE-819</a>]
  Add `RelRoot`, a contract for the result of a relational expression

#### Avatica features and bug-fixes

* [<a href="https://issues.apache.org/jira/browse/CALCITE-951">CALCITE-951</a>]
  Print the server-side stack in the local exception (Josh Elser)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-936">CALCITE-936</a>]
  Make HttpServer configurable (Navis Ryu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-903">CALCITE-903</a>]
  Enable Avatica client to recover from missing server-side state (Josh Elser)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-921">CALCITE-921</a>]
  Fix incorrectness when calling `getString()` on binary data (Josh Elser)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-913">CALCITE-913</a>]
  Construct proper `ColumnMetaData` for arrays (Josh Elser)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-871">CALCITE-871</a>]
  In `JdbcMeta`, register each statement using an id from a generator (Bruno
  Dumon)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-645">CALCITE-645</a>]
  Implement `AvaticaSqlException` to pass server-side exception information to
  clients (Josh Elser)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-912">CALCITE-912</a>]
  Add Avatica `OpenConnectionRequest` (Bruno Dumon)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-919">CALCITE-919</a>]
  Avoid `setScale` on `BigDecimal` when scale is 0 (Josh Elser)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-927">CALCITE-927</a>]
  Call finagle for all calls that return ResultSetResponses (Josh Elser)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-705">CALCITE-705</a>]
  DML in Avatica, and split `Execute` out from `Fetch` request (Yeong Wei)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-914">CALCITE-914</a>]
  Add `JsonSubType` for `ExecuteResponse`, and fix JSON docs (Josh Elser)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-905">CALCITE-905</a>]
  `getTables` returns empty result in `JdbcMeta` (Jan Van Besien)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-906">CALCITE-906</a>]
  Avatica `JdbcMeta` statement IDs are not unique
* [<a href="https://issues.apache.org/jira/browse/CALCITE-866">CALCITE-866</a>]
  Break out Avatica documentation and add JSON reference (Josh Elser)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-843">CALCITE-843</a>]
  `AvaticaConnection.getAutoCommit` throws `NullPointerException`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-840">CALCITE-840</a>]
  Protocol buffer serialization over HTTP for Avatica Server (Josh Elser)

#### Materializations

* [<a href="https://issues.apache.org/jira/browse/CALCITE-952">CALCITE-952</a>]
  Organize applicable materializations in reversed topological order (Maryann
  Xue)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-890">CALCITE-890</a>]
  Register all combinations of materialization substitutions (Maryann Xue)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-891">CALCITE-891</a>]
  When substituting materializations, match `TableScan` without `Project`
  (Maryann Xue)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-890">CALCITE-890</a>]
  Register all combinations of materialization substitutions (Maryann Xue)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-925">CALCITE-925</a>]
  Match materialized views when predicates contain strings and ranges (Amogh
  Margoor)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-793">CALCITE-793</a>]
  Planner requires unnecessary collation when using materialized view (Maryann
  Xue)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-825">CALCITE-825</a>]
  Allow user to specify sort order of an `ArrayTable`

#### Planner rules

* [<a href="https://issues.apache.org/jira/browse/CALCITE-953">CALCITE-953</a>]
  Improve `RelMdPredicates` to deal with `RexLiteral` (Pengcheng Xiong)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-939">CALCITE-939</a>]
  Variant of `SortUnionTransposeRule` for order-preserving `Union`
  (Maryann Xue)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-931">CALCITE-931</a>]
  Wrong collation trait in `SortJoinTransposeRule` for right joins
  (Maryann Xue)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-938">CALCITE-938</a>]
  More accurate rowCount for `Aggregate` applied to already unique keys
  (Maryann Xue)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-935">CALCITE-935</a>]
  Improve how `ReduceExpressionsRule` handles duplicate constraints (Pengcheng
  Xiong)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-922">CALCITE-922</a>]
  Extract value of an `INTERVAL` literal (Hsuan-Yi Chu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-889">CALCITE-889</a>]
  Implement `SortUnionTransposeRule` (Pengcheng Xiong)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-909">CALCITE-909</a>]
  Make `ReduceExpressionsRule` extensible
* [<a href="https://issues.apache.org/jira/browse/CALCITE-856">CALCITE-856</a>]
  Make more rules extensible
* [<a href="https://issues.apache.org/jira/browse/CALCITE-902">CALCITE-902</a>]
  Match nullability when reducing expressions in a `Project`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-895">CALCITE-895</a>]
  Simplify "(`CASE` ... `END`) = constant" inside `AND` or `OR` (Hsuan-Yi Chu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-828">CALCITE-828</a>]
  Use RelBuilder in rules rather than type-specific RelNode factories
* [<a href="https://issues.apache.org/jira/browse/CALCITE-892">CALCITE-892</a>]
  Implement `SortJoinTransposeRule`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-876">CALCITE-876</a>]
  After pushing `LogicalProject` past `LogicalWindow`, adjust references to
  constants properly (Hsuan-Yi Chu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-844">CALCITE-844</a>]
  Push `Project` through `Window` (Hsuan-Yi Chu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-841">CALCITE-841</a>]
  Redundant windows when window function arguments are expressions (Hsuan-Yi
  Chu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-846">CALCITE-846</a>]
  Push `Aggregate` with `Filter` through `Union(all)`

#### RelBuilder and Piglet

* [<a href="https://issues.apache.org/jira/browse/CALCITE-933">CALCITE-933</a>]
  `RelBuilder.scan()` now gives a nice exception if the table does not exist
  (Andy Grove)
* Fix Piglet `DUMP` applied to multisets and structs
* Multisets and `COLLECT` in Piglet
* [<a href="https://issues.apache.org/jira/browse/CALCITE-785">CALCITE-785</a>]
  Add "Piglet", a subset of Pig Latin on top of Calcite algebra
* [<a href="https://issues.apache.org/jira/browse/CALCITE-869">CALCITE-869</a>]
  Add `VALUES` command to Piglet
* [<a href="https://issues.apache.org/jira/browse/CALCITE-868">CALCITE-868</a>]
  Add API to execute queries expressed as `RelNode`
* In RelBuilder, build expressions by table alias

#### Bug-fixes, API changes and minor enhancements

* [<a href="https://issues.apache.org/jira/browse/CALCITE-948">CALCITE-948</a>]
  Indicator columns not preserved by `RelFieldTrimmer`
* Fix Windows issues (line endings and checkstyle suppressions)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-937">CALCITE-937</a>]
  User-defined function within view
* [<a href="https://issues.apache.org/jira/browse/CALCITE-926">CALCITE-926</a>]
  Rules fail to match because of missing link to parent equivalence set
  (Maryann Xue)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-908">CALCITE-908</a>]
  Bump protobuf to 3.0.0-beta-1, fix deprecations and update docs (Josh Elser)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-932">CALCITE-932</a>]
  Fix muddled columns when `RelFieldTrimmer` is applied to `Aggregate`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-930">CALCITE-930</a>]
  Now Calcite is a top-level project, remove references to "incubating"
* [<a href="https://issues.apache.org/jira/browse/CALCITE-929">CALCITE-929</a>]
  Calls to `AbstractRelNode` may result in NPE
* [<a href="https://issues.apache.org/jira/browse/CALCITE-923">CALCITE-923</a>]
  Type mismatch when converting `LEFT JOIN` to `INNER`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-666">CALCITE-666</a>]
  Anti-semi-joins against JDBC adapter give wrong results (Yeong Wei)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-918">CALCITE-918</a>]
  `createProject` in `RelOptUtil` should uniquify field names
* [<a href="https://issues.apache.org/jira/browse/CALCITE-792">CALCITE-792</a>]
  Obsolete `RelNode.isKey` and `isDistinct` methods
* Allow FlatLists of different length to be compared
* [<a href="https://issues.apache.org/jira/browse/CALCITE-898">CALCITE-898</a>]
  Type of 'Java<Long> * `INTEGER`' should be `BIGINT`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-894">CALCITE-894</a>]
  Do not generate redundant column alias for the left relation when
  translating `IN` sub-query (Maryann Xue)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-897">CALCITE-897</a>]
  Enable debugging using "-Dcalcite.debug"
* [<a href="https://issues.apache.org/jira/browse/CALCITE-885">CALCITE-885</a>]
  Add Oracle test environment
* [<a href="https://issues.apache.org/jira/browse/CALCITE-888">CALCITE-888</a>]
  Overlay window loses `PARTITION BY` list (Hsuan-Yi Chu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-886">CALCITE-886</a>]
  System functions in `GROUP BY` clause
* [<a href="https://issues.apache.org/jira/browse/CALCITE-860">CALCITE-860</a>]
  Correct LICENSE file for generated web site
* [<a href="https://issues.apache.org/jira/browse/CALCITE-882">CALCITE-882</a>]
  Allow web site to be deployed not as the root directory of the web server
  (Josh Elser)
* Upgrade parent POM to apache-17
* [<a href="https://issues.apache.org/jira/browse/CALCITE-687">CALCITE-687</a>]
  Synchronize HSQLDB at a coarse level using a Lock (Josh Elser)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-870">CALCITE-870</a>]
  Remove copyright content from archers.json
* Replace `Stack` with `ArrayDeque`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-874">CALCITE-874</a>]
  `ReflectiveRelMetadataProvider` is not thread-safe
* Add `LogicalWindow.create()`
* Add `ImmutableBitSet.get(int, int)`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-865">CALCITE-865</a>]
  Unknown table type causes `NullPointerException` in `JdbcSchema`
  * Add table types used by Oracle and DB2
* [<a href="https://issues.apache.org/jira/browse/CALCITE-862">CALCITE-862</a>]
  `JdbcSchema` gives `NullPointerException` on non-standard column type (Marc
  Prud'hommeaux)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-847">CALCITE-847</a>]
  `AVG` window function in `GROUP BY` gives `AssertionError` (Hsuan-Yi Chu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-827">CALCITE-827</a>]
  Calcite incorrectly permutes columns of `OVER` query (Hsuan-Yi Chu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-809">CALCITE-809</a>]
  `TableScan` does not support large/infinite scans (Jesse Yates)
* Lazily create exception only when it needs to be thrown (Marc Prud'hommeaux)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-812">CALCITE-812</a>]
  Make JSON reader and writer use properly quoted key names (Marc
  Prud'hommeaux)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-820">CALCITE-820</a>]
  Validate that window functions have `OVER` clause (Hsuan-Yi Chu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-824">CALCITE-824</a>]
  Type inference when converting `IN` clause to semijoin (Josh Wills)


## <a href="https://github.com/apache/calcite/releases/tag/calcite-1.4.0-incubating">1.4.0-incubating</a> / 2015-09-02
{: #v1-4-0}

In addition to a large number of bug-fixes and minor enhancements,
this release includes improvements to lattices and materialized views,
and adds a builder API so that you can easily create relational
algebra expressions.

#### New features

* [<a href="https://issues.apache.org/jira/browse/CALCITE-748">CALCITE-748</a>]
      Add `RelBuilder`, builder for expressions in relational algebra
* [<a href="https://issues.apache.org/jira/browse/CALCITE-758">CALCITE-758</a>]
      Use more than one lattice in the same query (Rajat Venkatesh)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-761">CALCITE-761</a>]
      Pre-populated materializations (Maryann Xue)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-786">CALCITE-786</a>]
      Detect if materialized view can be used to rewrite a query in
  non-trivial cases (Amogh Margoor)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-732">CALCITE-732</a>]
      Implement multiple distinct-`COUNT` using `GROUPING SETS`
* Add various `BitSet` and `ImmutableBitSet` utilities

#### Web site updates

* [<a href="https://issues.apache.org/jira/browse/CALCITE-810">CALCITE-810</a>]
      Add committers' organizations to the web site
* Add news item (XLDB best lighting talk), and some talks
* Fix javadoc links
* Add license notice for web site
* Wrap file header in HTML comments
* How to release
* Move disclaimer out of every page's footer and into home page and downloads
  page
* For web site files, add license headers where possible, apache-rat
  exclusions otherwise
* Calcite DOAP
* [<a href="https://issues.apache.org/jira/browse/CALCITE-355">CALCITE-355</a>]
      Web site

#### Bug-fixes, API changes and minor enhancements

* [<a href="https://issues.apache.org/jira/browse/CALCITE-741">CALCITE-741</a>]
      Ensure that the source release's `DEPENDENCIES` file includes all module
  dependencies
* [<a href="https://issues.apache.org/jira/browse/CALCITE-743">CALCITE-743</a>]
      Ensure only a single source assembly is executed
* [<a href="https://issues.apache.org/jira/browse/CALCITE-850">CALCITE-850</a>]
      Remove push down expressions from `FilterJoinRule` and create a new rule
  for it
* [<a href="https://issues.apache.org/jira/browse/CALCITE-834">CALCITE-834</a>]
      `StackOverflowError` getting predicates from the metadata provider
* [<a href="https://issues.apache.org/jira/browse/CALCITE-833">CALCITE-833</a>]
      `RelOptUtil.splitJoinCondition` incorrectly splits a join condition
  (Hsuan-Yi Chu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-822">CALCITE-822</a>]
      Add a unit test case to test collation of `LogicalAggregate`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-822">CALCITE-822</a>]
      Revert incorrect `LogicalAggregate` collation inferring logic made in
  [<a href="https://issues.apache.org/jira/browse/CALCITE-783">CALCITE-783</a>]
  (Milinda Pathirage)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-826">CALCITE-826</a>]
      Use `ProjectFactory` in `AggregateJoinTranposeRule` and `FilterJoinRule`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-821">CALCITE-821</a>]
      `Frameworks` gives NPE when `FrameworkConfig` has no default schema
* [<a href="https://issues.apache.org/jira/browse/CALCITE-811">CALCITE-811</a>]
      Extend `JoinProjectTransposeRule` with option to support outer joins
* [<a href="https://issues.apache.org/jira/browse/CALCITE-805">CALCITE-805</a>]
      Add support for using an alternative grammar specification for left and
  right curly braces. Additionally, add support for including addition token
  manager declarations
* [<a href="https://issues.apache.org/jira/browse/CALCITE-803">CALCITE-803</a>]
      Add `MYSQL_ANSI` Lexing policy
* [<a href="https://issues.apache.org/jira/browse/CALCITE-717">CALCITE-717</a>]
      Compare BINARY and VARBINARY on unsigned byte values (Low Chin Wei)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-814">CALCITE-814</a>]
      `RexBuilder` reverses precision and scale of `DECIMAL` literal
* [<a href="https://issues.apache.org/jira/browse/CALCITE-813">CALCITE-813</a>]
      Upgrade `updateCount`, `maxRows` from int to long
* [<a href="https://issues.apache.org/jira/browse/CALCITE-714">CALCITE-714</a>]
      When de-correlating, push join condition into sub-query
* [<a href="https://issues.apache.org/jira/browse/CALCITE-751">CALCITE-751</a>]
      Push aggregate with aggregate functions through join
* Add `RelBuilder.avg`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-806">CALCITE-806</a>]
      `ROW_NUMBER` should emit distinct values
* Document JSON model, making javadoc consistent with the model reference
* [<a href="https://issues.apache.org/jira/browse/CALCITE-808">CALCITE-808</a>]
      Optimize `ProjectMergeRule`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-791">CALCITE-791</a>]
      Optimize `RelOptUtil.pushFilterPastProject`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-783">CALCITE-783</a>]
      Infer collation of `Project` using monotonicity (Milinda Pathirage)
* Change the argument types of `SqlOperator.getMonotonicity` to allow it to be
  used for `RexNode` as well as `SqlNode`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-800">CALCITE-800</a>]
      Window function defined within another window function should be invalid
  (Hsuan-Yi Chu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-787">CALCITE-787</a>]
      Star table wrongly assigned to materialized view (Amogh Margoor)
* Remove duplicate resources from XML test reference files
* [<a href="https://issues.apache.org/jira/browse/CALCITE-795">CALCITE-795</a>]
      Loss of precision when sending a decimal number via the remote JSON
  service (Lukáš Lalinský)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-774">CALCITE-774</a>]
      When `GROUP BY` is present, ensure that window function operands only
  refer to grouping keys (Hsuan-Yi Chu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-799">CALCITE-799</a>]
      Incorrect result for `HAVING count(*) > 1`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-801">CALCITE-801</a>]
      `NullPointerException` using `USING` on table alias with column aliases
* [<a href="https://issues.apache.org/jira/browse/CALCITE-390">CALCITE-390</a>]
      Infer predicates for semi-join
* [<a href="https://issues.apache.org/jira/browse/CALCITE-789">CALCITE-789</a>]
      `MetaImpl.MetaCatalog` should expose `TABLE_CAT` instead of
      `TABLE_CATALOG`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-752">CALCITE-752</a>]
      Add back sqlline as a dependency to csv example
* [<a href="https://issues.apache.org/jira/browse/CALCITE-780">CALCITE-780</a>]
      HTTP error 413 when sending a long string to the Avatica server
* In `RelBuilder`, calling `sort` then `limit` has same effect as calling
  `sortLimit`
* Add `Ord.reverse`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-788">CALCITE-788</a>]
      Allow `EnumerableJoin` to be sub-classed (Li Yang)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-280">CALCITE-280</a>]
      `BigDecimal` underflow (Li Yang)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-763">CALCITE-763</a>]
      Missing translation from `Sort` to `MutableSort` (Maryann Xue)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-770">CALCITE-770</a>]
      Ignore window aggregates and ranking functions when finding aggregate
  functions
* [<a href="https://issues.apache.org/jira/browse/CALCITE-765">CALCITE-765</a>]
      Set `Content-Type` from the RPC server to `application/json` (Lukáš Lalinský)
* Fix Windows line-endings in `RelBuilderTest`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-727">CALCITE-727</a>]
      Constant folding involving `CASE` and `NULL`
* Related to
  [<a href="https://issues.apache.org/jira/browse/CALCITE-758">CALCITE-758</a>],
  speed up matching by not considering tiles separately from other
  materialized views
* Test case and workaround for
  [<a href="https://issues.apache.org/jira/browse/CALCITE-760">CALCITE-760</a>]
      `Aggregate` recommender blows up if row count estimate is too high
* [<a href="https://issues.apache.org/jira/browse/CALCITE-753">CALCITE-753</a>]
      `Aggregate` operators may derive row types with duplicate column names
* [<a href="https://issues.apache.org/jira/browse/CALCITE-457">CALCITE-457</a>]
      Push condition of non-ansi join into join operator
* Change jsonRequest encoding to UTF-8 (Guitao Ding)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-757">CALCITE-757</a>]
      Fix expansion of view of another view (Venki Korukanti)
* Fix coverity warnings
* Remove deprecated `SqlTypeName` methods
* [<a href="https://issues.apache.org/jira/browse/CALCITE-754">CALCITE-754</a>]
      Validator error when resolving `OVER` clause of `JOIN` query
* [<a href="https://issues.apache.org/jira/browse/CALCITE-429">CALCITE-429</a>]
      Cardinality provider for use by lattice algorithm
* [<a href="https://issues.apache.org/jira/browse/CALCITE-740">CALCITE-740</a>]
      Redundant `WHERE` clause causes wrong result in MongoDB adapter
* [<a href="https://issues.apache.org/jira/browse/CALCITE-665">CALCITE-665</a>]
      `ClassCastException` in MongoDB adapter
* Separate `TableFactory` from suggested table name, so one `TableFactory` can be
  used for several tables
* [<a href="https://issues.apache.org/jira/browse/CALCITE-749">CALCITE-749</a>]
      Add `MaterializationService.TableFactory` (Rajat Venkatesh)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-718">CALCITE-718</a>]
      Enable fetch to work for `Statement.execute()` for Avatica (Xavier Leong)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-712">CALCITE-712</a>]
      Obey `setMaxRows` for statement execute (Xavier Leong)
* Add `LoggingLocalJsonService`, to make it easier to test that JDBC requests
  cause the right RPCs
* [<a href="https://issues.apache.org/jira/browse/CALCITE-708">CALCITE-708</a>]
      Implement `DatabaseMetaData.getTypeInfo` (Xavier Leong)
* Enable Travis CI on new-master branch and bug-fix branches named
  "NNN-description"
* Clean up
* Upgrade tpcds
* Make `JdbcTest.testVersion` more permissive, so that `version.major` and
  `version.minor` can be set just before a release, rather than just after as at
  present

## <a href="https://github.com/apache/calcite/releases/tag/calcite-1.3.0-incubating">1.3.0-incubating</a> / 2015-05-30
{: #v1-3-0}

Mainly bug-fixes, but this release adds support for
<a href="https://issues.apache.org/jira/browse/CALCITE-505">modifiable views</a>
and
<a href="https://issues.apache.org/jira/browse/CALCITE-704">filtered aggregate functions</a>
and various improvements to Avatica.

#### New features

* [<a href="https://issues.apache.org/jira/browse/CALCITE-505">CALCITE-505</a>]
  Support modifiable view
* [<a href="https://issues.apache.org/jira/browse/CALCITE-704">CALCITE-704</a>]
  `FILTER` clause for aggregate functions
* [<a href="https://issues.apache.org/jira/browse/CALCITE-522">CALCITE-522</a>]
  In remote JDBC driver, transmit static database properties as a map
* [<a href="https://issues.apache.org/jira/browse/CALCITE-661">CALCITE-661</a>]
  Remote fetch in Calcite JDBC driver
* Support Date, Time, Timestamp parameters

#### API changes

* [<a href="https://issues.apache.org/jira/browse/CALCITE-722">CALCITE-722</a>]
  Rename markdown files to lower-case
* [<a href="https://issues.apache.org/jira/browse/CALCITE-697">CALCITE-697</a>]
  Obsolete class `RelOptQuery`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-693">CALCITE-693</a>]
  Allow clients to control creation of `RelOptCluster`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-691">CALCITE-691</a>]
  Allow projects to supply alternate SQL parser
* [<a href="https://issues.apache.org/jira/browse/CALCITE-675">CALCITE-675</a>]
  Enable `AggregateProjectMergeRule` in standard rule set
* [<a href="https://issues.apache.org/jira/browse/CALCITE-679">CALCITE-679</a>]
  Factory method for `SemiJoin`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-674">CALCITE-674</a>]
  Add a `SWAP_OUTER` static instance to `JoinCommuteRule` (Maryann Xue)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-735">CALCITE-735</a>]
  `Primitive.DOUBLE.min` should be large and negative

#### Bug-fixes and internal changes

* [<a href="https://issues.apache.org/jira/browse/CALCITE-688">CALCITE-688</a>]
  `splitCondition` does not behave correctly when one side of the condition
  references columns from different inputs
* [<a href="https://issues.apache.org/jira/browse/CALCITE-259">CALCITE-259</a>]
  Using sub-queries in `CASE` statement against JDBC tables generates invalid
  Oracle SQL (Yeong Wei)
* In sample code in README.md, rename optiq to calcite (Ethan)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-720">CALCITE-720</a>]
  `VolcanoPlanner.ambitious` comment doc is inconsistent (Santiago M. Mola)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-729">CALCITE-729</a>]
  `IndexOutOfBoundsException` in `ROLLUP` query on JDBC data source
* [<a href="https://issues.apache.org/jira/browse/CALCITE-733">CALCITE-733</a>]
  Multiple distinct-`COUNT` query gives wrong results
* [<a href="https://issues.apache.org/jira/browse/CALCITE-730">CALCITE-730</a>]
  `ClassCastException` in table from `CloneSchema`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-728">CALCITE-728</a>]
  Test suite hangs on Windows
* [<a href="https://issues.apache.org/jira/browse/CALCITE-723">CALCITE-723</a>]
  Document lattices
* [<a href="https://issues.apache.org/jira/browse/CALCITE-515">CALCITE-515</a>]
  Add Apache headers to markdown files
* Upgrade quidem
* [<a href="https://issues.apache.org/jira/browse/CALCITE-716">CALCITE-716</a>]
  Scalar sub-query and aggregate function in `SELECT` or `HAVING` clause gives
  `AssertionError`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-694">CALCITE-694</a>]
  Scan `HAVING` clause for sub-queries and `IN`-lists (Hsuan-Yi Chu)
* Upgrade hydromatic-resource-maven-plugin
* [<a href="https://issues.apache.org/jira/browse/CALCITE-710">CALCITE-710</a>]
  Identical conditions in the `WHERE` clause cause `AssertionError` (Sean
  Hsuan-Yi Chu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-695">CALCITE-695</a>]
  Do not add `SINGLE_VALUE` aggregate function to a sub-query that will never
  return more than one row (Hsuan-Yi Chu)
* Add tests for scalar sub-queries, including test cases for
  [<a href="https://issues.apache.org/jira/browse/CALCITE-709">CALCITE-709</a>]
  Errors with `LIMIT` inside scalar sub-query
* [<a href="https://issues.apache.org/jira/browse/CALCITE-702">CALCITE-702</a>]
  Add validator test for monotonic expressions
* [<a href="https://issues.apache.org/jira/browse/CALCITE-699">CALCITE-699</a>]
  In Avatica, synchronize access to Calendar
* [<a href="https://issues.apache.org/jira/browse/CALCITE-700">CALCITE-700</a>]
  Pass time zone into tests
* [<a href="https://issues.apache.org/jira/browse/CALCITE-698">CALCITE-698</a>]
  For `GROUP BY ()`, `areColumnsUnique()` should return true for any key
* Disable tests that fail under JDK 1.7 due to
  [<a href="https://issues.apache.org/jira/browse/CALCITE-687">CALCITE-687</a>]
* Add "getting started" to HOWTO
* [<a href="https://issues.apache.org/jira/browse/CALCITE-692">CALCITE-692</a>]
  Add back sqlline as a dependency
* [<a href="https://issues.apache.org/jira/browse/CALCITE-677">CALCITE-677</a>]
  `RemoteDriverTest.testTypeHandling` fails east of Greenwich
* Disable test for
  [<a href="https://issues.apache.org/jira/browse/CALCITE-687">CALCITE-687</a>]
  Make `RemoteDriverTest.testStatementLifecycle` thread-safe
* [<a href="https://issues.apache.org/jira/browse/CALCITE-686">CALCITE-686</a>]
  `SqlNode.unparse` produces invalid SQL
* [<a href="https://issues.apache.org/jira/browse/CALCITE-507">CALCITE-507</a>]
  Update HOWTO.md with running integration tests
* Add H2 integration test
* Add PostgreSQL integration test
* [<a href="https://issues.apache.org/jira/browse/CALCITE-590">CALCITE-590</a>]
  Update MongoDB test suite to calcite-test-dataset
* Add `CalciteAssert.assertArrayEqual` for more user-friendly asserts
* [<a href="https://issues.apache.org/jira/browse/CALCITE-585">CALCITE-585</a>]
  Avatica JDBC methods should throw `SQLFeatureNotSupportedException` (Ng Jiunn
  Jye)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-671">CALCITE-671</a>]
  `ByteString` does not deserialize properly as a `FetchRequest` parameter value
* [<a href="https://issues.apache.org/jira/browse/CALCITE-676">CALCITE-676</a>]
  `AssertionError` in `GROUPING SETS` query
* [<a href="https://issues.apache.org/jira/browse/CALCITE-678">CALCITE-678</a>]
  `SemiJoinRule` mixes up fields when `Aggregate.groupSet` is not field #0

## <a href="https://github.com/apache/calcite/releases/tag/calcite-1.2.0-incubating">1.2.0-incubating</a> / 2015-04-07
{: #v1-2-0}

A short release, less than a month after 1.1.

There have been many changes to Avatica, hugely improving its coverage of the
JDBC API and overall robustness. A new provider, `JdbcMeta`, allows
you to remote an existing JDBC driver.

[<a href="https://issues.apache.org/jira/browse/CALCITE-606">CALCITE-606</a>]
improves how the planner propagates traits such as collation and
distribution among relational expressions.

[<a href="https://issues.apache.org/jira/browse/CALCITE-613">CALCITE-613</a>]
and [<a href="https://issues.apache.org/jira/browse/CALCITE-307">CALCITE-307</a>]
improve implicit and explicit conversions in SQL.

#### New features

* [<a href="https://issues.apache.org/jira/browse/CALCITE-366">CALCITE-366</a>]
  Support Aggregate push down in bushy joins (Jesus Camacho Rodriguez)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-613">CALCITE-613</a>]
  Implicitly convert character values in comparisons
* [<a href="https://issues.apache.org/jira/browse/CALCITE-307">CALCITE-307</a>]
  Implement `CAST` between date-time types
* [<a href="https://issues.apache.org/jira/browse/CALCITE-634">CALCITE-634</a>]
  Allow `ORDER BY` aggregate function in `SELECT DISTINCT`, provided that it
  occurs in `SELECT` clause (Sean Hsuan-Yi Chu)
* In linq4j, implement `firstOrDefault`, `single`, and `singleOrDefault` methods
  (Daniel Cooper)
* JDBC adapter
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-631">CALCITE-631</a>]
    Push theta joins down to JDBC adapter (Ng Jiunn Jye)
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-657">CALCITE-657</a>]
    `NullPointerException` when executing `JdbcAggregate.implement`
    method (Yuri Au Yong)
* Metadata
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-659">CALCITE-659</a>]
    Missing types in `averageTypeValueSize` method in `RelMdSize`
    (Jesus Camacho Rodriguez)
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-650">CALCITE-650</a>]
    Add metadata for average size of a tuple in `SemiJoin` (Jesus
    Camacho Rodriguez)
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-649">CALCITE-649</a>]
    Extend `splitCondition` method in `RelOptUtil` to handle multiple
    joins on the same key (Jesus Camacho Rodriguez)

#### Avatica features and bug-fixes

* [<a href="https://issues.apache.org/jira/browse/CALCITE-670">CALCITE-670</a>]
  `AvaticaPreparedStatement` should support `execute()` and
  `executeUpdate()` (Nick Dimiduk)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-641">CALCITE-641</a>]
  Implement logging throughout Avatica server (Nick Dimiduk)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-646">CALCITE-646</a>]
  `AvaticaStatement.execute` method broken over remote JDBC (Yeong Wei
  and Julian Hyde)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-660">CALCITE-660</a>]
  Improve Avatica date support
* [<a href="https://issues.apache.org/jira/browse/CALCITE-655">CALCITE-655</a>]
  Implement `ConnectionSync` RPC (Nick Dimiduk)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-654">CALCITE-654</a>]
  Tighten up `AvaticaStatement.execute` semantics (Nick Dimiduk)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-658">CALCITE-658</a>]
  Cleanup dependency usage (Nick Dimiduk)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-652">CALCITE-652</a>]
  Move server pieces of `avatica` into `avatica-server` (Nick Dimiduk)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-651">CALCITE-651</a>]
  In `JdbcMeta`, convert property definitions to an enum (Nick Dimiduk)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-640">CALCITE-640</a>]
  Avatica server should expire stale connections/statements (Nick Dimiduk)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-639">CALCITE-639</a>]
  Open up permissions on avatica server components (Nick Dimiduk)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-637">CALCITE-637</a>]
  Implement Avatica `CloseConnection` RPC (Nick Dimiduk)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-636">CALCITE-636</a>]
  Connection isolation for Avatica clients (Nick Dimiduk)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-626">CALCITE-626</a>]
  Implement `CloseStatement` RPC (Nick Dimiduk)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-630">CALCITE-630</a>]
  Flesh out `AvaticaParameter.setObject` (Nick Dimiduk)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-627">CALCITE-627</a>]
  Add Avatica support for `getTableTypes`, `getColumns` (Xavier FH Leong)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-618">CALCITE-618</a>]
  Add Avatica support for `getTables` (Julian Hyde and Nick Dimiduk)

#### API changes

* [<a href="https://issues.apache.org/jira/browse/CALCITE-617">CALCITE-617</a>]
  Check at initialization time in `CachingInvocationHandler` that MD provider
  is not null (Jesus Camacho Rodriguez)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-638">CALCITE-638</a>]
  SQL standard `REAL` is 4 bytes, `FLOAT` is 8 bytes

#### Bug-fixes and internal changes

* [<a href="https://issues.apache.org/jira/browse/CALCITE-672">CALCITE-672</a>]
  SQL `ANY` type should be nullable (Jinfeng Ni)
* Disable tests, pending
  [<a href="https://issues.apache.org/jira/browse/CALCITE-673">CALCITE-673</a>]
  Timeout executing joins against MySQL
* Fix traits in MongoDB adapter, and `NullPointerException` in `JdbcTest`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-662">CALCITE-662</a>]
  Query validation fails when an `ORDER BY` clause is used with `WITH CLAUSE`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-606">CALCITE-606</a>]
  Fix trait propagation and add test case
* Remove checkstyle Eclipse properties from git tracking
* [<a href="https://issues.apache.org/jira/browse/CALCITE-644">CALCITE-644</a>]
  Increase check style line limit to 100 chars (Nick Dimiduk)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-648">CALCITE-648</a>]
  Update `ProjectMergeRule` description for new naming convention (Jinfeng Ni)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-625">CALCITE-625</a>]
  `README.md` linking to the wrong page of `optiq-csv` (hongbin ma)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-632">CALCITE-632</a>]
  Sort order returned by `SUPERCLASS_COMPARATOR` in
  `ReflectiveRelMetadataProvider` is inconsistent (Jesus Camacho
  Rodriguez)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-335">CALCITE-335</a>]
  Remove uses of linq4j `Functions.adapt`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-592">CALCITE-592</a>]
  Upgrade to Guava 14.0.1
* [<a href="https://issues.apache.org/jira/browse/CALCITE-596">CALCITE-596</a>]
  JDBC adapter incorrectly reads null values as 0 (Ng Jiunn Jye)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-633">CALCITE-633</a>]
  `WITH ... ORDER BY` cannot find table
* [<a href="https://issues.apache.org/jira/browse/CALCITE-614">CALCITE-614</a>]
  `IN` clause in `CASE` in `GROUP BY` gives `AssertionError`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-619">CALCITE-619</a>]
  Slim down dependencies in parent POM

## <a href="https://github.com/apache/calcite/releases/tag/calcite-1.1.0-incubating">1.1.0-incubating</a> / 2015-03-13
{: #v1-1-0}

This Calcite release makes it possible to exploit physical properties
of relational expressions to produce more efficient plans, introducing
collation and distribution as traits, `Exchange` relational operator,
and several new forms of metadata.

We add experimental support for streaming SQL.

This release drops support for JDK 1.6; Calcite now requires 1.7 or
later.

We have introduced static `create` methods for many sub-classes of
`RelNode`. We strongly suggest that you use these rather than
calling constructors directly.

#### New features

* SQL
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-602">CALCITE-602</a>]
    Streaming queries (experimental)
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-588">CALCITE-588</a>]
    Allow `TableMacro` to consume maps and collections
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-583">CALCITE-583</a>]
    Operator `||` mishandles `ANY` type (Sean Hsuan-Yi Chu)
* Planner rule improvements
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-445">CALCITE-445</a>]
    Pull up filters rejected by a `ProjectableFilterableTable`
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-600">CALCITE-600</a>]
    Use `SetOpFactory` in rules containing `Union` operator (Jesus
    Camacho Rodriguez)
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-603">CALCITE-603</a>]
    Metadata providers for size, memory, parallelism
    * [<a href="https://issues.apache.org/jira/browse/CALCITE-607">CALCITE-607</a>]
      Change visibility of constructor in metadata providers for size,
      memory, parallelism (Jesus Camacho Rodriguez)
    * [<a href="https://issues.apache.org/jira/browse/CALCITE-608">CALCITE-608</a>]
      Exception is thrown when `RelMdDistribution` for `Project`
      operator is called (Jesus Camacho Rodriguez)
* Collation and distribution as traits
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-88">CALCITE-88</a>]
    Add collation as a trait and a kind of `RelNode` metadata
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-569">CALCITE-569</a>]
    `ArrayIndexOutOfBoundsException` when deducing collation (Aman Sinha)
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-581">CALCITE-581</a>]
    Add `LogicalSort` relational expression, and make `Sort` abstract
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-526">CALCITE-526</a>]
    Add `EnumerableMergeJoin`, which exploits sorted inputs
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-71">CALCITE-71</a>]
    Provide a way to declare that tables are sorted
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-576">CALCITE-576</a>]
    Make `RelCollation` trait and `AbstractRelNode.getCollationList` consistent
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-254">CALCITE-254</a>]
    Propagate `RelCollation` on aliased columns in `JoinRule`
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-569">CALCITE-569</a>]
    `ArrayIndexOutOfBoundsException` when deducing collation
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-594">CALCITE-594</a>]
    Add `RelDistribution` trait and `Exchange` relational expression

#### API changes

* Many sub-classes of `RelNode` now have a static `create` method
  which automatically sets up traits such as collation and
  distribution. The constructors are not marked deprecated, but we
  strongly suggest that you use the `create` method if it exists.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-591">CALCITE-591</a>]
  Drop support for Java 1.6 (and JDBC 4.0)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-587">CALCITE-587</a>]
  Upgrade `jetty-server` to 9.2.7.v20150116 and port avatica-server `HttpServer`
  (Trevor Hartman)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-577">CALCITE-577</a>]
  Revert temporary API changes introduced in
  [<a href="https://issues.apache.org/jira/browse/CALCITE-575">CALCITE-575</a>]
* Add means to create `Context` instances by wrapping objects and by chaining
  contexts
* [<a href="https://issues.apache.org/jira/browse/CALCITE-599">CALCITE-599</a>]
  `EquiJoin` in wrong package (Jesus Camacho Rodriguez)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-573">CALCITE-573</a>]
  Use user-given names in `RelOptUtil.createProject` and `createRename`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-572">CALCITE-572</a>]
  Remove `Project.flags` (methods are deprecated, to be removed before 2.0)

#### Bug-fixes and internal changes

* Remove the `LICENSE` file of calcite-example-csv (the former
  optiq-csv) and move its history into main history
* [<a href="https://issues.apache.org/jira/browse/CALCITE-615">CALCITE-615</a>]
  AvaticaParameter should be Jackson serializable (Nick Dimiduk)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-612">CALCITE-612</a>]
  Update AvaticaStatement to handle cancelled queries (Parth Chandra)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-605">CALCITE-605</a>]
  Reduce dependency on third-party maven repositories
* [<a href="https://issues.apache.org/jira/browse/CALCITE-611">CALCITE-611</a>]
  Method `setAggChildKeys` should take into account indicator columns of
  `Aggregate` operator (Jesus Camacho Rodriguez)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-566">CALCITE-566</a>]
  `ReduceExpressionsRule` requires planner to have an `Executor`
* Refactor `TableScanNode.create` method
* [<a href="https://issues.apache.org/jira/browse/CALCITE-593">CALCITE-593</a>]
  Validator in `Frameworks` should expand identifiers (Jinfeng Ni)
* Australian time-zones changed in `tzdata2014f`, Java 1.8.0_31
* [<a href="https://issues.apache.org/jira/browse/CALCITE-580">CALCITE-580</a>]
  Average aggregation on an `Integer` column throws `ClassCastException`
* In Travis, ask Surefire to print results to screen
* [<a href="https://issues.apache.org/jira/browse/CALCITE-586">CALCITE-586</a>]
  Prevent JSON serialization of `Signature.internalParameters`

## <a href="https://github.com/apache/calcite/releases/tag/calcite-1.0.0-incubating">1.0.0-incubating</a> / 2015-01-31
{: #v1-0-0}

Calcite's first major release.

Since the previous release we have re-organized the into the `org.apache.calcite`
namespace. To make migration of your code easier, we have described the
<a href="https://issues.apache.org/jira/secure/attachment/12681620/mapping.txt">mapping from old to new class names</a>
as an attachment to
[<a href="https://issues.apache.org/jira/browse/CALCITE-296">CALCITE-296</a>].

The release adds SQL support for `GROUPING SETS`, `EXTEND`, `UPSERT` and sequences;
a remote JDBC driver;
improvements to the planner engine and built-in planner rules;
improvements to the algorithms that implement the relational algebra,
including an interpreter that can evaluate queries without compilation;
and fixes about 30 bugs.

#### New features

* SQL
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-494">CALCITE-494</a>]
    Support `NEXT`/`CURRENT VALUE FOR` syntax for using sequences
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-492">CALCITE-492</a>]
    Support `UPSERT` statement in parser
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-493">CALCITE-493</a>]
    Add `EXTEND` clause, for defining columns and their types at query/DML time
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-497">CALCITE-497</a>]
    Support optional qualifier for column name references
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-356">CALCITE-356</a>]
    Allow column references of the form `schema.table.column`
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-462">CALCITE-462</a>]
    Allow table functions in `LATERAL` expression
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-282">CALCITE-282</a>]
    Add `{fn QUARTER(date)}` function (Benoy Antony)
  * Grouping sets
    * [<a href="https://issues.apache.org/jira/browse/CALCITE-370">CALCITE-370</a>]
      Support `GROUPING SETS`, `CUBE`, `ROLLUP` in SQL and algebra
    * [<a href="https://issues.apache.org/jira/browse/CALCITE-512">CALCITE-512</a>]
      Add `GROUP_ID`,`GROUPING_ID`, `GROUPING` functions
* Planner rule improvements
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-92">CALCITE-92</a>]
    Optimize away `Project` that merely renames fields
  * Detect and merge duplicate predicates `AND(x, y, x)` to `AND(x, y)` in more
    circumstances
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-557">CALCITE-557</a>]
    Speed up planning by never creating `AbstractConverter`
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-545">CALCITE-545</a>]
    When a projected expression can only have one value, replace with that
    constant
  * Grouping sets
    * [<a href="https://issues.apache.org/jira/browse/CALCITE-542">CALCITE-542</a>]
      Support for `Aggregate` with grouping sets in `RelMdColumnOrigins` (Jesus
      Camacho Rodriguez)
    * [<a href="https://issues.apache.org/jira/browse/CALCITE-533">CALCITE-533</a>]
      Support for grouping sets in `FilterAggregateTransposeRule` (Jesus Camacho
      Rodriguez)
    * [<a href="https://issues.apache.org/jira/browse/CALCITE-532">CALCITE-532</a>]
      Support for grouping sets in `AggregateFilterTransposeRule` (Jesus Camacho
      Rodriguez)
    * [<a href="https://issues.apache.org/jira/browse/CALCITE-513">CALCITE-513</a>]
      Support for grouping sets in `AggregateProjectMergeRule` (Jesus Camacho
      Rodriguez)
    * [<a href="https://issues.apache.org/jira/browse/CALCITE-510">CALCITE-510</a>]
      Support for grouping sets in `AggregateExpandDistinctAggregatesRule` (Jesus
      Camacho Rodriguez)
    * [<a href="https://issues.apache.org/jira/browse/CALCITE-502">CALCITE-502</a>]
      Support for grouping sets in `AggregateUnionTransposeRule` (Jesus Camacho
      Rodriguez)
    * [<a href="https://issues.apache.org/jira/browse/CALCITE-503">CALCITE-503</a>]
      Tests to check rules on `Aggregate` operator without grouping sets (Jesus
      Camacho Rodriguez)
* Algorithms
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-451">CALCITE-451</a>]
    Implement theta join, inner and outer, in enumerable convention
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-489">CALCITE-489</a>]
    Update `Correlate` mechanics and implement `EnumerableCorrelate` (aka nested
    loops join)
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-544">CALCITE-544</a>]
    Implement `Union` in interpreter
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-562">CALCITE-562</a>]
    Implement inner `Join` in interpreter and improve handling of scalar expressions
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-543">CALCITE-543</a>]
    Implement `Aggregate` (including `GROUPING SETS`) in interpreter (Jacques
    Nadeau)
  * In progress towards
    [<a href="https://issues.apache.org/jira/browse/CALCITE-558">CALCITE-558</a>]
    add `BINDABLE` convention (but `ENUMERABLE` is still the default), and add
    `ArrayBindable` and `Scalar` interfaces
* Remote driver
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-93">CALCITE-93</a>]
    Calcite RPC server
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-94">CALCITE-94</a>]
    Remote JDBC driver
  * Make `JsonHandler` and `JsonService` thread-safe

#### API changes

* The great code re-org
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-296">CALCITE-296</a>]
    Re-organize package structure
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-419">CALCITE-419</a>]
    Naming convention for planner rules
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-306">CALCITE-306</a>]
    Standardize code style for "import package.*;"
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-474">CALCITE-474</a>]
    Clean up rule naming in order to support enabling/disabling rules
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-460">CALCITE-460</a>]
    Add `ImmutableBitSet` and replace uses of `BitSet`
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-479">CALCITE-479</a>]
    Migrate `RelNode.getChildExps` to `RelNode.accept(RexShuttle)`
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-527">CALCITE-527</a>]
    Drop `rowType` field and constructor/copy argument of `Calc`
* Add linq4j and example-csv modules
  * Remove unused packages in linq4j, and fix checkstyle issues in linq4j and csv
  * Add calcite-linq4j and calcite-example-csv as POM sub-modules
  * Import 'optiq-csv' project as 'example/csv/', and add Apache headers
  * Import 'linq4j' project, and add Apache headers
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-478">CALCITE-478</a>]
    Move CSV tutorial (Siva Narayanan)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-464">CALCITE-464</a>]
  Make parser accept configurable max length for SQL identifier
* [<a href="https://issues.apache.org/jira/browse/CALCITE-465">CALCITE-465</a>]
  Remove `OneRow` and `Empty` relational expressions; `Values` will suffice

#### Bug-fixes and internal changes

* Build improvements
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-541">CALCITE-541</a>]
    Update maven-source-plugin to 2.4 to get speedup in jdk 1.8
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-537">CALCITE-537</a>]
    Skip overwrite of `NOTICE`, `DEPENDENCIES`, and `LICENSE` files
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-538">CALCITE-538</a>]
    Generate `Parser.jj` only at first build
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-539">CALCITE-539</a>]
    Avoid rewrite of `org-apache-calcite-jdbc.properties`
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-540">CALCITE-540</a>]
    Create git.properties file only at first build. This saves time in
    development at a cost of stale `git.properties`
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-536">CALCITE-536</a>]
    Add `@PackageMarker` to `package-info.java` so maven-compiler skips
    compilation when the sources are unchanged
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-535">CALCITE-535</a>]
    Support skip overwrite in hydromatic-resource
* [<a href="https://issues.apache.org/jira/browse/CALCITE-582">CALCITE-582</a>]
  `EnumerableTableScan` broken when table has single column
* [<a href="https://issues.apache.org/jira/browse/CALCITE-575">CALCITE-575</a>]
  Variant of `ProjectRemoveRule` that considers a project trivial only if its
  field names are identical (John Pullokkaran)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-571">CALCITE-571</a>]
  `ReduceExpressionsRule` tries to reduce `SemiJoin` condition to non-equi
  condition
* [<a href="https://issues.apache.org/jira/browse/CALCITE-568">CALCITE-568</a>]
  Upgrade to a version of `pentaho-aggdesigner` that does not pull in
  `servlet-api`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-567">CALCITE-567</a>]
  Make `quidem` dependency have scope "test"
* [<a href="https://issues.apache.org/jira/browse/CALCITE-570">CALCITE-570</a>]
  `ReduceExpressionsRule` throws "duplicate key" exception
* [<a href="https://issues.apache.org/jira/browse/CALCITE-561">CALCITE-561</a>]
  Upgrade parent POM
* [<a href="https://issues.apache.org/jira/browse/CALCITE-458">CALCITE-458</a>]
  ArrayIndexOutOfBoundsException when using just a single column in interpreter
* Fix spurious extra row from `FULL JOIN`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-554">CALCITE-554</a>]
  Outer join over NULL keys generates wrong result
* [<a href="https://issues.apache.org/jira/browse/CALCITE-489">CALCITE-489</a>]
  Teach `CalciteAssert` to respect multiple settings
* [<a href="https://issues.apache.org/jira/browse/CALCITE-516">CALCITE-516</a>]
  `GROUP BY` on a `CASE` expression containing `IN` predicate fails (Aman Sinha)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-552">CALCITE-552</a>]
  Upgrade tpcds (which depends on an old version of guava)
* Copy identifier when fully-qualifying, so column aliases have the right case
* [<a href="https://issues.apache.org/jira/browse/CALCITE-548">CALCITE-548</a>]
  Extend `induce` method to return `CUBE` and `ROLLUP` (Jesus Camacho Rodriguez)
  * Simplify `Group.induce` by assuming that group sets are sorted
* Test case for
  [<a  href="https://issues.apache.org/jira/browse/CALCITE-212">CALCITE-212</a>]
  Join condition with `OR`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-550">CALCITE-550</a>]
  Case-insensitive matching of sub-query columns fails
  * Add more unit tests (Jinfeng Ni)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-448">CALCITE-448</a>]
  `FilterIntoJoinRule` creates filters containing invalid `RexInputRef`
* When registering a `RelNode`, be tolerant if it is equivalent to a `RelNode`
  with different traits
* [<a href="https://issues.apache.org/jira/browse/CALCITE-547">CALCITE-547</a>]
  Set nullability while inferring return type of `item(any,...)` operator
* In Travis CI, enable containers, and cache `.m2` directory
* [<a href="https://issues.apache.org/jira/browse/CALCITE-534">CALCITE-534</a>]
  Missing implementation of `ResultSetMetaData.getColumnClassName` (Knut
  Forkalsrud)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-506">CALCITE-506</a>]
  Update `EnumerableRelImplementor.stash` so it is suitable for all kinds of
  classes
* Merge join algorithm for `Enumerable`s
* Efficient `Enumerable` over random-access list
* Add a test that calls all functions with arguments of all types that they
  claim to accept
* [<a href="https://issues.apache.org/jira/browse/CALCITE-511">CALCITE-511</a>]
  `copy` method in `LogicalAggregate` not copying the indicator value properly
* Add a model that has lattices and works against HSQLDB
* [<a href="https://issues.apache.org/jira/browse/CALCITE-509">CALCITE-509</a>]
  `RelMdColumnUniqueness` uses `ImmutableBitSet.Builder` twice, gets
  `NullPointerException`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-488">CALCITE-488</a>]
  `Enumerable<Holder>` does not work if where `Holder` is a custom class
  with a single field; Calcite tries to treat it as `SCALAR` due to premature
  `JavaRowFormat.optimize`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-352">CALCITE-352</a>]
  Throw exception if `ResultSet.next()` is called after `close()`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-403">CALCITE-403</a>]
  `Enumerable` gives `NullPointerException` with `NOT` on nullable expression
* [<a href="https://issues.apache.org/jira/browse/CALCITE-469">CALCITE-469</a>]
  Update example/csv README.md instructions
* Document `WITH`, `LATERAL`, `GROUPING SETS`, `CUBE`, `ROLLUP`;
  add descriptions for all built-in functions and operators
* [<a href="https://issues.apache.org/jira/browse/CALCITE-470">CALCITE-470</a>]
  Print warning when column type hint is not understood;
  Update `EMPS.deptno` column Integer &rarr; int
* Fix `Linq4j.product`; the cartesian product of 0 attributes is one row of 0
  attributes
* Update link optiq-mat-plugin &rarr; mat-calcite-plugin
* [<a href="https://issues.apache.org/jira/browse/CALCITE-467">CALCITE-467</a>]
  Incorrect namespace in `package-info.java`
* Add headers, to appease the RAT
* [<a href="https://issues.apache.org/jira/browse/CALCITE-446">CALCITE-446</a>]
  CSV adapter should read from directory relative to the model file
* Add examples of scannable and filterable tables, matching
  [<a href="https://issues.apache.org/jira/browse/CALCITE-436">CALCITE-436</a>]
  Simpler SPI to query Table
* Fix `JdbcTest.testVersion` now that version is 1.0
* Update release HOWTO

## <a href="https://github.com/apache/calcite/releases/tag/calcite-0.9.2-incubating">0.9.2-incubating</a> / 2014-11-05
{: #v0-9-2}

A fairly minor release, and last release before we rename all of the
packages and lots of classes, in what we expect to call 1.0. If you
have an existing application, it's worth upgrading to this first,
before you move on to 1.0.

#### New features

* [<a href="https://issues.apache.org/jira/browse/CALCITE-436">CALCITE-436</a>]
  Simpler SPI to query `Table`

#### API changes

* [<a href="https://issues.apache.org/jira/browse/CALCITE-447">CALCITE-447</a>]
  Change semi-join rules to make use of factories
* [<a href="https://issues.apache.org/jira/browse/CALCITE-442">CALCITE-442</a>
  Add `RelOptRuleOperand` constructor that takes a predicate

#### Bug-fixes and internal changes

* [<a href="https://issues.apache.org/jira/browse/CALCITE-397">CALCITE-397</a>]
  `SELECT DISTINCT *` on reflective schema gives `ClassCastException` at runtime
* Various lattice improvements.
* sqlline: Looking for class-path in inconsistent locations.
* Re-order test suite, so that fast tests are run first.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-444">CALCITE-444</a>]
  Filters wrongly pushed into full outer join
* Make it more convenient to unit test `RelMetadataQuery`, and add some more
  tests for
  [<a href="https://issues.apache.org/jira/browse/CALCITE-443">CALCITE-443</a>]
* [<a href="https://issues.apache.org/jira/browse/CALCITE-443">CALCITE-443</a>]
  `getPredicates` from a Union is not correct
* Update references to web sites, git repositories, jira, mailing lists,
  travis CI now that [INFRA-8413] is fixed
* [<a href="https://issues.apache.org/jira/browse/CALCITE-434">CALCITE-435</a>]
  `FilterAggregateTransposeRule` loses conditions that cannot be pushed
* [<a href="https://issues.apache.org/jira/browse/CALCITE-435">CALCITE-435</a>]
  `LoptOptimizeJoinRule` incorrectly re-orders outer joins
* [<a href="https://issues.apache.org/jira/browse/CALCITE-439">CALCITE-439</a>]
  `SqlValidatorUtil.uniquify()` may not terminate under some conditions
* [<a href="https://issues.apache.org/jira/browse/CALCITE-438">CALCITE-438</a>]
  Push predicates through `SemiJoinRel`
* Add test case for `LIKE ... ESCAPE`.
* HOWTO: Modify release instructions.
* Update `DiffRepository` documentation.
* Add tests for windowed aggregates without `ORDER BY`. (Works already.)

## <a href="https://github.com/apache/calcite/releases/tag/calcite-0.9.1-incubating">0.9.1-incubating</a> / 2014-10-02
{: #v0-9-1}

This is the first release as Calcite. (The project was previously called Optiq.)

#### New features

* [<a href="https://issues.apache.org/jira/browse/CALCITE-430">CALCITE-430</a>]
  Rename project from Optiq to Calcite
* [<a href="https://issues.apache.org/jira/browse/CALCITE-426">CALCITE-426</a>]
  Pool JDBC data sources, to make it easier to pool connections
* [<a href="https://issues.apache.org/jira/browse/CALCITE-416">CALCITE-416</a>]
  Execute logical `RelNode`s using an interpreter
* [<a href="https://issues.apache.org/jira/browse/CALCITE-376">CALCITE-376</a>]
  Move `SqlRun` into its own artifact,
  <a href="https://github.com/julianhyde/quidem">Quidem</a>.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-269">CALCITE-269</a>]
  MongoDB result sets larger than 16MB
* [<a href="https://issues.apache.org/jira/browse/CALCITE-373">CALCITE-373</a>]
  `NULL` values in `NOT IN` sub-queries
* SQL functions:
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-422">CALCITE-422</a>]
    Add `REGR_SXX` and `REGR_SYY` regression functions
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-421">CALCITE-421</a>]
    Add `COVAR_POP` and `COVAR_SAMP` aggregate functions
* Planner rules:
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-425">CALCITE-425</a>]
    Add `FilterAggregateTransposeRule`, that pushes a filter through an
    aggregate
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-399">CALCITE-399</a>]
    Factorize common `AND` factors out of `OR` predicates
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-404">CALCITE-404</a>]
    `MergeProjectRule` should not construct `RexProgram`s for simple mappings
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-394">CALCITE-394</a>]
    Add `RexUtil.toCnf()`, to convert expressions to conjunctive normal form
    (CNF)
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-389">CALCITE-389</a>]
    `MergeFilterRule` should flatten `AND` condition
* Lattices:
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-428">CALCITE-428</a>]
    Use optimization algorithm to suggest which tiles of a lattice to
    materialize
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-410">CALCITE-410</a>]
    Allow lattice tiles to satisfy a query by rolling up
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-406">CALCITE-406</a>]
    Add tile and measure elements to lattice model element
  * Now, a lattice can materialize an aggregate-join and use it in a subsequent
    query.
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-402">CALCITE-402</a>]
    Lattice should create materializations on demand
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-344">CALCITE-344</a>]
    Lattice data structure
* Field trimmer:
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-408">CALCITE-408</a>]
    Make `FieldTrimmer` work with `RelNode` base classes
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-388">CALCITE-388</a>]
    Handle semi-joins in field trimmer
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-395">CALCITE-395</a>]
    Make `FieldTrimmer.trimFields(SetOp)` generate `ProjectRel` instead of
    `CalcRel`
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-393">CALCITE-393</a>]
    If no fields are projected from a table, field trimmer should project a
    dummy expression

#### API changes

* [<a href="https://issues.apache.org/jira/browse/CALCITE-413">CALCITE-413</a>]
  Add `RelDataTypeSystem` plugin, allowing different max precision of a
  `DECIMAL`
* In `Planner`, query de-correlation no longer requires state in a
  `SqlToRelConverter`.
* Factories:
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-392">CALCITE-392</a>]
    `RelFieldTrimmer` should use factory to create new rel nodes
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-382">CALCITE-382</a>]
    Refactoring rules to use factories
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-398">CALCITE-398</a>]
    Move `CalcRel.createProject` methods to `RelOptUtil`
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-396">CALCITE-396</a>]
    Change return type of `JoinFactory.createJoin()`; add `SemiJoinFactory`

#### Bug-fixes and internal changes

* [<a href="https://issues.apache.org/jira/browse/CALCITE-386">CALCITE-386</a>]
  Fix NOTICE
* Add tests inspired by Derby bugs.
* Add recent presentation to README.md.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-427">CALCITE-427</a>]
  Off-by-one issues in `RemoveDistinctAggregateRule`,
  `AggregateFilterTransposeRule`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-414">CALCITE-414</a>]
  Bad class name in `sqlline` shell script
* Bad package name in `package-info.java` was causing errors in Eclipse.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-412">CALCITE-412</a>]
  `RelFieldTrimmer`: when trimming `SortRel`, the collation and trait set don't
  match
* Add test case for
  [<a href="https://issues.apache.org/jira/browse/CALCITE-411">CALCITE-411</a>]
  Duplicate column aliases
* [<a href="https://issues.apache.org/jira/browse/CALCITE-407">CALCITE-407</a>]
  `RemoveTrivialProjectRule` drops child node's traits
* [<a href="https://issues.apache.org/jira/browse/CALCITE-409">CALCITE-409</a>]
  `PushFilterPastProjectRule` should not push filters past windowed aggregates
* Fix tests on Windows.
* Don't load `FoodMartQuerySet` unless we have to. It's big.
* Enable connection pooling in test suite.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-384">CALCITE-384</a>]
  Add `apache-` prefix to tarball and directory within tarball
* Freeze hive fmpp > freemarker plugin dependency.
* Upgrade Janino
* Removed hardcoded foodmart schema information
* [<a href="https://issues.apache.org/jira/browse/CALCITE-387">CALCITE-387</a>]
  CompileException when cast TRUE to nullable boolean
* Temporary fix for
  [<a href="https://issues.apache.org/jira/browse/CALCITE-390">CALCITE-390</a>]
  Transitive inference (`RelMdPredicates`) doesn't handle semi-join
* [<a href="https://issues.apache.org/jira/browse/CALCITE-385">CALCITE-385</a>]
  Change comment style for Java headers
* Disable test that is inconistent between JDK 1.7 and 1.8.
* Fix `git-commit-id-plugin` error when running in Travis-CI.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-381">CALCITE-381</a>]
  Remove plugin versions from the `<plugins>` tag in root pom
* [<a href="https://issues.apache.org/jira/browse/CALCITE-383">CALCITE-383</a>]
  Each jar should have a `git.properties` file describing its exact version
* Fix `mvn site` on JDK 1.8 and enable in Travis-CI.
* Status icon based on master branch, not whichever branch happened to build
  most recently.
* HOWTO:
  * Document how to build from git, and how to get jars from maven repo.
  * Optiq web site
  * Template emails for Apache votes
  * Update JIRA cases following release
  * Instructions for making and verifying a release

## <a href="https://github.com/apache/calcite/releases/tag/optiq-0.9.0-incubating">0.9.0-incubating</a> / 2014-08-19
{: #v0-9-0}

This is the first release under the Apache incubator process.

#### New features

* [<a href="https://issues.apache.org/jira/browse/CALCITE-371">CALCITE-371</a>]
  Implement `JOIN` whose `ON` clause contains mixed equi and theta
* [<a href="https://issues.apache.org/jira/browse/CALCITE-369">CALCITE-369</a>]
  Add `EnumerableSemiJoinRel`, implementation of semi-join in enumerable
  convention
* Add class `Strong`, for detecting null-rejecting predicates.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-368">CALCITE-368</a>]
  Add SemiJoinRule, planner rule to convert project-join-aggregate into semi-join
* [<a href="https://issues.apache.org/jira/browse/CALCITE-367">CALCITE-367</a>]
  `PushFilterPastJoinRule` should strengthen join type
* Add `EquiJoinRel`, base class for joins known to be equi-joins.
* Implement `CAST(<string> AS <datetime>)` and
  `<datetime> + <interval>`.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-360">CALCITE-360</a>]
  Introduce a rule to infer predicates from equi-join conditions
* [<a href="https://issues.apache.org/jira/browse/CALCITE-349">CALCITE-349</a>]
  Add heuristic join-optimizer that can generate bushy joins
* [<a href="https://issues.apache.org/jira/browse/CALCITE-346">CALCITE-346</a>]
  Add commutative join rule
* [<a href="https://issues.apache.org/jira/browse/CALCITE-347">CALCITE-347</a>]
  In `SqlRun`, add `!plan` command
* [<a href="https://issues.apache.org/jira/browse/CALCITE-314">CALCITE-314</a>]
  Allow simple UDFs based on methods
* [<a href="https://issues.apache.org/jira/browse/CALCITE-327">CALCITE-327</a>]
  Rules should use base class to find rule match & use factory for object
  creation
* [<a href="https://issues.apache.org/jira/browse/CALCITE-316">CALCITE-316</a>]
  In `SqlRun`, match output regardless of order if `ORDER BY` not present
* [<a href="https://issues.apache.org/jira/browse/CALCITE-300">CALCITE-300</a>]
  Support multiple parameters in `COUNT(DISTINCT x, y, ...)`

#### API changes

* [<a href="https://issues.apache.org/jira/browse/CALCITE-343">CALCITE-343</a>]
  RelDecorrelator should build its own mappings, not inherit from SqlToRelConverter
* Remove deprecated methods.
* Convert `Hook` to use Guava `Function` (was linq4j `Function1`).
* Add fluent method `withHook`, to more easily add hooks in tests.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-321">CALCITE-321</a>]
  Add support for overriding implementation of `CompoundIdentifier` in
  `SqlParser`.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-322">CALCITE-322</a>]
  Add support for `SqlExplain`, `SqlOrderBy` and `SqlWith` to support
  `SqlShuttle` use.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-323">CALCITE-323</a>]
  Override `SqlUnresolvedFunction.inferReturnType()` to return `ANY` type
  so framework implementors can support late bound function implementations.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-324">CALCITE-324</a>]
  Add `ViewExpander` for `Planner` in `Frameworks`. Expose additional
  properties of `ViewTable` to allow subclassing.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-247">CALCITE-247</a>]
  Add `Context` and `FrameworkConfig`

#### Bug-fixes and internal changes

* [<a href="https://issues.apache.org/jira/browse/CALCITE-380">CALCITE-380</a>]
  Downgrade to Guava 11.0.2
* Move several .md files into new 'doc' directory, to keep the root directory simple.
* Add DISCLAIMER
* Update history and HOWTO
* [<a href="https://issues.apache.org/jira/browse/CALCITE-377">CALCITE-377</a>]
  UnregisteredDriver should catch, log and re-throw NoClassDefFoundError
* Inherit maven-release-plugin from Apache POM.
* Test case for
  [<a href="https://issues.apache.org/jira/browse/CALCITE-373">CALCITE-373</a>]
  NOT IN and NULL values
* [<a href="https://issues.apache.org/jira/browse/CALCITE-372">CALCITE-372</a>]
  Change `LoptOptimizeJoinRule` &amp; `PushFilterPast`* rules to use factory
* Upgrade `maven-checkstyle-plugin`.
* Add class `Holder`, a mutable slot that can contain one object.
* Remove the 2-minute wait at the top of the hour for tests of
  `CURRENT_TIME`, etc.
* Tune `ImmutableIntList`'s iterators.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-364">CALCITE-364</a>]
  Validator rejects valid `WITH ... ORDER BY` query
* [<a href="https://issues.apache.org/jira/browse/CALCITE-363">CALCITE-363</a>]
  Use `dependencyManagement` and `pluginManagement` in POM files
* Add `FilterFactory`.
* Add `README` file, incubation disclaimers, and how-to build and running tests.
* Add `KEYS` and start how-to for making snapshots and releases.
* Capital case component names; inherit license info from Apache parent POM.
* Only run `apache-rat` and `git-commit-id-plugin` in "release" maven profile.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-348">CALCITE-348</a>]
  Add Apache RAT as maven plugin
* Change license headers from "Julian Hyde" to "ASF"; add headers where missing.
* Fix build breakage on JDK 1.6 due to missing method `BitSet.previousClearBit`.
* Refactor test infrastructure to allow testing against heuristic bushy-join
  optimizer.
* Add methods and tests for BitSets, and re-organize tests.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-354">CALCITE-354</a>]
  Change maven groupId to "org.apache.optiq"
* Specify return type when calling `RexBuilder.makeCall`, if possible.
* Eliminate duplicate conditions in `RexProgramBuilder.addCondition`, not
  `RexBuilder.makeCall` as previously.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-345">CALCITE-345</a>]
  `AssertionError` in `RexToLixTranslator` comparing to date literal
* Restore `PushFilterPastJoinRule` to `RelDecorrelator`; interim pending
  [<a href="https://issues.apache.org/jira/browse/CALCITE-343">CALCITE-343</a>]
  fix.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-340">CALCITE-340</a>]
  Fix bug in `SqlToRelConverter` when push expressions in join conditions into
  `ProjectRel`.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-313">CALCITE-313</a>]
  Query decorrelation fails
* While unifying a `RelNode` tree with a materialized view expression,
  switch representation to `MutableRel`s.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-305">CALCITE-305</a>]
  Unit test failure on release candidates
* [<a href="https://issues.apache.org/jira/browse/CALCITE-325">CALCITE-325</a>]
  Use Java list instead of Guava list to avoid null checks in case of
  `SqlTypeExplicitPrecedenceList`.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-326">CALCITE-326</a>]
  Fix `RelOptUtil` `ANY` type check.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-303">CALCITE-303</a>]
  Migrate issue URLs
* [<a href="https://issues.apache.org/jira/browse/CALCITE-331">CALCITE-331</a>]
  Precision/scale compatibility checks should always succeed for `ANY` type
* In `SqlRun`, allow `!plan` after `!ok` for same SQL statement.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-318">CALCITE-318</a>]
  Add unit test for `SqlRun`
* Fix a bug where composite `SELECT DISTINCT` would return duplicate rows.

## <a href="https://github.com/apache/calcite/releases/tag/optiq-parent-0.8">0.8</a> / 2014-06-27
{: #v0-8}

#### New features

* [<a href="https://issues.apache.org/jira/browse/CALCITE-310">CALCITE-310</a>]
   Implement LEAD, LAG and NTILE windowed aggregates
* Reduce `COUNT(not-null-expression)` to `COUNT()`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-292">CALCITE-292</a>]
   Improve windowed aggregate return types
* [<a href="https://issues.apache.org/jira/browse/CALCITE-302">CALCITE-302</a>]
   Use heuristic rule to plan queries with large numbers of joins
* [<a href="https://issues.apache.org/jira/browse/CALCITE-283">CALCITE-283</a>]
  Add TPC-DS data generator
* [<a href="https://issues.apache.org/jira/browse/CALCITE-294">CALCITE-294</a>]
  Implement DENSE_RANK windowed aggregate function
* SqlRun utility
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-290">CALCITE-290</a>]
    Add `SqlRun`, an idempotent utility for running SQL test scripts
  * Add "!skip" command to SqlRun.
  * Add MySQL formatting mode to SqlRun.

#### API changes

* Re-organize planner initialization,
  to make it easier to use heuristic join order.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-301">CALCITE-301</a>]
  Add `Program` interface, a planner phase more general than current `RuleSet`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-263">CALCITE-263</a>]
  Add operand type that will cause a rule to fire when a new subset is created
* Clean up and document SqlKind.
  * Add `IS_NOT_TRUE` and `IS_NOT_FALSE` `SqlKind` enums.
  * Add `SqlKind.IS_NOT_NULL` enum value, and use where possible,
    including for `IS_NOT_UNKNOWN` operator.

#### Bug-fixes and internal changes

* [<a href="https://issues.apache.org/jira/browse/CALCITE-312">CALCITE-312</a>]
  Trim non-required fields before `WindowRel`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-311">CALCITE-311</a>]
  Wrong results when filtering the results of windowed aggregation
* More tests for `WITH ... ORDER BY`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-309">CALCITE-309</a>]
  `WITH ... ORDER BY` query gives `AssertionError`
* Enable `MultiJoinRel` and some other planner rule tests.
* Add `ImmutableNullableList` and `UnmodifiableArrayList`,
  and remove calls to `Arrays.asList`.
* Add method `IntPair.zip`.
* Reimplement regular and windowed aggregates
* Switch from github to Apache JIRA for issues tracking.
  * In release history, update issue URLs from github to Apache JIRA
* The Apache mailing list is now the official mailing list. Add presentations.
* Add test for overloaded UDF.
* Add tests for `NOT IN` where sub-query returns NULL values.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-288">CALCITE-288</a>]
  Add tests for windowed aggregation based on Postgres reference queries
* [<a href="https://issues.apache.org/jira/browse/CALCITE-286">CALCITE-286</a>]
  Error casting MongoDB date
* [<a href="https://issues.apache.org/jira/browse/CALCITE-284">CALCITE-284</a>]
  Window functions range defaults to `CURRENT ROW`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-285">CALCITE-285</a>]
  Window functions throw exception without `ORDER BY`
* Test case for
  [<a href=“https://issues.apache.org/jira/browse/CALCITE-285”>CALCITE-285</a>].
* [<a href="https://issues.apache.org/jira/browse/CALCITE-281">CALCITE-281</a>]
  `EXTRACT` function's SQL return type is `BIGINT` but implemented as Java `int`

## <a href="https://github.com/apache/calcite/releases/tag/optiq-parent-0.7">0.7</a> / 2014-05-13
{: #v0-7}

#### New features

* Implement table functions.
* Arrays and multi-sets:
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-267">CALCITE-267</a>]
    Improve support for ARRAY data type
  * Better type information for JDBC Array; nested array now possible.
  * Implement `JOIN LATERAL` and `JOIN UNNEST`.
  * Implement the `UNNEST` relational operator, and various improvements
    to `ARRAY` and `MULTISET` data types.
  * Represent `ARRAY` columns as Java lists.
  * Implement `CARDINALITY(ARRAY)` SQL operator.
* Implement scalar sub-query in `SELECT` clause.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-273">CALCITE-273</a>]
  Support column alias in WITH queries (common table expressions)
* Windowed aggregates:
  * Aggregate over constants, e.g. `SUM(1) OVER (ROWS 10 PRECEDING)`;
  * `UNBOUNDED PRECEDING` window range;
  * Windowed aggregates computed over primitive scalars.
* Fix return type inference for aggregate calls. If the `GROUP BY` clause is
  empty, `SUM` may return null.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-37">CALCITE-37</a>]
  Document JSON model file format (as <a href="{{ site.sourceRoot }}/site/_docs/model.md">model.md</a>).
* [<a href="https://issues.apache.org/jira/browse/CALCITE-238">CALCITE-238</a>]
  Add adapter that generates TPC-H data
* Improve exception message in `AvaticaConnection`; add
  `ExceptionMessageTest`.
* Implement micro-benchmarks via
  <a href="http://openjdk.java.net/projects/code-tools/jmh/">JMH</a>.

#### API changes

* Provide an option to create root schema without the "metadata" schema.
* Schema SPI:
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-175">CALCITE-175</a>]
    Modify Schema SPI to allow caching
  * Get sub-schemas defined by a Schema SPI, and cache their `OptiqSchema`
    wrappers. (Tobi Vollebregt and Julian Hyde)
* SqlAdvisor callable from client via JDBC.

#### Bug-fixes and internal changes

* Add Apache incubator proposal.
* Rename RELEASE.md to HISTORY.md.
* Upgrade maven-release-plugin.
* Upgrade to linq4j-0.3.
* Code generation improvements:
 * Move code-generation optimizer to linq4j;
 * Improve translation of strict functions;
 * Mark most methods in `SqlFunctions` as `@Deterministic`;
 * Support `static final` constants generated by linq4j.
 * Avoid excessive box and unbox of primitives when using `Object[]` storage.
 * In JDBC result set, avoid row computation on each accessor call.
* Test composite join conditions in various flavors of outer join.
* Use `fromTrait` of the just previously converted `RelNode` instead
  of the original `RelNode`.
* Disable a MongoDB test, pending
  [<a href="https://issues.apache.org/jira/browse/CALCITE-270">CALCITE-270</a>].
* Hush warnings from `SplunkAdapterTest` if Splunk is not available.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-252">CALCITE-252</a>]
  Scalar sub-query that returns 0 rows should become NULL value
* `SplunkAdapterTest` now uses the same Foodmart database as `JdbcTest`.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-242">CALCITE-242</a>]
  SplunkAdapterTest fails
* Remove some obsolete classes.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-205">CALCITE-205</a>]
  Suspicious map.get in VolcanoPlanner.reregister

## <a href="https://github.com/apache/calcite/releases/tag/optiq-parent-0.6">0.6</a> / 2014-04-11
{: #v0-6}

#### New features

* [<a href="https://issues.apache.org/jira/browse/CALCITE-214">CALCITE-214</a>]
  Modify Frameworks to allow Schema to be re-used
  Obsoletes `name` field of `ReflectiveSchema`.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-237">CALCITE-237</a>]
  Allow user-defined aggregate functions (UDAs) to be defined in a model
* [<a href="https://issues.apache.org/jira/browse/CALCITE-227">CALCITE-227</a>]
  Extend `EXTRACT` function to support `DATE`, `TIME` and `TIMESTAMP` values
* [<a href="https://issues.apache.org/jira/browse/CALCITE-222">CALCITE-222</a>]
  User-defined table macros
* [<a href="https://issues.apache.org/jira/browse/CALCITE-179">CALCITE-179</a>]
  Optiq on Windows
  * Add `sqlline.bat` and fix issues running `sqlline` under Cygwin.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-195">CALCITE-195</a>]
  Push aggregation into MongoDB adapter
* [<a href="https://issues.apache.org/jira/browse/CALCITE-193">CALCITE-193</a>]
  Implement OFFSET and LIMIT in MongoDB adapter
* [<a href="https://issues.apache.org/jira/browse/CALCITE-164">CALCITE-164</a>]
  Improve query performance of optiq over MongoDB
* Add Phoenix (HBase) SQL dialect (Bruno Dumon)

#### API changes

* Obsolete `RexImpTable.AggregateImplementor` and rename `AggImplementor2`.
  (**This is a breaking change**.)
* Convert `CombinedParser.jj` into freemarker template to allow
  custom parser implementations. (Venki Korukanti)
* Extend `Planner` to pass a custom `ConvertletTable` and custom SQL parser.
* In `Frameworks`, add a way to specify list of `TraitDef`s that will be used
  by planner. (Jinfeng Ni)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-198">CALCITE-198</a>]
  Use `RexExecutor` to evaluate projections and filters
* [<a href="https://issues.apache.org/jira/browse/CALCITE-219">CALCITE-219</a>]
  Parse `ALTER scope SET option = value` statement
* [<a href="https://issues.apache.org/jira/browse/CALCITE-215">CALCITE-215</a>]
  A Schema should not have to remember its name and parent
  (**This is a breaking change**.)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-180">CALCITE-180</a>]
  Common base class for TableFunction, ScalarFunction
  (**This is a breaking change**.)
* Add methods for dealing with symbols; deprecate
  `SqlLiteral.booleanValue(SqlNode)`, `SqlLiteral.symbolValue(SqlNode)`.
* Add `RelOptPlanner.clear()`; now it is safe to call `transform` twice.
  (Jinfeng Ni)
* Remove APIs deprecated for 0.5.
* Move around some operator classes and singletons.

#### Bug-fixes and internal changes

* Upgrade to linq4j-0.2.
* `FETCH` and `LIMIT` are ignored during SQL-to-RelNode translation.
  (Venki Korukanti)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-245">CALCITE-245</a>]
  Off-by-one translation of ON clause of JOIN
* [<a href="https://issues.apache.org/jira/browse/CALCITE-191">CALCITE-191</a>]
  Rotate time/date/timestamp vals to local timezone
* [<a href="https://issues.apache.org/jira/browse/CALCITE-244">CALCITE-244</a>]
  `RelOptTableImpl.create` always expects `QueryableTable` type in
  `OptiqSchema.TableEntry`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-225">CALCITE-225</a>]
  Optiq doesn't correctly decorrelate queries
* Clean up package-info.  Remove duplicates in test packages so they
  don't conflict with those in non-test packages.
* Add `Pair.adjacents(Iterable)`.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-199">CALCITE-199</a>]
  Various `ANY` type conditions aren't correctly being considered
  (Jacques Nadeau)
* Add files to `.gitignore` that shouldn't be checked in when using
  Eclipse. (Jacques Nadeau)
* Add class `ControlFlowException`, and make it base class of
  existing control-flow exception classes.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-232">CALCITE-232</a>]
  Sum and avg of empty set should be null as per SQL specification
* Add `SqlUnresolvedFunction`, to improve how return type of
  user-defined functions is resolved. (Vladimir Sitnikov)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-228">CALCITE-228</a>]
  Error while compiling generated Java code when using UDF in expression
* [<a href="https://issues.apache.org/jira/browse/CALCITE-226">CALCITE-226</a>]
  User-defined functions should work without explicit schema prefix
* [<a href="https://issues.apache.org/jira/browse/CALCITE-229">CALCITE-229</a>]
  Join between different JDBC schemas not implementable
* [<a href="https://issues.apache.org/jira/browse/CALCITE-230">CALCITE-230</a>]
  RemoveSortRule derives trait set from sort, should derive it from sort's child
* Test view and sub-query with `ORDER BY` and `LIMIT`.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-223">CALCITE-223</a>]
  Add `NOTICE` and `LICENSE` files in all generated JAR files
* [<a href="https://issues.apache.org/jira/browse/CALCITE-209">CALCITE-209</a>]
  Consistent strategy for line-endings in tests
  Convert uses of `NL` in tests to Linux newline "\n".
  This makes string constants simpler.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-218">CALCITE-218</a>]
  Functions case sensitive when using `Lex.MYSQL`
* Add tests that a query with aggregate expressions in the `SELECT`
  clause is considered an aggregate query, even if there is no `GROUP BY`.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-216">CALCITE-216</a>]
  Inconsistent use of provided operator table causes inability to
  add aggregate functions
* [<a href="https://issues.apache.org/jira/browse/CALCITE-200">CALCITE-200</a>]
  Javadoc generation fails under JDK 1.8
* Add class `XmlOutput` (based on `org.eigenbase.xom.XMLOutput`) and remove
  dependency on eigenbase-xom.
* Performance: Don't create stack-trace for exceptions used for control-flow.
  (Vladimir Sitnikov)
* Performance: Tune `RexProgramBuilder` by using `Pair` rather than `String` as
  expression key. (Vladimir Sitnikov)
* Fix NPE using TRIM function with JDBC. (Bruno Dumon)
* Add dependency on
  <a href="https://github.com/julianhyde/hydromatic-resource">hydromatic-resource-maven-plugin</a>
  and obsolete our copy of the resource framework.
* Fix race condition in `SpaceList`.
* In planner, use `RelTrait.subsumes` rather than `equals` in an assert.
  (Jinfeng Ni)

## <a href="https://github.com/apache/calcite/releases/tag/optiq-parent-0.5">0.5</a> / 2014-03-14
{: #v0-5}

#### New features

* Allow `quoting`, `quotedCasing`, `unquotedCasing`, and `caseSensitive`
  properties to be specified explicitly (Vladimir Sitnikov)
* Recognize more kinds of materializations, including filter-on-project (where
  project contains expressions) and some kinds of aggregation.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-128">CALCITE-128</a>]
  Support `WITH` queries (common table expressions)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-53">CALCITE-53</a>]
  Allow `WHEN` clause in simple `CASE` expression to have multiple values
* [<a href="https://issues.apache.org/jira/browse/CALCITE-156">CALCITE-156</a>]
  Optiq should recognize 'SYSTEM TABLE', 'JOIN', 'INDEX' as table types
* Support querying ARRAY columns from JDBC source. (Gabriel Reid)

#### API changes

* Add
  `ProjectRelBase.copy(RelTraitSet, RelNode, List<RexNode>, RelDataType)`
  and make `ProjectRelBase.copy(RelTraitSet, RelNode)` final.
  (**This is a breaking change** for sub-classes of `ProjectRelBase`.)
* Change `RexBuilder.makeRangeReference` parameter type.
* `RexBuilder.makeInputRef` replaces `RelOptUtil.createInputRef`.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-160">CALCITE-160</a>]
  Allow comments in schema definitions
* [<a href="https://issues.apache.org/jira/browse/CALCITE-147">CALCITE-147</a>]
  Create a new kind of `SqlCall` that keeps operands in fields, not an operands
  array
  * Very widely used parse tree nodes with complex operands, including
    `SqlSelect`, `SqlJoin`, `SqlInsert`, and a new node type `SqlOrderBy`, are
    now sub-classes of `SqlCall` but not `SqlBasicCall`.
  * (**This is a breaking change** to code that assumes that, say,
    `SqlSelect` has an `operands` field.)
* Convert all enum constants to upper-case.
  (**This is a breaking change**.)

#### Bug-fixes and internal changes

* Generate optiq-core-VERSION-tests.jar not parent-VERSION-tests.jar.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-176">CALCITE-176</a>]
  ORDER BY expression doesn't work with SELECT \*
* Fix VARCHAR casts sent to hsqldb source (Bruno Dumon)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-143">CALCITE-143</a>]
  Remove dependency on eigenbase-resgen
* [<a href="https://issues.apache.org/jira/browse/CALCITE-173">CALCITE-173</a>]
  Case-insensitive table names are not supported for `Casing.UNCHANGED`
* `DATE.getLimit` now returns `Calendar` in GMT time zone (Vladimir Sitnikov)
* Set `en_US` locale in tests that match against error numbers, dates
  (Vladimir Sitnikov)
* Use 1 test thread per CPU to avoid thread starvation on dual core CPUs
  (Vladimir Sitnikov)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-174">CALCITE-174</a>]
  Move hsqldb to test scope
* Add unit tests for `RexExecutorImpl`.
* Correct JSON model examples in Javadoc comments. (Karel Vervaeke)
* Move test reference logs from `src/test/java` to `src/test/resources`
  (reduces the number of 'untracked files' reported by git)
* Tune `Util.SpaceList`, fix race condition, and move into new utility class
  `Spaces`.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-163">CALCITE-163</a>]
  Equi-join warning
* [<a href="https://issues.apache.org/jira/browse/CALCITE-157">CALCITE-157</a>]
  Handle `SQLFeatureNotSupported` when calling `setQueryTimeout`
  (Karel Vervaeke)
* Fix Optiq on Windows. (All tests and checkstyle checks pass.)
* In checkstyle, support Windows-style file separator, otherwise build fails in
  Windows due to suppressions not used. (Vladimir Sitnikov)
* Enable MongoDB tests when `-Dcalcite.test.mongodb=true`.
* Cleanup cache exception-handling and an assert.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-153">CALCITE-153</a>]
  Error using MongoDB adapter: Failed to set setXIncludeAware(true)
* Disable spark engine unless Spark libraries are on the class path and
  `spark=true` is specified in the connect string.
* Fix path to `mongo-zips-model.json` in HOWTO. (Mariano Luna)
* Fix bug deriving the type of a join-key.
* Fix the value of `ONE_MINUS_EPSILON`.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-158">CALCITE-158</a>]
  Optiq fails when call `Planner.transform()` multiple times, each with
  different ruleset
* [<a href="https://issues.apache.org/jira/browse/CALCITE-148">CALCITE-148</a>]
 Less verbose description of collation. Also, optimize `RelTraitSet` creation
 and amortize `RelTraitSet.toString()`.
* Add generics to SQL parser.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-145">CALCITE-145</a>]
  Unexpected upper-casing of keywords when using java lexer
* Remove duplicate `maven-source-plugin`.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-141">CALCITE-141</a>]
  Downgrade to guava-11.0.2. (This is necessary for Hadoop compatibility.
  Later versions of Guava can also be used.)
* Upgrade to spark-0.9.0. (Because this version of spark is available from
  maven-central, we can make optiq-spark part of the regular build, and remove
  the spark profile.)

## <a href="https://github.com/apache/calcite/releases/tag/optiq-parent-0.4.18">0.4.18</a> / 2014-02-14
{: #v0-4-18}

#### API and functionality changes

* Configurable lexical policy
    * [<a href="https://issues.apache.org/jira/browse/CALCITE-33">CALCITE-33</a>]
      SQL parser should allow different identifier quoting
    * [<a href="https://issues.apache.org/jira/browse/CALCITE-34">CALCITE-34</a>]
      Policy for case-sensitivity of identifiers should be configurable
    * New connect-string parameter "lex", with allowable values
      "ORACLE", "MYSQL", "SQL_SERVER", "JAVA" sets policy to be like those
      databases, in terms of quote string, whether quoted and unquoted
      identifiers are converted to upper/lower case, and whether
      identifiers are matched case-sensitively. "JAVA" is case-sensitive,
      even for unquoted identifiers. It should be possible
      for each connection to have its own settings for these. Objects
      shared between sessions (views, materialized views) might
      require more work.
    * Added various internals to make it easy for developers to do the
      right thing. When you need to look up a schema, table or
      column/field name, you should use a catalog reader, and it will
      apply the right case-sensitivity policy.
    * Enable optiq consumer to utilize different lexical settings in
      Frameworks/Planner. (Jacques Nadeau)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-135">CALCITE-115</a>]
  Add a PARSE_TREE hook point with SqlNode parameter
* Change planner rules to use `ProjectFactory` for creating
  projects. (John Pullokkaran)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-131">CALCITE-131</a>]
  Add interfaces for metadata (statistics)
  (**This is a breaking change**.)
* Update Avatica to allow `Cursor` & `Accessor` implementations to throw
  `SQLException`. (Jacques Nadeau)
* Separate cost model (`RelOptCostFactory`) from planner. Allow
  `VolcanoPlanner` to be sub-classed with different cost factory.
    * Remove references to VolcanoCost from RelSubset, so clients can
      use a different `RelOptCost`. (Harish Butani)
    * Make `VolcanoCost` immutable.
* Break `SqlTypeStrategies` into `OperandTypes`, `ReturnTypes` and
  `InferTypes`, and rename its static members to upper-case, per
  checkstyle. (**This is a breaking change**.)
* Add a mechanism for defining configuration parameters and have them
  appear in the responses to `AvaticaDatabaseMetaData` methods.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-113">CALCITE-113</a>]
  User-defined scalar functions
* Add rules to short-cut a query if `LIMIT 0` is present. Also remove
  sort, aggregation, join if their inputs are known to be empty, and
  propagate the fact that the relational expressions are known to be
  empty up the tree. (We already do this for union, filter, project.)
* `RexNode` and its sub-classes are now immutable.

#### Bug-fixes and internal changes

* [<a href="https://issues.apache.org/jira/browse/CALCITE-61">CALCITE-16</a>]
  Upgrade to janino-2.7
* Upgrade to guava-15.0 (guava-14.0.1 still allowed), sqlline-1.1.7,
  maven-surefire-plugin-2.16, linq4j-0.1.13.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-136">CALCITE-136</a>]
  Support Hive dialect
* [<a href="https://issues.apache.org/jira/browse/CALCITE-138">CALCITE-138</a>]
  SqlDataTypeSpec.clone handles collection types wrong
* [<a href="https://issues.apache.org/jira/browse/CALCITE-137">CALCITE-137</a>]
  If a subset is created that is subsumed by an existing subset, its
  'best' is not assigned
    * If best rel in a Volcano subset doesn't have metadata, see if
      other rels have metadata.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-127">CALCITE-127</a>]
  EnumerableCalcRel can't support 3+ AND conditions (Harish Butani)
* Fix push-down of datetime literals to JDBC data sources.
* Add `Util.startsWith(List, List)` and `Util.hashCode(double)`.
* Add maven-checkstyle-plugin, enable in "verify" phase, and fix exceptions.
* Fix `SqlValidator` to rely on `RelDataType` to do field name matching.  Fix
  `RelDataTypeImpl` to correctly use the case sensitive flag rather than
  ignoring it.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-119">CALCITE-119</a>]
  Comparing Java type long with SQL type INTEGER gives wrong answer
* Enable multi-threaded testing, and fix race conditions.
    * Two of the race conditions involved involving trait caches. The
      other was indeterminacy in type system when precision was not
      specified but had a default; now we canonize TIME to TIME(0), for
      instance.
* Convert files to `us-ascii`.
* Work around
  [<a href="http://jira.codehaus.org/browse/JANINO-169">JANINO-169</a>].
* Refactor SQL validator testing infrastructure so SQL parser is
  configurable.
* Add `optiq-mat-plugin` to README.
* Fix the check for duplicate subsets in a rule match.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-112">CALCITE-112</a>]
  Java boolean column should be treated as SQL boolean
* Fix escaped unicode characters above 0x8000. Add tests for unicode
  strings.

## <a href="https://github.com/apache/calcite/releases/tag/optiq-parent-0.4.17">0.4.17</a> / 2014-01-13
{: #v0-4-17}

#### API changes

* [<a href="https://issues.apache.org/jira/browse/CALCITE-106">CALCITE-106</a>]
  Make `Schema` and `Table` SPIs simpler to implement, and make them
  re-usable across connections
  (**This is a breaking change**.)
* Make it easier to define sub-classes of rule operands. The new class
  `RelOptRuleOperandChildren` contains the children of an operand and
  the policy for dealing with them. Existing rules now use the new
  methods to construct operands: `operand()`, `leaf()`, `any()`, `none()`,
  `unordered()`. The previous methods are now deprecated and will be
  removed before 0.4.18. (**This is a breaking change**.)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-101">CALCITE-101</a>]
  Enable phased access to the Optiq engine
* List-handling methods in `Util`: add methods `skipLast`, `last`, `skip`;
  remove `subList`, `butLast`.
* Convert `SqlIdentifier.names` from `String[]` to `ImmutableList<String>`.
* Rename `OptiqAssert.assertThat()` to `that()`, to avoid clash with junit's
  `Assert.assertThat()`.
* Usability improvements for `RelDataTypeFactory.FieldInfoBuilder`. It
  now has a type-factory, so you can just call `build()`.
* Rework `HepProgramBuilder` into a fluent API.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-105">CALCITE-105</a>]
  Externalize RelNode to and from JSON

#### Tuning

* If `EnumerableAggregateRel` has no aggregate functions, generate a
   call to `Enumerable.distinct()`, thereby saving the effort of
   building trivial accumulators.
* Default rule set now does not introduce `CalcRel` until a later phase
  of planning. This reduces the number of trivial projects and calcs
  created, merged, and elimated.
* Reduce the amount of time spent creating record types that
  already exist.
* More efficient implementation of `Util.isDistinct` for small lists.
* When an internal record has 0 fields, rather than generating a
  synthetic class and lots of instances that are all the same, use the
  new `Unit` class, which is a singleton.
* To take advantage of asymmetric hash join added recently in linq4j,
  tweak cost of `EnumerableJoinRel` so that join is cheaper if the
  larger input is on the left, and more expensive if it is a cartesian
  product.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-70">CALCITE-70</a>]
  Joins seem to be very expensive in memory
* Make planning process more efficient by not sorting the list of
  matched rules each cycle. It is sorted if tracing is enabled;
  otherwise we scan to find the most important element. For this list,
  replace `LinkedList` with `ChunkList`, which has an O(1) remove and add,
  a fast O(n) get, and fast scan.

#### Other

* [<a href="https://issues.apache.org/jira/browse/CALCITE-87">CALCITE-87</a>]
  Constant folding
  * Rules for constant-expression reduction, and to simplify/eliminate
    `VALUES` operator.
* Graph algorithms: Implement breadth-first iterator and cycle-detector.
* Fix bug in planner which occurred when two `RelNode`s have identical
  digest but different row-type.
* Fix link to optiq-csv tutorial.
* Fix bugs in `RemoveTrivialProjectRule.strip`, `JdbcProjectRel.implement`
  and `SortRel.computeSelfCost`.
* Reformat code, and remove `@author` tags.
* Upgrade to eigenbase-xom-1.3.4, eigenbase-properties-1.1.4,
  eigenbase-resgen-1.3.6.
* Upgrade to linq4j-0.1.12.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-97">CALCITE-97</a>]
  Correlated EXISTS
* Fix a bug in `VolcanoCost`.
* Add class `FoodMartQuerySet`, that contains the 6,700 foodmart queries.
* Fix factory class names in `UnregisteredDriver`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-96">CALCITE-96</a>]
  LIMIT against a table in a clone schema causes UnsupportedOperationException
* Disable spark module by default.
* Allow `CloneSchema` to be specified in terms of url, driver, user,
  password; not just dataSource.
* Wrap internal error in `SQLException`.

## <a href="https://github.com/apache/calcite/releases/tag/optiq-parent-0.4.16">0.4.16</a> / 2013-11-24
{: #v0-4-16}

* [<a href="https://issues.apache.org/jira/browse/CALCITE-69">CALCITE-69</a>]
  Can't join on string columns and other problems with expressions in the join
  condition
* [<a href="https://issues.apache.org/jira/browse/CALCITE-74">CALCITE-74</a>]
  JOIN ... USING fails in 3-way join with UnsupportedOperationException.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-65">CALCITE-65</a>]
  Fix issues in the JDBC driver, and in particular to DatabaseMetaData methods,
  to make Squirrel-SQL run better.
* Fix JDBC column, table, schema names for when the table is not in a schema of
  depth 1.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-85">CALCITE-85</a>]
  Adding a table to the root schema causes breakage in OptiqPrepareImpl
* [<a href="https://issues.apache.org/jira/browse/CALCITE-84">CALCITE-84</a>]
  Extract Optiq's JDBC driver as a new JDBC driver framework, Avatica.
  Other projects can use this to implement a JDBC driver by implementing
  just a few methods. If you wish to use Optiq's JDBC driver, you will
  now need to include optiq-avatica.jar in addition to optiq-core.jar.
  Avatica does not depend on anything besides the standard Java library.
* Support for parameters in PreparedStatement.
* First steps in recognizing complex materializations. Internally we introduce a
  concept called a "star table", virtual table composed of real tables joined
  together via many-to-one relationships. The queries that define
  materializations and end-user queries are canonized in terms of star tables.
  Matching (not done yet) will then be a matter of looking for sort, groupBy,
  project. It is not yet possible to define a star in an Optiq model file.
* Add section to <a href="{{ site.sourceRoot }}/site/_docs/howto.md">HOWTO</a>
  on implementing adapters.
* Fix data type conversions when creating a clone table in memory.
* Fix how strings are escaped in JsonBuilder.
* Test suite now depends on an embedded hsqldb database, so you can run
  <code>mvn test</code> right after pulling from git. You can instead use a
  MySQL database if you specify '-Dcalcite.test.db=mysql', but you need to
  manually populate it.
* Fix a planner issue which occurs when the left and right children of join are
  the same relational expression, caused by a self-join query.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-76">CALCITE-76</a>]
  Precedence of the item operator, <code>map[index]</code>; remove the space
  before '[' when converting parse tree to string.
* Allow <code>CAST(expression AS ANY)</code>, and fix an issue with the ANY type
  and NULL values.
* Handle null timestamps and dates coming out of JDBC adapter.
* Add <code>jdbcDriver</code> attribute to JDBC schema in model, for drivers
  that do not auto-register.
* Allow join rules to match any subclass of JoinRelBase.
* Push projects, filters and sorts down to MongoDB. (Fixes
  [<a href="https://issues.apache.org/jira/browse/CALCITE-57">CALCITE-57</a>],
  [<a href="https://issues.apache.org/jira/browse/CALCITE-60">CALCITE-60</a>] and
  [<a href="https://issues.apache.org/jira/browse/CALCITE-72">CALCITE-72</a>].)
* Add instructions for loading FoodMart data set into MongoDB, and how to enable
  tracing.
* Now runs on JDK 1.8 (still runs on JDK 1.6 and JDK 1.7).
* Upgrade to junit-4.11 (avoiding the dodgy junit-4.1.12).
* Upgrade to linq4j-0.1.11.

## <a href="https://github.com/apache/calcite/releases/tag/optiq-parent-0.4.15">0.4.15</a> / 2013-10-14
{: #v0-4-15}

* Lots of good stuff that this margin is too small to contain. See
  <a href="{{ site.sourceRoot }}/site/_docs/reference.md">SQL language reference</a> and
  <a href="{{ site.sourceRoot }}/site/_docs/model.md">JSON model reference</a>.

# Optiq-csv release history

Optiq-csv-0.3 was the last independent release of optiq-csv. From
calcite-0.9.2 onwards, the code was included as the
calcite-example-csv module.

* Upgrade to calcite-0.9.1
* Support gzip-compressed CSV and JSON files (recognized by '.gz' suffix)
* Cleanup, and fix minor timezone issue in a test
* Support for date types (date, time, timestamp) (Martijn van den Broek)
* Upgrade to optiq-0.8, optiq-avatica-0.8, linq4j-0.4
* Add support for JSON files (recognized by '.json' suffix)
* Upgrade maven-release-plugin to version 2.4.2
* Upgrade to optiq-0.6, linq4j-0.2
* Add NOTICE and LICENSE files in generated JAR file

## <a href="https://github.com/julianhyde/optiq-csv/releases/tag/optiq-csv-0.3">0.3</a> / 2014-03-21
{: #csv-v0-3}

* Upgrade to optiq-0.5
* Add workaround to
  [<a href="https://github.com/jline/jline2/issues/62">JLINE2-62</a>]
  to `sqlline.bat` (windows) and `sqlline` (windows using cygwin)
* Fix classpath construction: `sqlline.bat` copies dependencies to
  `target/dependencies`; `sqlline` constructs `target/classpath.txt`
* Build, checkstyle and tests now succeed on windows (both native and cygwin)
* Models can now contain comments
* [<a href="https://github.com/julianhyde/optiq-csv/issues/2">OPTIQ-CSV-2</a>]
  Update tutorial to reflect changes to Optiq's JDBC adapter

## <a href="https://github.com/julianhyde/optiq-csv/releases/tag/optiq-csv-0.2">0.2</a> / 2014-02-18
{: #csv-v0-2}

* Add test case for
  [<a href="https://issues.apache.org/jira/browse/CALCITE-112">CALCITE-112</a>]
* Add `sqlline.bat`, Windows SQL shell (based on fix for
  [<a href="https://issues.apache.org/jira/browse/DRILL-338">DRILL-338</a>])
* Upgrade to optiq-0.4.18, sqlline-1.1.7
* Return a single object for single-col enumerator (Gabriel Reid)
* Enable maven-checkstyle-plugin; fix checkstyle exceptions

## <a href="https://github.com/julianhyde/optiq-csv/releases/tag/optiq-csv-0.1">0.1</a> / 2014-01-13
{: #csv-v0-1}

* Add release notes and history
* Enable maven-release-plugin
* Upgrade to optiq-0.4.17, linq4j-0.1.12, sqlline-1.1.6
* Upgrade tutorial for new Schema and Table SPIs
* Fixes for optiq SPI changes in
  [<a href="https://issues.apache.org/jira/browse/CALCITE-106">CALCITE-106</a>]
* Enable oraclejdk8 in Travis CI
* Fix bug where non-existent directory would give NPE; instead print warning
* Add an example of a planner rule
* Add `CsvTableFactory`, an example of a custom table
* Add a view to tutorial
* Split into scenario with a "simple" schema that generates tables
  (`CsvTable`) that just execute and a "smart" schema that generates
  tables (`CsvSmartTable`) that undergo optimization
* Make `CsvEnumerator` a top-level class
* Implement the algorithms to sniff names and types from the first
  row, and to return an enumerator of all rows
* Read column types from header of CSV file

# Linq4j release history

Linq4j-0.4 was the last independent release of linq4j. From
calcite-0.9.2 onwards, the code was included as calcite-linq4j, and
features added to linq4j in a particular calcite release are described
with the other changes in that release.

## <a href="https://github.com/julianhyde/linq4j/releases/tag/linq4j-0.4">0.4</a> / 2014-05-28
{: #linq4j-v0-4}

* Fix <a href="https://github.com/julianhyde/linq4j/issues/27">#27</a>,
  "Incorrectly inlines non-final variable".
* Maven build process now deploys web site.
* Implement `Enumerable` methods: `any`, `all`,
  `contains` with `EqualityComparer`, `first`, `first` with predicate.

## <a href="https://github.com/julianhyde/linq4j/releases/tag/linq4j-0.3">0.3</a> / 2014-04-21
{: #linq4j-v0-3}

* Move optimizer visitor from optiq to linq4j; add
  `ExpressionType.modifiesLvalue` to avoid invalid inlining.
* Fix <a href="https://github.com/julianhyde/linq4j/issues/17">#17</a>,
  "Assign constant expressions to 'static final' members";
  add `@Deterministic` annotation to help deduce which expressions are
  constant.
* Multi-pass optimization: some of the variables might be avoided and
  inlined after the first pass.
* Various other peephole optimizations: `Boolean.valueOf(const)`,
  'not' expressions (`!const`, `!!a`, `!(a==b)`, `!(a!=b)`, `!(a>b)`,
  etc.),
  '?' expressions coming from `CASE` (`a ? booleanConstant : b` and `a
  ? b : booleanConstant`).
* Implement left, right and full outer join.
* Clean build on cygwin/Windows.

## <a href="https://github.com/julianhyde/linq4j/releases/tag/linq4j-0.2">0.2</a> / 2014-04-11
{: #linq4j-v0-2}

* Fix <a href="https://github.com/julianhyde/linq4j/issues/8">#8</a>,
  "Javadoc generation fails under JDK 1.8".
* Fix <a href="https://github.com/julianhyde/linq4j/issues/15">#15</a>,
  "`Expressions.ifThenElse` does not work".
* Use `HashMap` for searching of declarations to reuse; consider both
  `optimizing` and `optimize` flags when reusing.
* Implement `equals` and `hashCode` for expressions. Hash codes for
  complex expressions are cached into a field of the expression.
* Add example, `com.example.Linq4jExample`.
* Fix optimizing away parameter declarations in assignment target.
* Support Windows path names in checkstyle-suppresions.
* Support `Statement.toString` via `ExpressionWriter`.
* Use `AtomicInteger` for naming of `ParameterExpression`s to avoid
  conflicts in multithreaded usage
* Cleanup: use `Functions.adapt` rather than `new AbstractList`
* Add `NOTICE` and `LICENSE` files in generated JAR file.
* Optimize `select()` if selector is identity.
* Enable checkstyle.

## <a href="https://github.com/julianhyde/linq4j/releases/tag/linq4j-0.1.13">0.1.13</a> / 2014-01-20
{: #linq4j-v0-1-13}

* Remove spurious "null" generated when converting expression to string.
* Allow a field declaration to not have an initializer.
* Add `Primitive.defaultValue`.
* Enable `oraclejdk8` in <a href="https://travis-ci.org/julianhyde/linq4j">Travis CI</a>.

## <a href="https://github.com/julianhyde/linq4j/releases/tag/linq4j-0.1.12">0.1.12</a> / 2013-12-07
{: #linq4j-v0-1-12}

* Add release notes.
* Fix implementation of `Enumerable.asEnumerable` in
  `DefaultQueryable` (inherited by most classes that implement
  `Queryable`).

## <a href="https://github.com/julianhyde/linq4j/releases/tag/linq4j-0.1.11">0.1.11</a> / 2013-11-06
{: #linq4j-v0-1-11}

* Initial commit
