// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Generates the test jobs of main.yml. Each run picks MATRIX_JOBS rows: the
// required rows below, then rows that cover value pairs no earlier row covers.
// A pull request keeps the same rows across pushes, and every run prints its
// seed. To list the rows of a run locally:
//
//   npm ci --prefix .github/workflows
//   RNG_SEED=<seed> MATRIX_JOBS=6 node .github/workflows/matrix.mjs
//
// Add --coverage to print the share of value pairs the rows cover.
import { createGitHubMatrixBuilder, setGitHubOutput } from '@vlsi/github-actions-random-matrix/github';

const { matrix } = createGitHubMatrixBuilder();
matrix.failOnUnsatisfiableFilters(true);

// The tests run on the JDK that runs Gradle. Gradle 8.14 runs on JDK 24 at
// most, so JDK 25 compiles the code but does not run the tests yet.
matrix.addAxis({
  name: 'java_version',
  title: x => 'JDK ' + x,
  values: ['8', '11', '17', '21', '24'],
});

matrix.addAxis({
  name: 'java_distribution',
  // Oracle is missing: Oracle publishes current builds only for JDK 25, which
  // cannot run Gradle 8.14. setup-java takes older Oracle JDKs from the archive,
  // which holds only the first GA build of each.
  values: ['zulu', 'temurin', 'corretto', 'liberica', 'microsoft'],
});

// GUAVA_MIN and GUAVA_MAX come from main.yml, which the ErrorProne and
// Checker Framework jobs read as well.
const guavaMin = process.env.GUAVA_MIN || '21.0';
const guavaMax = process.env.GUAVA_MAX || '33.4.8-jre';
matrix.addAxis({
  name: 'guava',
  title: x => 'Guava ' + x,
  values: [guavaMin, '27.1-jre', '31.1-jre', guavaMax],
});

matrix.addAxis({
  name: 'os',
  title: x => x.value.replace(/-latest$/, ''),
  values: [
    {value: 'ubuntu-latest', weight: 4},
    {value: 'windows-latest', weight: 1},
    {value: 'macos-15', weight: 1},
  ],
});

matrix.addAxis({
  name: 'tz',
  values: [
    'UTC',
    // UTC-05:00 in winter, UTC-04:00 in summer
    'America/New_York',
    // UTC+12:45 in winter, UTC+13:45 in summer
    'Pacific/Chatham',
    // UTC+00:00 in winter, so DST bugs hide behind a zero offset
    'Europe/London',
  ],
});

matrix.addAxis({
  name: 'locale',
  title: x => x.language + '_' + x.country,
  values: [
    {language: 'en', country: 'US'},
    // tr_TR is missing: innodb-java-reader 1.0.10 uppercases column types in
    // the default locale, so "int" becomes "İNT" and InnodbAdapterDataTypesTest
    // fails.
    // Uses a comma as the decimal separator
    {language: 'de', country: 'DE'},
    {language: 'fr', country: 'CA'},
    {language: 'ru', country: 'RU'},
  ],
});

// Calcite pins one Avatica release in gradle.properties; "main" builds Avatica
// from its main branch, so incompatible changes show up before a release.
matrix.addAxis({
  name: 'avatica',
  values: [
    {value: 'release', title: '', weight: 4},
    {value: 'main', title: 'Avatica main', weight: 1},
  ],
});

matrix.setNamePattern(['java_version', 'java_distribution', 'guava', 'os', 'tz', 'locale', 'avatica']);

// Microsoft builds only LTS releases starting with 11.
matrix.imply({java_distribution: 'microsoft'}, {java_version: ['11', '17', '21']});
// Avatica's own CI builds on JDK 8 and 11, on Linux.
matrix.imply({avatica: {value: 'main'}}, {java_version: ['8', '11'], os: {value: 'ubuntu-latest'}});
// Temurin publishes no JDK 8 for macOS on arm64.
matrix.exclude({os: {value: 'macos-15'}, java_distribution: 'temurin', java_version: '8'});

const javaVersions = matrix.axisByName.java_version.values;

const include = matrix.generateRows(Number(process.env.MATRIX_JOBS || 6), {
  require: [
    {os: {value: 'windows-latest'}},
    {os: {value: 'macos-15'}},
    // Javadoc options depend on the JDK. JDK 24 is missing: its javadoc
    // reports "reference not found" in RelBuilder, SqlFunctions, and
    // SqlValidatorScope.
    {
      filter: {java_version: '8'},
      tag: row => { row.javadoc = true; },
    },
    {
      filter: {java_version: '21'},
      tag: row => { row.javadoc = true; },
    },
    {java_version: javaVersions[javaVersions.length - 1]},
    {guava: guavaMin},
    {guava: guavaMax},
    {avatica: {value: 'main'}},
  ],
});
if (include.length === 0) {
  throw new Error('Matrix list is empty');
}

include.sort((a, b) => a.name.localeCompare(b.name, undefined, {numeric: true}));

include.forEach(v => {
  // These reach the test JVM only; Gradle keeps the runner's locale.
  const jvmArgs = [
    `-Duser.language=${v.locale.language}`,
    `-Duser.country=${v.locale.country}`,
    // The JVM on Windows ignores TZ, so set the time zone explicitly.
    `-Duser.timezone=${v.tz}`,
  ];
  v.testExtraJvmArgs = jvmArgs.join(' ');
  v.os = v.os.value;
  v.avatica = v.avatica.value;
  delete v.locale;
});

if (process.argv.includes('--coverage')) {
  const coverage = matrix.pairCoverageReport();
  console.log(`Pair coverage: ${coverage.covered}/${coverage.total} (${coverage.percentage}%)`);
} else {
  console.log(include);
  setGitHubOutput('matrix', {include});
}
