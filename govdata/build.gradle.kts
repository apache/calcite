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
dependencies {
    api(project(":core"))
    api(project(":linq4j"))
    api(project(":file"))
    api("org.checkerframework:checker-qual")

    implementation("com.google.guava:guava")
    implementation("org.apache.calcite.avatica:avatica-core")
    implementation("commons-io:commons-io")
    implementation("org.apache.commons:commons-lang3")

    // XML processing (SEC is XML-based, we'll use standard parsers)
    implementation("javax.xml.bind:jaxb-api:2.3.1")

    // Parquet dependencies (inherited from file adapter)
    implementation("org.apache.parquet:parquet-arrow:1.15.2")
    implementation("org.apache.parquet:parquet-avro:1.15.2")
    implementation("org.apache.parquet:parquet-column:1.15.2")
    implementation("org.apache.parquet:parquet-common:1.15.2")
    implementation("org.apache.parquet:parquet-encoding:1.15.2")
    implementation("org.apache.parquet:parquet-hadoop:1.15.2")

    // Hadoop dependencies needed for Parquet
    implementation("org.apache.hadoop:hadoop-common:3.3.6")
    implementation("org.apache.hadoop:hadoop-client:3.3.6")

    // HTTP client for SEC EDGAR API
    implementation("org.apache.httpcomponents:httpclient:4.5.14")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.17")

    // HTML parsing for inline XBRL
    implementation("org.jsoup:jsoup")

    // Embedding model dependencies
    implementation("com.microsoft.onnxruntime:onnxruntime:1.16.3")
    implementation("ai.djl:api:0.25.0")
    implementation("ai.djl.huggingface:tokenizers:0.25.0")

    testImplementation(project(":testkit"))
    testImplementation("org.junit.jupiter:junit-jupiter-api")
    testImplementation("org.duckdb:duckdb_jdbc:1.1.3")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
    testRuntimeOnly("org.apache.logging.log4j:log4j-slf4j2-impl:2.23.1")
    testRuntimeOnly("org.apache.logging.log4j:log4j-core:2.23.1")
}

tasks.test {
    workingDir = layout.buildDirectory.get().asFile

    testLogging {
        events("passed", "skipped", "failed", "standardOut", "standardError")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
        showExceptions = true
        showCauses = true
        showStackTraces = true
    }

    useJUnitPlatform {
        includeTags("unit")
        if (project.hasProperty("runAllTests")) {
            includeTags()
        }
        if (project.hasProperty("includeTags")) {
            val tags = project.property("includeTags").toString().split(",")
            includeTags(*tags.toTypedArray())
        }
    }
}
