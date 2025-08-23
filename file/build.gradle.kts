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
import com.github.vlsi.gradle.ide.dsl.settings
import com.github.vlsi.gradle.ide.dsl.taskTriggers

plugins {
    id("com.github.vlsi.ide")
}

dependencies {
    api(project(":core"))
    api(project(":linq4j"))
    api(project(":arrow")) // Need arrow for ParquetTable
    api("org.checkerframework:checker-qual")

    implementation("com.google.guava:guava")
    implementation("com.joestelmach:natty")
    implementation("com.opencsv:opencsv:5.7.1")
    implementation("org.apache.calcite.avatica:avatica-core")
    implementation("commons-io:commons-io")
    implementation("org.apache.commons:commons-lang3")
    implementation("org.jsoup:jsoup")
    implementation("org.apache.poi:poi:5.3.0")
    implementation("org.apache.poi:poi-ooxml:5.3.0")
    implementation("com.fasterxml.jackson.core:jackson-core:2.17.2")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.17")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.17.2")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.17.2")

    // Arrow dependencies for vectorized execution engine
    implementation("org.apache.arrow:arrow-vector:15.0.2")
    implementation("org.apache.arrow:arrow-memory-netty:15.0.2")
    implementation("org.apache.arrow:arrow-format:15.0.2")
    // Removed arrow-c-data - was only for DuckDB Arrow integration which has 400+ms overhead
    implementation("org.apache.arrow.gandiva:arrow-gandiva")

    // Parquet dependencies for Parquet execution engine
    implementation("org.apache.parquet:parquet-arrow:1.15.2")
    implementation("org.apache.parquet:parquet-avro:1.15.2")
    implementation("org.apache.parquet:parquet-column:1.15.2")
    implementation("org.apache.parquet:parquet-common:1.15.2")
    implementation("org.apache.parquet:parquet-encoding:1.15.2")
    implementation("org.apache.parquet:parquet-hadoop:1.15.2")

    // Hadoop dependencies for Parquet
    implementation("org.apache.hadoop:hadoop-common:3.3.6")
    implementation("org.apache.hadoop:hadoop-client:3.3.6")

    // Storage provider dependencies
    implementation("com.amazonaws:aws-java-sdk-s3:1.12.565")
    implementation("commons-net:commons-net:3.9.0")
    implementation("com.jcraft:jsch:0.1.55")

    // Apache Iceberg support
    implementation("org.apache.iceberg:iceberg-core:1.4.0")
    implementation("org.apache.iceberg:iceberg-api:1.4.0")
    implementation("org.apache.iceberg:iceberg-common:1.4.0")
    implementation("org.apache.iceberg:iceberg-parquet:1.4.0")
    implementation("org.apache.iceberg:iceberg-data:1.4.0")

    testImplementation(project(":testkit"))
    // DuckDB for performance comparison tests and optional execution engine
    compileOnly("org.duckdb:duckdb_jdbc:1.1.3")
    testImplementation("org.duckdb:duckdb_jdbc:1.1.3")
    
    annotationProcessor("org.immutables:value")
    compileOnly("org.immutables:value-annotations")
    compileOnly("com.google.code.findbugs:jsr305")
    testRuntimeOnly("org.apache.logging.log4j:log4j-slf4j2-impl:2.23.1")
    testRuntimeOnly("org.apache.logging.log4j:log4j-core:2.23.1")
}

fun JavaCompile.configureAnnotationSet(sourceSet: SourceSet) {
    source = sourceSet.java
    classpath = sourceSet.compileClasspath
    options.compilerArgs.add("-proc:only")
    org.gradle.api.plugins.internal.JvmPluginsHelper.configureAnnotationProcessorPath(sourceSet, sourceSet.java, options, project)
    destinationDirectory.set(temporaryDir)

    // only if we aren't running compileJava, since doing twice fails (in some places)
    onlyIf { !project.gradle.taskGraph.hasTask(sourceSet.getCompileTaskName("java")) }
}

val annotationProcessorMain by tasks.registering(JavaCompile::class) {
    configureAnnotationSet(sourceSets.main.get())
}

ide {
    fun generatedSource(compile: TaskProvider<JavaCompile>) {
        project.rootProject.configure<org.gradle.plugins.ide.idea.model.IdeaModel> {
            project {
                settings {
                    taskTriggers {
                        afterSync(compile.get())
                    }
                }
            }
        }
    }

    generatedSource(annotationProcessorMain)
}

tasks.test {
    // Set working directory to build directory for all tests
    workingDir = layout.buildDirectory.get().asFile
    
    testLogging {
        // Show standard out and standard error of the test JVM(s) on the console
        showStandardStreams = true
        
        // Show logging output from tests
        events("passed", "skipped", "failed", "standardOut", "standardError")
        
        // Show more detailed output
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
        showExceptions = true
        showCauses = true
        showStackTraces = true
    }
    
    useJUnitPlatform {
        // By default, run only unit tests for faster feedback
        includeTags("unit")
        // To run all tests: ./gradlew test -PrunAllTests=true
        // To run specific tags: ./gradlew test -PincludeTags=integration,performance
        if (project.hasProperty("runAllTests")) {
            includeTags() // Clear filters to run all tests
        }
        if (project.hasProperty("includeTags")) {
            val tags = project.property("includeTags").toString().split(",")
            includeTags(*tags.toTypedArray())
        }
        if (project.hasProperty("excludeTags")) {
            val tags = project.property("excludeTags").toString().split(",")
            excludeTags(*tags.toTypedArray())
        }
    }
    jvmArgs(
        "--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED",
        "--add-opens=java.base/java.nio=org.apache.arrow.memory.netty,ALL-UNNAMED",
        "--add-modules=jdk.incubator.vector"
    )
}

// Task to print test classpath for performance test script
tasks.register("printTestClasspath") {
    doLast {
        println(sourceSets.test.get().runtimeClasspath.asPath)
    }
}
