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
plugins {
    calcite.javacc
    id("com.github.vlsi.ide")
}

dependencies {
    api(project(":core"))
    api(project(":linq4j"))

    implementation("com.google.guava:guava")
    implementation("org.apache.calcite.avatica:avatica-core")
    implementation("org.apache.hadoop:hadoop-common")
    implementation("org.apache.pig:pig::h2")
    implementation("org.checkerframework:checker-qual")
    implementation("org.slf4j:slf4j-api")

    testImplementation(project(":core", "testClasses"))
    testImplementation("net.hydromatic:scott-data-hsqldb")
    testImplementation("org.apache.hadoop:hadoop-client")
    testImplementation("org.hsqldb:hsqldb")
    testRuntimeOnly("org.slf4j:slf4j-log4j12")
}

val javaCCMain by tasks.registering(org.apache.calcite.buildtools.javacc.JavaCCTask::class) {
    inputFile.from(file("src/main/javacc/PigletParser.jj"))
    packageName.set("org.apache.calcite.piglet.parser")
}

ide {
    fun generatedSource(javacc: TaskProvider<org.apache.calcite.buildtools.javacc.JavaCCTask>, sourceSet: String) =
        generatedJavaSources(javacc.get(), javacc.get().output.get().asFile, sourceSets.named(sourceSet))

    generatedSource(javaCCMain, "main")
}
