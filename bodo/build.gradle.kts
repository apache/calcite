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
import com.github.autostyle.gradle.AutostyleTask

plugins {
    id("com.github.vlsi.ide")
    calcite.fmpp
    calcite.javacc
}

dependencies {
    api(project(":core"))
    api("org.apache.calcite.avatica:avatica-core")

    implementation("com.google.guava:guava")
    implementation("org.slf4j:slf4j-api")

    testImplementation("net.hydromatic:quidem")
    testImplementation("net.hydromatic:scott-data-hsqldb")
    testImplementation("org.hsqldb:hsqldb")
    testImplementation("org.incava:java-diff")
    testImplementation(project(":testkit"))

    testRuntimeOnly("org.apache.logging.log4j:log4j-slf4j-impl")
}

val fmppMain by tasks.registering(org.apache.calcite.buildtools.fmpp.FmppTask::class) {
    inputs.dir("src/main/codegen")
    config.set(file("src/main/codegen/config.fmpp"))
    templates.set(file("$rootDir/core/src/main/codegen/templates"))
}

val javaCCMain by tasks.registering(org.apache.calcite.buildtools.javacc.JavaCCTask::class) {
    dependsOn(fmppMain)
    // NOTE: This sets the global lookahead of the parser to 2 for Bodo.
    // I'm copying this into the Bodo parser because:
    // 1. We have already hard copied a lot of parser code from Babel into the core parser
    //    (and may continue to)
    // 2. Babel requires a global lookahead of 2
    // 3. The stuff that we've copied may assume a global lookahead of 2
    //
    //
    // We have not yet seen any parsing issues with the copied parser code in the core parser,
    // but that doesn't
    // mean it doesn't exist, so I'm going to set this to a lookahead of 2 as a safety measure
    // until we determine the original
    // reason Babel requires a 2 token lookahead by default.
    // This may make parsing slightly slower,
    // but I expect this to be negligible since the time spent in Calcite/BodoSQL
    // is still very small relative to the spent compiling in Bodo.
    //
    lookAhead.set(2)
    val parserFile = fmppMain.map {
        it.output.asFileTree.matching { include("**/Parser.jj") }
    }
    inputFile.from(parserFile)
    packageName.set("org.apache.calcite.sql.parser.bodo")
}

tasks.withType<Checkstyle>().matching { it.name == "checkstyleMain" }
    .configureEach {
        mustRunAfter(javaCCMain)
    }

tasks.withType<AutostyleTask>().configureEach {
    mustRunAfter(javaCCMain)
}

ide {
    fun generatedSource(javacc: TaskProvider<org.apache.calcite.buildtools.javacc.JavaCCTask>, sourceSet: String) =
        generatedJavaSources(javacc.get(), javacc.get().output.get().asFile, sourceSets.named(sourceSet))

    generatedSource(javaCCMain, "main")
}
