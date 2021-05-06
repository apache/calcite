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
    calcite.fmpp
    calcite.javacc
    id("com.github.vlsi.ide")
}

dependencies {
    api(project(":core"))
    api("org.apache.calcite.avatica:avatica-core")

    implementation(project(":linq4j"))
    implementation("org.checkerframework:checker-qual")
    implementation("com.google.guava:guava")
    implementation("org.slf4j:slf4j-api")

    testImplementation(project(":core", "testClasses"))
    testImplementation("net.hydromatic:quidem")
    testImplementation("net.hydromatic:scott-data-hsqldb")
    testImplementation("org.hsqldb:hsqldb")
    testImplementation("org.incava:java-diff")
    testRuntimeOnly("org.slf4j:slf4j-log4j12")
}

val fmppMain by tasks.registering(org.apache.calcite.buildtools.fmpp.FmppTask::class) {
    inputs.dir("src/main/codegen")
    config.set(file("src/main/codegen/config.fmpp"))
    templates.set(file("$rootDir/core/src/main/codegen/templates"))
}

val javaCCMain by tasks.registering(org.apache.calcite.buildtools.javacc.JavaCCTask::class) {
    dependsOn(fmppMain)
    val parserFile = fmppMain.map {
        it.output.asFileTree.matching { include("**/Parser.jj") }
    }
    inputFile.from(parserFile)
    packageName.set("org.apache.calcite.sql.parser.ddl")
}

ide {
    fun generatedSource(javacc: TaskProvider<org.apache.calcite.buildtools.javacc.JavaCCTask>, sourceSet: String) =
        generatedJavaSources(javacc.get(), javacc.get().output.get().asFile, sourceSets.named(sourceSet))

    generatedSource(javaCCMain, "main")
}
