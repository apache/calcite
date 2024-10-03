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
import com.github.vlsi.gradle.ide.dsl.settings
import com.github.vlsi.gradle.ide.dsl.taskTriggers

plugins {
    calcite.javacc
    id("com.github.vlsi.ide")
}

dependencies {
    api(project(":core"))
    api(project(":linq4j"))
    api("com.google.guava:guava")
//    api("org.apache.pig:pig::h2")

    implementation("org.apache.calcite.avatica:avatica-core")
    implementation("org.apache.hadoop:hadoop-common:3.3.6")
    implementation("org.checkerframework:checker-qual")
    implementation("org.slf4j:slf4j-api")

    testImplementation(project(":testkit"))
    testImplementation("net.hydromatic:scott-data-hsqldb")
    testImplementation("org.apache.hadoop:hadoop-client:3.3.6")
    testImplementation("org.hsqldb:hsqldb::jdk8")
    testRuntimeOnly("org.slf4j:slf4j-log4j12")
    annotationProcessor("org.immutables:value")
    compileOnly("org.immutables:value-annotations")
    compileOnly("com.google.code.findbugs:jsr305")
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
    dependsOn(javaCCMain)
    configureAnnotationSet(sourceSets.main.get())
}

tasks.withType<Checkstyle>().matching { it.name == "checkstyleMain" }
    .configureEach {
        mustRunAfter(javaCCMain)
    }

tasks.withType<AutostyleTask>().configureEach {
    mustRunAfter(javaCCMain)
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
