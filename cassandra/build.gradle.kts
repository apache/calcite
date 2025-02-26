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

    api("org.apache.cassandra:java-driver-core")
    api("com.google.guava:guava")
    api("org.slf4j:slf4j-api")

    implementation("org.apache.calcite.avatica:avatica-core")

    testImplementation(project(":testkit"))
    testImplementation("org.apache.cassandra:cassandra-all") {
        exclude("org.slf4j", "log4j-over-slf4j")
            .because("log4j is already present in the classpath")
        exclude("ch.qos.logback", "logback-core")
            .because("conflicts with log4j-slf4j-impl")
        exclude("ch.qos.logback", "logback-classic")
            .because("conflicts with log4j-slf4j-impl")
    }
    testImplementation("org.cassandraunit:cassandra-unit") {
        exclude("ch.qos.logback", "logback-core")
            .because("conflicts with log4j-slf4j-impl")
        exclude("ch.qos.logback", "logback-classic")
            .because("conflicts with log4j-slf4j-impl")
    }
    testRuntimeOnly("net.java.dev.jna:jna")

    annotationProcessor("org.immutables:value")
    compileOnly("org.immutables:value-annotations")
    compileOnly("com.google.code.findbugs:jsr305")
    testRuntimeOnly("org.apache.logging.log4j:log4j-slf4j-impl")
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
    // generate annotation processed files on project import/sync.
    // adds to idea path but skip don't add to SourceSet since that triggers checkstyle
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
