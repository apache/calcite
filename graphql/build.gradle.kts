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
import org.gradle.api.tasks.SourceSet

plugins {
    id("com.github.vlsi.ide")
    id("java-library")
}

dependencies {

    implementation("org.apache.logging.log4j:log4j-core:2.14.1")
    implementation("org.apache.logging.log4j:log4j-api:2.14.1")
    testImplementation("org.apache.logging.log4j:log4j-core:2.14.1")
    testImplementation("org.apache.logging.log4j:log4j-api:2.14.1")

    api(project(":core"))
    api(project(":linq4j"))
    implementation("org.apache.calcite.avatica:avatica-core")

    implementation("com.graphql-java:graphql-java:22.3")
    implementation("com.google.code.gson:gson:2.11.0")

    // Jackson dependencies
    implementation("com.fasterxml.jackson.core:jackson-core:2.15.2")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.15.2")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jdk8:2.15.2")
    implementation("com.fasterxml.jackson.core:jackson-annotations:2.15.2")

    implementation("com.squareup.okhttp3:okhttp:4.12.0")
    implementation("com.graphql-java-kickstart:graphql-java-tools:11.0.1") // for graphql
    // https://mvnrepository.com/artifact/io.lettuce/lettuce-core
    implementation("io.lettuce:lettuce-core:6.1.5.RELEASE")

    testImplementation("org.junit.jupiter:junit-jupiter-api:5.9.2")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.9.2")

    annotationProcessor("org.immutables:value")
    compileOnly("org.immutables:value-annotations")
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

tasks.withType<JavaCompile> {
    options.annotationProcessorPath = configurations.annotationProcessor.get().asFileTree
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

tasks.withType<Checkstyle> {
    enabled = false
}
