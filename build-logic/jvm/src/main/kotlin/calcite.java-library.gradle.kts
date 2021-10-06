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
import buildlogic.filterEolSimple

plugins {
    `java-library`
    id("calcite.java")
    id("calcite.testing")
}

tasks.withType<Javadoc>().configureEach {
    excludes.add("org/testng/internal/**")
}

tasks.withType<JavaCompile>().configureEach {
    inputs.property("java.version", System.getProperty("java.version"))
    inputs.property("java.vm.version", System.getProperty("java.vm.version"))
    options.apply {
        encoding = "UTF-8"
        compilerArgs.add("-Xlint:deprecation")
        compilerArgs.add("-Werror")
    }
}

tasks.withType<Jar>().configureEach {
    into("META-INF") {
        filterEolSimple("crlf")
        from("$rootDir/LICENSE.txt")
        from("$rootDir/NOTICE")
        duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    }
    manifest {
        // providers.gradleProperty does not work
        // see https://github.com/gradle/gradle/issues/14972
        val name = rootProject.findProperty("project.name")
        val vendor = rootProject.findProperty("project.vendor.name")
        attributes(
            mapOf(
                "Specification-Title" to name,
                "Specification-Version" to project.version,
                "Specification-Vendor" to vendor,
                "Implementation-Title" to name,
                "Implementation-Version" to project.version,
                "Implementation-Vendor" to vendor,
                "Implementation-Vendor-Id" to rootProject.findProperty("project.vendor.id"),
                "Implementation-Url" to rootProject.findProperty("project.url"),
            )
        )
    }
}

@Suppress("unused")
val transitiveSourcesElements by configurations.creating {
    description = "Share sources folder with other projects for aggregation (e.g. sources, javadocs, etc)"
    isVisible = false
    isCanBeResolved = false
    isCanBeConsumed = true
    extendsFrom(configurations.implementation.get())
    attributes {
        attribute(Usage.USAGE_ATTRIBUTE, objects.named(Usage.JAVA_RUNTIME))
        attribute(Category.CATEGORY_ATTRIBUTE, objects.named(Category.DOCUMENTATION))
        attribute(DocsType.DOCS_TYPE_ATTRIBUTE, objects.named("source-folders"))
    }
    // afterEvaluate is to allow creation of the new source sets
    afterEvaluate {
        sourceSets.main.get().java.srcDirs.forEach { outgoing.artifact(it) }
    }
}
