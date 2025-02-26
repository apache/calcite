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
pluginManagement {
    plugins {
        fun String.v() = extra["$this.version"].toString()
        fun PluginDependenciesSpec.idv(id: String, key: String = id) = id(id) version key.v()

        idv("com.gradle.develocity")
        idv("com.gradle.common-custom-user-data-gradle-plugin")
        idv("com.autonomousapps.dependency-analysis")
        idv("org.checkerframework")
        idv("com.github.autostyle")
        idv("com.github.johnrengelman.shadow")
        idv("com.github.spotbugs")
        idv("com.github.vlsi.crlf", "com.github.vlsi.vlsi-release-plugins")
        idv("com.github.vlsi.gradle-extensions", "com.github.vlsi.vlsi-release-plugins")
        idv("com.github.vlsi.ide", "com.github.vlsi.vlsi-release-plugins")
        idv("com.github.vlsi.jandex", "com.github.vlsi.vlsi-release-plugins")
        idv("com.github.vlsi.license-gather", "com.github.vlsi.vlsi-release-plugins")
        idv("com.github.vlsi.stage-vote-release", "com.github.vlsi.vlsi-release-plugins")
        idv("com.google.protobuf")
        idv("de.thetaphi.forbiddenapis")
        idv("jacoco")
        idv("me.champeau.jmh")
        idv("net.ltgt.errorprone")
        idv("org.jetbrains.gradle.plugin.idea-ext")
        idv("org.nosphere.apache.rat")
        idv("org.owasp.dependencycheck")
        idv("org.sonarqube")
        kotlin("jvm") version "kotlin".v()
    }
    if (extra.has("enableMavenLocal") && extra["enableMavenLocal"].toString().ifBlank { "true" }.toBoolean()) {
        repositories {
            mavenLocal()
            gradlePluginPortal()
        }
    }
}

plugins {
    id("com.gradle.develocity")
    id("com.gradle.common-custom-user-data-gradle-plugin")
}

// This is the name of a current project
// Note: it cannot be inferred from the directory name as developer might clone Calcite to calcite_tmp folder
rootProject.name = "calcite"

include(
    "bom",
    "release",
    "arrow",
    "babel",
    "cassandra",
    "core",
    "druid",
    "elasticsearch",
    "example:csv",
    "example:function",
    "file",
    "geode",
    "innodb",
    "kafka",
    "linq4j",
    "mongodb",
    "pig",
    "piglet",
    "plus",
    "redis",
    "server",
    "spark",
    "splunk",
    "testkit",
    "ubenchmark"
)

// See https://github.com/gradle/gradle/issues/1348#issuecomment-284758705 and
// https://github.com/gradle/gradle/issues/5321#issuecomment-387561204
// Gradle inherits Ant "default excludes", however we do want to archive those files
org.apache.tools.ant.DirectoryScanner.removeDefaultExclude("**/.gitattributes")
org.apache.tools.ant.DirectoryScanner.removeDefaultExclude("**/.gitignore")

fun property(name: String) =
    when (extra.has(name)) {
        true -> extra.get(name) as? String
        else -> null
    }

val isCiServer = System.getenv().containsKey("CI")

develocity {
    server = "https://develocity.apache.org"
    projectId = "calcite"

    buildScan {
        uploadInBackground = !isCiServer
        publishing.onlyIf { it.isAuthenticated() }
        obfuscation {
            ipAddresses { addresses -> addresses.map { "0.0.0.0" } }
        }
    }
}

// Cache build artifacts, so expensive operations do not need to be re-computed
buildCache {
    local {
        isEnabled = !isCiServer
    }
}

// This enables to use local clone of vlsi-release-plugins for debugging purposes
property("localReleasePlugins")?.ifBlank { "../vlsi-release-plugins" }?.let {
    println("Importing project '$it'")
    includeBuild(it)
}

// This enables to open both Calcite and Calcite Avatica as a single project
property("localAvatica")?.ifBlank { "../calcite-avatica" }?.let {
    println("Importing project '$it'")
    includeBuild(it)
}

// This enables to try local Autostyle
property("localAutostyle")?.ifBlank { "../autostyle" }?.let {
    println("Importing project '$it'")
    includeBuild("../autostyle")
}
