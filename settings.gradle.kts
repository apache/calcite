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

        idv("com.autonomousapps.dependency-analysis")
        idv("org.checkerframework")
        idv("com.github.autostyle")
        idv("com.github.burrunan.s3-build-cache")
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
        idv("me.champeau.gradle.jmh")
        idv("net.ltgt.errorprone")
        idv("org.jetbrains.gradle.plugin.idea-ext")
        idv("org.nosphere.apache.rat")
        idv("org.owasp.dependencycheck")
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
    `gradle-enterprise`
    id("com.github.burrunan.s3-build-cache")
}

// This is the name of a current project
// Note: it cannot be inferred from the directory name as developer might clone Calcite to calcite_tmp folder
rootProject.name = "calcite"

include(
    "bom",
    "release",
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

if (isCiServer) {
    gradleEnterprise {
        buildScan {
            termsOfServiceUrl = "https://gradle.com/terms-of-service"
            termsOfServiceAgree = "yes"
            tag("CI")
        }
    }
}

// Cache build artifacts, so expensive operations do not need to be re-computed
// The logic is as follows:
//  1. Cache is populated only in CI that has S3_BUILD_CACHE_ACCESS_KEY_ID and
//     S3_BUILD_CACHE_SECRET_KEY (GitHub Actions in main branch)
//  2. Otherwise the cache is read-only (e.g. everyday builds and PR builds)
buildCache {
    local {
        isEnabled = !isCiServer
    }
    if (property("s3.build.cache")?.ifBlank { "true" }?.toBoolean() == true) {
        val pushAllowed = property("s3.build.cache.push")?.ifBlank { "true" }?.toBoolean() ?: true
        remote<com.github.burrunan.s3cache.AwsS3BuildCache> {
            region = "us-east-2"
            bucket = "calcite-gradle-cache"
            endpoint = "s3.us-east-2.wasabisys.com"
            isPush = isCiServer && pushAllowed && !awsAccessKeyId.isNullOrBlank()
        }
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
