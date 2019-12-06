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

import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    java
    `kotlin-dsl` apply false
    id("com.diffplug.gradle.spotless")
}

repositories {
    jcenter()
    gradlePluginPortal()
}

subprojects {
    repositories {
        jcenter()
        gradlePluginPortal()
    }
    applyKotlinProjectConventions()
}

fun Project.applyKotlinProjectConventions() {
    apply(plugin = "org.gradle.kotlin.kotlin-dsl")

    plugins.withType<KotlinDslPlugin> {
        configure<KotlinDslPluginOptions> {
            experimentalWarning.set(false)
        }
    }

    tasks.withType<KotlinCompile> {
        sourceCompatibility = "unused"
        targetCompatibility = "unused"
        kotlinOptions {
            jvmTarget = "1.8"
        }
    }
    apply(plugin = "com.diffplug.gradle.spotless")
    spotless {
        kotlin {
            ktlint()
            trimTrailingWhitespace()
            endWithNewline()
        }
        kotlinGradle {
            ktlint()
            trimTrailingWhitespace()
            endWithNewline()
        }
    }
}

dependencies {
    subprojects.forEach {
        runtimeOnly(project(it.path))
    }
}
