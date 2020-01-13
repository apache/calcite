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

package org.apache.calcite.buildtools.javacc

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.kotlin.dsl.withType

open class JavaCCPlugin : Plugin<Project> {
    companion object {
        const val JAVACC_CLASSPATH_CONFIGURATION_NAME = "javaccClaspath"
        const val GENERATE_SOURCES_TASK_NAME = "generateSources"
    }

    override fun apply(target: Project) {
        target.configureJavaCC()
    }

    fun Project.configureJavaCC() {
        configurations.create(JAVACC_CLASSPATH_CONFIGURATION_NAME) {
            isCanBeConsumed = false
        }.defaultDependencies {
            // TODO: use properties for versions
            add(dependencies.create("net.java.dev.javacc:javacc:4.0")) // 7.0.5"))
        }

        tasks.register(GENERATE_SOURCES_TASK_NAME) {
            description = "Generates sources (e.g. JavaCC)"
            dependsOn(tasks.withType<JavaCCTask>())
        }
    }
}
