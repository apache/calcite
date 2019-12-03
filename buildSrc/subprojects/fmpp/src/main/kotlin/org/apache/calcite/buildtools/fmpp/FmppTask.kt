/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.calcite.buildtools.fmpp

import javax.inject.Inject
import org.gradle.api.DefaultTask
import org.gradle.api.artifacts.Configuration
import org.gradle.api.model.ObjectFactory
import org.gradle.api.tasks.Classpath
import org.gradle.api.tasks.InputDirectory
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.OutputDirectory
import org.gradle.api.tasks.TaskAction
import org.gradle.kotlin.dsl.property
import org.gradle.kotlin.dsl.withGroovyBuilder

open class FmppTask @Inject constructor(
    objectFactory: ObjectFactory
) : DefaultTask() {
    @Classpath
    val fmppClasspath = objectFactory.property<Configuration>()
        .convention(project.configurations.named(FmppPlugin.FMPP_CLASSPATH_CONFIGURATION_NAME))

    @InputFile
    val config = objectFactory.fileProperty()

    @InputDirectory
    val templates = objectFactory.directoryProperty()

    @OutputDirectory
    val output = objectFactory.directoryProperty()
        .convention(project.layout.buildDirectory.dir("fmpp/$name"))

    @TaskAction
    fun run() {
        project.delete(output.asFileTree)
        ant.withGroovyBuilder {
            "taskdef"("name" to "fmpp",
                "classname" to "fmpp.tools.AntTask",
                "classpath" to fmppClasspath.get().asPath)
            "fmpp"(
                "configuration" to config.get(),
                "sourceRoot" to templates.get().asFile,
                "outputRoot" to output.get().asFile
            )
        }
    }
}
