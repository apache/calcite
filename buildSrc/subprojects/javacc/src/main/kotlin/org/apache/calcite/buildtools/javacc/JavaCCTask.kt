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

import java.io.File
import javax.inject.Inject
import org.gradle.api.DefaultTask
import org.gradle.api.artifacts.Configuration
import org.gradle.api.model.ObjectFactory
import org.gradle.api.tasks.Classpath
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.Internal
import org.gradle.api.tasks.OutputDirectory
import org.gradle.api.tasks.TaskAction
import org.gradle.kotlin.dsl.property

open class JavaCCTask @Inject constructor(
    objectFactory: ObjectFactory
) : DefaultTask() {
    @Classpath
    val javaCCClasspath = objectFactory.property<Configuration>()
        .convention(project.configurations.named(JavaCCPlugin.JAVACC_CLASSPATH_CONFIGURATION_NAME))

    @Internal
    val inputFile = objectFactory.property<File>()

    // See https://github.com/gradle/gradle/issues/12627
    @get:InputFile
    val actualInputFile: File? get() = try {
        inputFile.get()
    } catch (e: IllegalStateException) {
        // This means Gradle queries property too early
        null
    }

    @Input
    val lookAhead = objectFactory.property<Int>().convention(1)

    @Input
    val static = objectFactory.property<Boolean>().convention(false)

    @OutputDirectory
    val output = objectFactory.directoryProperty()
        .convention(project.layout.buildDirectory.dir("javacc/$name"))

    @Input
    val packageName = objectFactory.property<String>()

    @TaskAction
    fun run() {
        project.delete(output.asFileTree)
        project.javaexec {
            classpath = javaCCClasspath.get()
            // The class is in the top-level package
            main = "javacc"
            args("-STATIC=${static.get()}")
            args("-LOOKAHEAD:${lookAhead.get()}")
            args("-OUTPUT_DIRECTORY:${output.get()}/${packageName.get().replace('.', '/')}")
            args(inputFile.get())
        }
    }
}
