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

package org.apache.calcite.buildtools.asmchecker

import java.nio.file.Files
import java.nio.file.Paths
import org.gradle.api.DefaultTask
import org.gradle.api.tasks.CacheableTask
import org.gradle.api.tasks.TaskAction
import org.objectweb.asm.ClassReader
import org.objectweb.asm.ClassWriter
import org.objectweb.asm.commons.ClassRemapper
import org.objectweb.asm.commons.Remapper
import org.objectweb.asm.util.CheckClassAdapter

@CacheableTask
open class AsmCheckerTask : DefaultTask() {

    @TaskAction
    fun run() {
        project.buildDir.walk().filter { file -> file.getName().toLowerCase().endsWith(".class") }
            .forEach {
                val classReader = ClassReader(Files.readAllBytes(Paths.get(it.getPath())))
                val classVisitor = CheckClassAdapter(ClassWriter(ClassWriter.COMPUTE_MAXS))
                val classRemapper = ClassRemapper(classVisitor, object : Remapper() {})
                try {
                    classReader.accept(classRemapper, ClassReader.EXPAND_FRAMES)
                } catch (e: java.lang.RuntimeException) {
                    throw java.lang.RuntimeException("Invalid bytecode file:" + it, e)
                }
            }
    }
}
