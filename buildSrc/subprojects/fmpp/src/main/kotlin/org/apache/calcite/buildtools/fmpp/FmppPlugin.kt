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

package org.apache.calcite.buildtools.fmpp

import org.gradle.api.Plugin
import org.gradle.api.Project

open class FmppPlugin : Plugin<Project> {
    companion object {
        const val FMPP_CLASSPATH_CONFIGURATION_NAME = "fmppClaspath"
    }

    override fun apply(target: Project) {
        target.configureFmpp()
    }

    fun Project.configureFmpp() {
        configurations.create(FMPP_CLASSPATH_CONFIGURATION_NAME) {
            isCanBeConsumed = false
        }.defaultDependencies {
            // TODO: use properties for versions
            add(dependencies.create("org.freemarker:freemarker:2.3.29"))
            add(dependencies.create("net.sourceforge.fmpp:fmpp:0.9.16"))
        }
    }
}
