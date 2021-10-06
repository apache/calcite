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
import org.gradle.api.tasks.testing.Test
import org.gradle.api.tasks.testing.logging.TestExceptionFormat

plugins {
    `java-library`
}

dependencies {
    // TODO: add junit5 by default
}

tasks.withType<Test>().configureEach {
    outputs.cacheIf("test results depend on the database configuration, so we souldn't cache it") {
        false
    }
    useJUnitPlatform {
        excludeTags("slow")
    }
    // Test JVM is forked, and we can pass extra JVM arguments via -Ptest.calcite.extra.jvmargs=...
    providers.gradleProperty("test.calcite.extra.jvmargs")
        .forUseAtConfigurationTime()
        .orNull?.toString()?.trim()
        ?.takeIf { it.isNotEmpty() }
        ?.let {
            // TODO: support quoted arguments
            jvmArgs(it.split(Regex("\\s+")))
        }
    testLogging {
        exceptionFormat = TestExceptionFormat.FULL
        showStandardStreams = true
    }
    exclude("**/*Suite*")
    jvmArgs("-Xmx1536m")
    jvmArgs("-Djdk.net.URLClassPath.disableClassPathURLCheck=true")
    // Pass the property to tests
    fun passProperty(name: String, default: String? = null) {
        val value = System.getProperty(name) ?: default
        value?.let { systemProperty(name, it) }
    }
    passProperty("java.awt.headless")
    passProperty("junit.jupiter.execution.parallel.enabled", "true")
    passProperty("junit.jupiter.execution.parallel.mode.default", "concurrent")
    passProperty("junit.jupiter.execution.timeout.default", "5 m")
    passProperty("user.language", "TR")
    passProperty("user.country", "tr")
    passProperty("user.timezone", "UTC")
    val props = System.getProperties()
    for (e in props.propertyNames() as `java.util`.Enumeration<String>) {
        if (e.startsWith("calcite.") || e.startsWith("avatica.")) {
            passProperty(e)
        }
    }
}
