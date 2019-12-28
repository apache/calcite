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
import com.github.vlsi.gradle.crlf.CrLfSpec
import com.github.vlsi.gradle.crlf.LineEndings

plugins {
    kotlin("jvm")
    id("com.github.vlsi.crlf")
    id("com.github.vlsi.ide")
    calcite.fmpp
    calcite.javacc
}

val integrationTestConfig: (Configuration.() -> Unit) = {
    isCanBeConsumed = false
    isTransitive = true
    extendsFrom(configurations.testRuntimeClasspath.get())
}

val testH2 by configurations.creating(integrationTestConfig)
val testOracle by configurations.creating(integrationTestConfig)
val testPostgresql by configurations.creating(integrationTestConfig)
val testMysql by configurations.creating(integrationTestConfig)

dependencies {
    api(project(":linq4j"))

    api("com.fasterxml.jackson.core:jackson-annotations")
    api("org.apache.calcite.avatica:avatica-core")

    implementation("com.esri.geometry:esri-geometry-api")
    implementation("com.fasterxml.jackson.core:jackson-core")
    implementation("com.fasterxml.jackson.core:jackson-databind")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml")
    implementation("com.google.code.findbugs:jsr305"/* optional*/)
    implementation("com.google.guava:guava")
    implementation("com.jayway.jsonpath:json-path")
    implementation("com.yahoo.datasketches:sketches-core")
    implementation("commons-codec:commons-codec")
    implementation("net.hydromatic:aggdesigner-algorithm")
    implementation("org.apache.calcite.avatica:avatica-server")
    implementation("org.apache.commons:commons-dbcp2")
    implementation("org.apache.commons:commons-lang3")
    implementation("commons-io:commons-io")
    implementation("org.codehaus.janino:commons-compiler")
    implementation("org.codehaus.janino:janino")
    implementation("org.slf4j:slf4j-api")

    testH2("com.h2database:h2")
    testMysql("mysql:mysql-connector-java")
    testOracle("com.oracle.ojdbc:ojdbc8")
    testPostgresql("org.postgresql:postgresql")

    testImplementation("com.github.stephenc.jcip:jcip-annotations")
    testImplementation("net.hydromatic:foodmart-data-hsqldb")
    testImplementation("net.hydromatic:foodmart-queries")
    testImplementation("net.hydromatic:quidem")
    testImplementation("net.hydromatic:scott-data-hsqldb")
    testImplementation("org.apache.commons:commons-pool2")
    testImplementation("org.hsqldb:hsqldb")
    testImplementation("org.incava:java-diff")
    testImplementation("sqlline:sqlline")
    testImplementation(kotlin("stdlib-jdk8"))
    testImplementation(kotlin("test"))
    testImplementation(kotlin("test-junit5"))
    testRuntimeOnly("org.slf4j:slf4j-log4j12")
}

// There are users that reuse/extend test code (e.g. Apache Felix)
// So publish test jar to Nexus repository
// TODO: remove when calcite-test-framework is extracted to a standalone artifact
publishing {
    publications {
        named<MavenPublication>(project.name) {
            artifact(tasks.testJar.get())
        }
    }
}

tasks.jar {
    CrLfSpec(LineEndings.LF).run {
        into("codegen") {
            textFrom("$projectDir/src/main/codegen")
        }
    }
}

val generatedVersionDir = File(buildDir, "generated/sources/version")
val versionClass by tasks.registering(Sync::class) {
    val re = Regex("^(\\d+)\\.(\\d+).*")

    val version = project.version.toString()
    val matchResult = re.find(version) ?: throw GradleException("Unable to parse major.minor version parts from project.version '$version'")
    val (major, minor) = matchResult.destructured

    // This makes Gradle re-execute the task when version is updated
    inputs.property("version", version)

    // Note: Gradle does not analyze regexps below, so this variable tells Gradle
    // to treat the task input out of date when filtering logic is updated.
    inputs.property("replace.logic.version.bump.when.updating.filter.below", 1)

    outputs.dir(generatedVersionDir)

    into(generatedVersionDir)
    from("$projectDir/src/main/version") {
        include("**/*.java")
        val prop = Regex("""("[^"]++"|\S+)\s+/\* :(\w+) \*/""")
        filter { x: String ->
            prop.replace(x) {
                val variableName = it.groups[2]?.value
                when (variableName) {
                    "version" -> "\"$version\""
                    "major" -> major
                    "minor" -> minor
                    else -> "unknown variable: $x"
                } + """ /* :$variableName */"""
            }
        }
    }
}

ide {
    generatedJavaSources(versionClass.get(), generatedVersionDir)
}

sourceSets {
    main {
        resources.exclude("version/org-apache-calcite-jdbc.properties")
    }
}

tasks.withType<Checkstyle>().configureEach {
    exclude("org/apache/calcite/runtime/Resources.java")
}

val fmppMain by tasks.registering(org.apache.calcite.buildtools.fmpp.FmppTask::class) {
    config.set(file("src/main/codegen/config.fmpp"))
    templates.set(file("src/main/codegen/templates"))
}

val javaCCMain by tasks.registering(org.apache.calcite.buildtools.javacc.JavaCCTask::class) {
    dependsOn(fmppMain)
    val parserFile = fmppMain.map {
        it.output.asFileTree.matching { include("**/Parser.jj") }.singleFile
    }
    inputFile.set(parserFile)
    packageName.set("org.apache.calcite.sql.parser.impl")
}

val fmppTest by tasks.registering(org.apache.calcite.buildtools.fmpp.FmppTask::class) {
    config.set(file("src/test/codegen/config.fmpp"))
    templates.set(file("src/main/codegen/templates"))
}

val javaCCTest by tasks.registering(org.apache.calcite.buildtools.javacc.JavaCCTask::class) {
    dependsOn(fmppTest)
    val parserFile = fmppTest.map {
        it.output.asFileTree.matching { include("**/Parser.jj") }.singleFile
    }
    inputFile.set(parserFile)
    packageName.set("org.apache.calcite.sql.parser.parserextensiontesting")
}

ide {
    fun generatedSource(javacc: TaskProvider<org.apache.calcite.buildtools.javacc.JavaCCTask>, sourceSet: String) =
        generatedJavaSources(javacc.get(), javacc.get().output.get().asFile, sourceSets.named(sourceSet))

    generatedSource(javaCCMain, "main")
    generatedSource(javaCCTest, "test")
}

val integTestAll by tasks.registering() {
    group = LifecycleBasePlugin.VERIFICATION_GROUP
    description = "Executes integration JDBC tests for all DBs"
}

val coreTestClasses = sourceSets.main.get().output
val coreClasses = sourceSets.main.get().output + coreTestClasses
for (db in listOf("h2", "mysql", "oracle", "postgresql")) {
    val task = tasks.register("integTest" + db.capitalize(), Test::class) {
        group = LifecycleBasePlugin.VERIFICATION_GROUP
        description = "Executes integration JDBC tests with $db database"
        include("org/apache/calcite/test/JdbcAdapterTest.class")
        include("org/apache/calcite/test/JdbcTest.class")
        systemProperty("calcite.test.db", db)
        testClassesDirs = coreTestClasses.classesDirs
        classpath = coreClasses + configurations.getAt("test" + db.capitalize())
    }
    integTestAll {
        dependsOn(task)
    }
}
