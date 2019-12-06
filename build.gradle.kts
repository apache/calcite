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

import com.github.spotbugs.SpotBugsTask
import com.github.vlsi.gradle.crlf.CrLfSpec
import com.github.vlsi.gradle.crlf.LineEndings
import com.github.vlsi.gradle.git.FindGitAttributes
import com.github.vlsi.gradle.git.dsl.gitignore
import com.github.vlsi.gradle.properties.dsl.lastEditYear
import com.github.vlsi.gradle.properties.dsl.props
import com.github.vlsi.gradle.release.RepositoryType
import de.thetaphi.forbiddenapis.gradle.CheckForbiddenApis
import de.thetaphi.forbiddenapis.gradle.CheckForbiddenApisExtension
import org.gradle.api.tasks.testing.logging.TestExceptionFormat

plugins {
    publishing
    // Verification
    checkstyle
    id("com.diffplug.gradle.spotless")
    id("org.nosphere.apache.rat")
    id("com.github.spotbugs")
    id("de.thetaphi.forbiddenapis") apply false
    id("org.owasp.dependencycheck")
    id("com.github.johnrengelman.shadow") apply false
    // IDE configuration
    id("org.jetbrains.gradle.plugin.idea-ext")
    id("com.github.vlsi.ide")
    // Release
    id("com.github.vlsi.crlf")
    id("com.github.vlsi.gradle-extensions")
    id("com.github.vlsi.license-gather") apply false
    id("com.github.vlsi.stage-vote-release")
}

repositories {
    // At least for RAT
    mavenCentral()
}

fun reportsForHumans() = !(System.getenv()["CI"]?.toBoolean() ?: false)

val lastEditYear by extra(lastEditYear())

// Do not enable spotbugs by default. Execute it only when -Pspotbugs is present
val enableSpotBugs = props.bool("spotbugs")
val skipCheckstyle by props()
val skipSpotless by props()
val skipJavadoc by props()
val enableMavenLocal by props()
val enableGradleMetadata by props()
// By default use Java implementation to sign artifacts
// When useGpgCmd=true, then gpg command line tool is used for signing
val useGpgCmd by props()
val slowSuiteLogThreshold by props(0L)
val slowTestLogThreshold by props(2000L)

ide {
    copyrightToAsf()
    ideaInstructionsUri =
        uri("https://calcite.apache.org/docs/howto.html#setting-up-intellij-idea")
    doNotDetectFrameworks("android", "jruby")
}

// This task scans the project for gitignore / gitattributes, and that is reused for building
// source/binary artifacts with the appropriate eol/executable file flags
// It enables to automatically exclude patterns from .gitignore
val gitProps by tasks.registering(FindGitAttributes::class) {
    // Scanning for .gitignore and .gitattributes files in a task avoids doing that
    // when distribution build is not required (e.g. code is just compiled)
    root.set(rootDir)
}

val rat by tasks.getting(org.nosphere.apache.rat.RatTask::class) {
    gitignore(gitProps)
    // Note: patterns are in non-standard syntax for RAT, so we use exclude(..) instead of excludeFile
    exclude(rootDir.resolve(".ratignore").readLines())
}

tasks.validateBeforeBuildingReleaseArtifacts {
    dependsOn(rat)
}

val String.v: String get() = rootProject.extra["$this.version"] as String

val buildVersion = "calcite".v + releaseParams.snapshotSuffix

println("Building Apache Calcite $buildVersion")

val isReleaseVersion = rootProject.releaseParams.release.get()

releaseArtifacts {
    fromProject(":release")
}

// Configures URLs to SVN and Nexus
releaseParams {
    tlp.set("Calcite")
    componentName.set("Apache Calcite")
    releaseTag.set("rel/v$buildVersion")
    rcTag.set(rc.map { "v$buildVersion-rc$it" })
    sitePreviewEnabled.set(false)
    nexus {
        // https://github.com/marcphilipp/nexus-publish-plugin/issues/35
        packageGroup.set("org.apache.calcite")
        if (repositoryType.get() == RepositoryType.PROD) {
            // org.apache.calcite at repository.apache.org
            stagingProfileId.set("778fd0d4358bb")
        }
    }
    svnDist {
        staleRemovalFilters {
            includes.add(Regex(".*apache-calcite-\\d.*"))
            validates.empty()
            validates.add(provider {
                Regex("release/calcite/apache-calcite-${version.toString().removeSuffix("-SNAPSHOT")}")
            })
        }
    }
    validateBeforeBuildingReleaseArtifacts += Runnable {
        if (useGpgCmd && findProperty("signing.gnupg.keyName") == null) {
            throw GradleException("Please specify signing key id via signing.gnupg.keyName " +
                    "(see https://github.com/gradle/gradle/issues/8657)")
        }
    }
}

val javadocAggregate by tasks.registering(Javadoc::class) {
    group = JavaBasePlugin.DOCUMENTATION_GROUP
    description = "Generates aggregate javadoc for all the artifacts"

    val sourceSets = allprojects
        .mapNotNull { it.extensions.findByType<SourceSetContainer>() }
        .map { it.named("main") }

    classpath = files(sourceSets.map { set -> set.map { it.output + it.compileClasspath } })
    setSource(sourceSets.map { set -> set.map { it.allJava } })
    setDestinationDir(file("$buildDir/docs/javadocAggregate"))
}

val adaptersForSqlline = listOf(
    ":babel", ":cassandra", ":druid", ":elasticsearch", ":file", ":geode", ":kafka", ":mongodb",
    ":pig", ":piglet", ":plus", ":spark", ":splunk"
)

val sqllineClasspath by configurations.creating {
    isCanBeConsumed = false
}

dependencies {
    sqllineClasspath(platform(project(":bom")))
    sqllineClasspath("sqlline:sqlline")
    for (p in adaptersForSqlline) {
        sqllineClasspath(project(p))
    }
}

val buildSqllineClasspath by tasks.registering(Jar::class) {
    description = "Creates classpath-only jar for running SqlLine"
    // One can debug classpath with ./gradlew dependencies --configuration sqllineClasspath
    inputs.files(sqllineClasspath).withNormalizer(ClasspathNormalizer::class.java)
    archiveFileName.set("sqllineClasspath.jar")
    manifest {
        manifest {
            attributes(
                "Main-Class" to "sqlline.SqlLine",
                "Class-Path" to provider { sqllineClasspath.map { it.absolutePath }.joinToString(" ") }
            )
        }
    }
}

val semaphore = `java.util.concurrent`.Semaphore(1)

val javaccGeneratedPatterns = arrayOf(
    "org/apache/calcite/jdbc/CalciteDriverVersion.java",
    "**/parser/**/*ParserImpl*.*",
    "**/parser/**/PigletParser.*",
    "**/parser/**/PigletParserConstants.*",
    "**/parser/**/ParseException.*",
    "**/parser/**/SimpleCharStream.*",
    "**/parser/**/Token.*",
    "**/parser/**/TokenMgrError.*",
    "**/org/apache/calcite/runtime/Resources.java",
    "**/parser/**/*ParserTokenManager.*"
)

fun PatternFilterable.excludeJavaCcGenerated() {
    exclude(*javaccGeneratedPatterns)
}

allprojects {
    group = "org.apache.calcite"
    version = buildVersion

    val javaUsed = file("src/main/java").isDirectory
    if (javaUsed) {
        apply(plugin = "java-library")
    }

    plugins.withId("java-library") {
        dependencies {
            "implementation"(platform(project(":bom")))
        }
    }

    val hasTests = file("src/test/java").isDirectory || file("src/test/kotlin").isDirectory
    if (hasTests) {
        // Add default tests dependencies
        dependencies {
            val testImplementation by configurations
            val testRuntimeOnly by configurations
            testImplementation("org.junit.jupiter:junit-jupiter-api")
            testImplementation("org.junit.jupiter:junit-jupiter-params")
            testImplementation("org.hamcrest:hamcrest")
            testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
            if (project.props.bool("junit4", default = false)) {
                // Allow projects to opt-out of junit dependency, so they can be JUnit5-only
                testImplementation("junit:junit")
                testRuntimeOnly("org.junit.vintage:junit-vintage-engine")
            }
        }
    }

    if (!skipSpotless) {
        apply(plugin = "com.diffplug.gradle.spotless")
        spotless {
            kotlinGradle {
                ktlint()
                trimTrailingWhitespace()
                endWithNewline()
            }
            if (project == rootProject) {
                // Spotless does not exclude subprojects when using target(...)
                // So **/*.md is enough to scan all the md files in the codebase
                // See https://github.com/diffplug/spotless/issues/468
                format("markdown") {
                    target("**/*.md")
                    // Flot is known to have trailing whitespace, so the files
                    // are kept in their original format (e.g. to simplify diff on library upgrade)
                    trimTrailingWhitespace()
                    endWithNewline()
                }
            }
        }
    }
    if (!skipCheckstyle) {
        apply<CheckstylePlugin>()
        dependencies {
            checkstyle("com.puppycrawl.tools:checkstyle:${"checkstyle".v}")
            checkstyle("net.hydromatic:toolbox:${"hydromatic-toolbox".v}")
        }
        checkstyle {
            // Current one is ~8.8
            // https://github.com/julianhyde/toolbox/issues/3
            isShowViolations = true
            configDirectory.set(File(rootDir, "src/main/config/checkstyle"))
            configFile = configDirectory.get().file("checker.xml").asFile
            configProperties = mapOf(
                "base_dir" to rootDir.toString()
            )
        }
        tasks.register("checkstyleAll") {
            dependsOn(tasks.withType<Checkstyle>())
        }
        tasks.withType<Checkstyle>().configureEach {
            // Excludes here are faster than in suppressions.xml
            // Since here we can completely remove file from the analysis.
            // On the other hand, supporessions.xml still analyzes the file, and
            // then it recognizes it should suppress all the output.
            excludeJavaCcGenerated()
            // There are concurrency issues with Checkstyle 7.8.2
            // It could be in Checkstyle, in CheckstyleAnt task or in Gradle's Checkstyle plugin
            // The bug looks like as if suppression was not working
            doFirst {
                semaphore.acquire()
            }
            doLast {
                semaphore.release()
            }
        }
    }
    if (!skipSpotless || !skipCheckstyle) {
        tasks.register("style") {
            group = LifecycleBasePlugin.VERIFICATION_GROUP
            description = "Formats code (license header, import order, whitespace at end of line, ...) and executes Checkstyle verifications"
            if (!skipSpotless) {
                dependsOn("spotlessApply")
            }
            if (!skipCheckstyle) {
                dependsOn("checkstyleAll")
            }
        }
    }

    tasks.withType<AbstractArchiveTask>().configureEach {
        // Ensure builds are reproducible
        isPreserveFileTimestamps = false
        isReproducibleFileOrder = true
        dirMode = "775".toInt(8)
        fileMode = "664".toInt(8)
    }

    plugins.withType<SigningPlugin> {
        afterEvaluate {
            configure<SigningExtension> {
                val release = rootProject.releaseParams.release.get()
                // Note it would still try to sign the artifacts,
                // however it would fail only when signing a RELEASE version fails
                isRequired = release
                if (useGpgCmd) {
                    useGpgCmd()
                }
            }
        }
    }

    tasks {
        withType<Javadoc>().configureEach {
            excludeJavaCcGenerated()
            (options as StandardJavadocDocletOptions).apply {
                // Please refrain from using non-ASCII chars below since the options are passed as
                // javadoc.options file which is parsed with "default encoding"
                noTimestamp.value = true
                showFromProtected()
                // javadoc: error - The code being documented uses modules but the packages
                // defined in https://docs.oracle.com/javase/9/docs/api/ are in the unnamed module
                source = "1.8"
                docEncoding = "UTF-8"
                charSet = "UTF-8"
                encoding = "UTF-8"
                docTitle = "Apache Calcite ${project.name} API"
                windowTitle = "Apache Calcite ${project.name} API"
                header = "<b>Apache Calcite</b>"
                bottom =
                    "Copyright &copy; 2012-$lastEditYear Apache Software Foundation. All Rights Reserved."
                if (JavaVersion.current() >= JavaVersion.VERSION_1_9) {
                    addBooleanOption("html5", true)
                    links("https://docs.oracle.com/javase/9/docs/api/")
                } else {
                    links("https://docs.oracle.com/javase/8/docs/api/")
                }
            }
        }
    }

    plugins.withType<JavaPlugin> {
        configure<JavaPluginConvention> {
            sourceCompatibility = JavaVersion.VERSION_1_8
            targetCompatibility = JavaVersion.VERSION_1_8
        }

        repositories {
            if (enableMavenLocal) {
                mavenLocal()
            }
            mavenCentral()
        }
        val sourceSets: SourceSetContainer by project

        apply(plugin = "signing")
        apply(plugin = "de.thetaphi.forbiddenapis")
        apply(plugin = "maven-publish")

        if (!enableGradleMetadata) {
            tasks.withType<GenerateModuleMetadata> {
                enabled = false
            }
        }

        if (isReleaseVersion) {
            configure<SigningExtension> {
                // Sign all the publications
                sign(publishing.publications)
            }
        }

        if (!skipSpotless) {
            spotless {
                java {
                    targetExclude(*javaccGeneratedPatterns + "**/test/java/*.java")
                    licenseHeader(rootProject.ide.licenseHeaderJava)
                    if (!project.props.bool("junit4", default = false)) {
                        replace("junit5: Test", "org.junit.Test", "org.junit.jupiter.api.Test")
                        replaceRegex("junit5: Before", "org.junit.Before\\b", "org.junit.jupiter.api.BeforeEach")
                        replace("junit5: BeforeClass", "org.junit.BeforeClass", "org.junit.jupiter.api.BeforeAll")
                        replaceRegex("junit5: After", "org.junit.After\\b", "org.junit.jupiter.api.AfterEach")
                        replace("junit5: AfterClass", "org.junit.AfterClass", "org.junit.jupiter.api.AfterAll")
                        replace("junit5: Ignore", "org.junit.Ignore", "org.junit.jupiter.api.Disabled")
                        replaceRegex("junit5: @Before", "@Before\\b", "@BeforeEach")
                        replace("junit5: @BeforeClass", "@BeforeClass", "@BeforeAll")
                        replaceRegex("junit5: @After", "@After\\b", "@AfterEach")
                        replace("junit5: @AfterClass", "@AfterClass", "@AfterAll")
                        replace("junit5: @Ignore", "@Ignore", "@Disabled")
                        replace("junit5: Assert.assertThat", "org.junit.Assert.assertThat", "org.hamcrest.MatcherAssert.assertThat")
                        replace("junit5: Assert.fail", "org.junit.Assert.fail", "org.junit.jupiter.api.Assertions.fail")
                    }
                    importOrder(
                        "org.apache.calcite.",
                        "org.apache.",
                        "au.com.",
                        "com.",
                        "io.",
                        "mondrian.",
                        "net.",
                        "org.",
                        "scala.",
                        "java",
                        "",
                        "static com.",
                        "static org.apache.calcite.",
                        "static org.apache.",
                        "static org.",
                        "static java",
                        "static "
                    )
                    removeUnusedImports()
                    trimTrailingWhitespace()
                    indentWithSpaces(4)
                    endWithNewline()
                }
            }
        }
        if (enableSpotBugs) {
            apply(plugin = "com.github.spotbugs")
            spotbugs {
                toolVersion = "spotbugs".v
                reportLevel = "high"
                //  excludeFilter = file("$rootDir/src/main/config/spotbugs/spotbugs-filter.xml")
                // By default spotbugs verifies TEST classes as well, and we do not want that
                this.sourceSets = listOf(sourceSets["main"])
            }
            dependencies {
                // Parenthesis are needed here: https://github.com/gradle/gradle/issues/9248
                (constraints) {
                    "spotbugs"("org.ow2.asm:asm:${"asm".v}")
                    "spotbugs"("org.ow2.asm:asm-all:${"asm".v}")
                    "spotbugs"("org.ow2.asm:asm-analysis:${"asm".v}")
                    "spotbugs"("org.ow2.asm:asm-commons:${"asm".v}")
                    "spotbugs"("org.ow2.asm:asm-tree:${"asm".v}")
                    "spotbugs"("org.ow2.asm:asm-util:${"asm".v}")
                }
            }
        }

        configure<CheckForbiddenApisExtension> {
            failOnUnsupportedJava = false
            bundledSignatures.addAll(
                listOf(
                    "jdk-unsafe",
                    "jdk-deprecated",
                    "jdk-non-portable"
                )
            )
            signaturesFiles = files("$rootDir/src/main/config/forbidden-apis/signatures.txt")
        }

        tasks {
            withType<Jar>().configureEach {
                manifest {
                    attributes["Bundle-License"] = "Apache-2.0"
                    attributes["Implementation-Title"] = "Apache Calcite"
                    attributes["Implementation-Version"] = project.version
                    attributes["Specification-Vendor"] = "The Apache Software Foundation"
                    attributes["Specification-Version"] = project.version
                    attributes["Specification-Title"] = "Apache Calcite"
                    attributes["Implementation-Vendor"] = "Apache Software Foundation"
                    attributes["Implementation-Vendor-Id"] = "org.apache.calcite"
                }
            }

            withType<CheckForbiddenApis>().configureEach {
                excludeJavaCcGenerated()
                exclude(
                    "**/org/apache/calcite/adapter/os/Processes${'$'}ProcessFactory.class",
                    "**/org/apache/calcite/adapter/os/OsAdapterTest.class",
                    "**/org/apache/calcite/runtime/Resources${'$'}Inst.class",
                    "**/org/apache/calcite/test/concurrent/ConcurrentTestCommandScript.class",
                    "**/org/apache/calcite/test/concurrent/ConcurrentTestCommandScript${'$'}ShellCommand.class",
                    "**/org/apache/calcite/util/Unsafe.class"
                )
            }

            withType<JavaCompile>().configureEach {
                options.encoding = "UTF-8"
            }
            withType<Test>().configureEach {
                useJUnitPlatform {
                    excludeTags("slow")
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
                passProperty("junit.jupiter.execution.timeout.default", "5 m")
                passProperty("user.language", "TR")
                passProperty("user.country", "tr")
                val props = System.getProperties()
                for (e in props.propertyNames() as `java.util`.Enumeration<String>) {
                    if (e.startsWith("calcite.") || e.startsWith("avatica.")) {
                        passProperty(e)
                    }
                }
                // https://github.com/junit-team/junit5/issues/2041
                // Gradle does not print parameterized test names yet :(
                // Hopefully it will be fixed in Gradle 6.1
                fun String?.withDisplayName(displayName: String?, separator: String = ", "): String? = when {
                    displayName == null -> this
                    this == null -> displayName
                    endsWith(displayName) -> this
                    else -> "$this$separator$displayName"
                }
                fun printResult(descriptor: TestDescriptor, result: TestResult) {
                    val test = descriptor as org.gradle.api.internal.tasks.testing.TestDescriptorInternal
                    val classDisplayName = test.className.withDisplayName(test.classDisplayName)
                    val testDisplayName = test.name.withDisplayName(test.displayName)
                    val duration = "%5.1fsec".format((result.endTime - result.startTime) / 1000f)
                    val displayName = classDisplayName.withDisplayName(testDisplayName, " > ")
                    // Hide SUCCESS from output log, so FAILURE/SKIPPED are easier to spot
                    val resultType = result.resultType
                        .takeUnless { it == TestResult.ResultType.SUCCESS }
                        ?.toString()
                        ?: (if (result.skippedTestCount > 0 || result.testCount == 0L) "WARNING" else "       ")
                    if (!descriptor.isComposite) {
                        println("$resultType $duration, $displayName")
                    } else {
                        val completed = result.testCount.toString().padStart(4)
                        val failed = result.failedTestCount.toString().padStart(3)
                        val skipped = result.skippedTestCount.toString().padStart(3)
                        println("$resultType $duration, $completed completed, $failed failed, $skipped skipped, $displayName")
                    }
                }
                afterTest(KotlinClosure2<TestDescriptor, TestResult, Any>({ descriptor, result ->
                    // There are lots of skipped tests, so it is not clear how to log them
                    // without making build logs too verbose
                    if (result.resultType == TestResult.ResultType.FAILURE ||
                        result.endTime - result.startTime >= slowTestLogThreshold) {
                        printResult(descriptor, result)
                    }
                }))
                afterSuite(KotlinClosure2<TestDescriptor, TestResult, Any>({ descriptor, result ->
                    if (descriptor.name.startsWith("Gradle Test Executor")) {
                        return@KotlinClosure2
                    }
                    if (result.resultType == TestResult.ResultType.FAILURE ||
                        result.endTime - result.startTime >= slowSuiteLogThreshold) {
                        printResult(descriptor, result)
                    }
                }))
            }
            // Cannot be moved above otherwise configure each will override
            // also the specific configurations below.
            register<Test>("testSlow") {
                group = LifecycleBasePlugin.VERIFICATION_GROUP
                description = "Runs the slow unit tests."
                useJUnitPlatform() {
                    includeTags("slow")
                }
                jvmArgs("-Xmx6g")
            }
            withType<SpotBugsTask>().configureEach {
                group = LifecycleBasePlugin.VERIFICATION_GROUP
                if (enableSpotBugs) {
                    description = "$description (skipped by default, to enable it add -Dspotbugs)"
                }
                reports {
                    html.isEnabled = reportsForHumans()
                    xml.isEnabled = !reportsForHumans()
                }
                enabled = enableSpotBugs
            }

            afterEvaluate {
                // Add default license/notice when missing
                withType<Jar>().configureEach {
                    CrLfSpec(LineEndings.LF).run {
                        into("META-INF") {
                            filteringCharset = "UTF-8"
                            duplicatesStrategy = DuplicatesStrategy.EXCLUDE
                            // Note: we need "generic Apache-2.0" text without third-party items
                            // So we use the text from $rootDir/config/ since source distribution
                            // contains altered text at $rootDir/LICENSE
                            textFrom("$rootDir/src/main/config/licenses/LICENSE")
                            textFrom("$rootDir/NOTICE")
                        }
                    }
                }
            }
        }

        // Note: jars below do not normalize line endings.
        // Those jars, however are not included to source/binary distributions
        // so the normailzation is not that important

        val testJar by tasks.registering(Jar::class) {
            from(sourceSets["test"].output)
            archiveClassifier.set("tests")
        }

        val testSourcesJar by tasks.registering(Jar::class) {
            from(sourceSets["test"].allJava)
            archiveClassifier.set("test-sources")
        }

        val sourcesJar by tasks.registering(Jar::class) {
            from(sourceSets["main"].allJava)
            archiveClassifier.set("sources")
        }

        val javadocJar by tasks.registering(Jar::class) {
            from(tasks.named(JavaPlugin.JAVADOC_TASK_NAME))
            archiveClassifier.set("javadoc")
        }

        val testClasses by configurations.creating {
            extendsFrom(configurations["testRuntime"])
        }

        val archives by configurations.getting

        // Parenthesis needed to use Project#getArtifacts
        (artifacts) {
            testClasses(testJar)
            archives(sourcesJar)
            archives(testJar)
            archives(testSourcesJar)
        }

        val archivesBaseName = "calcite-$name"
        setProperty("archivesBaseName", archivesBaseName)

        configure<PublishingExtension> {
            if (project.path == ":") {
                // Do not publish "root" project. Java plugin is applied here for DSL purposes only
                return@configure
            }
            if (!project.props.bool("nexus.publish", default = true)) {
                // Some of the artifacts do not need to be published
                return@configure
            }
            publications {
                create<MavenPublication>(project.name) {
                    artifactId = archivesBaseName
                    version = rootProject.version.toString()
                    description = project.description
                    from(components["java"])

                    if (!skipJavadoc) {
                        // Eager task creation is required due to
                        // https://github.com/gradle/gradle/issues/6246
                        artifact(sourcesJar.get())
                        artifact(javadocJar.get())
                    }

                    // Use the resolved versions in pom.xml
                    // Gradle might have different resolution rules, so we set the versions
                    // that were used in Gradle build/test.
                    versionMapping {
                        usage(Usage.JAVA_RUNTIME) {
                            fromResolutionResult()
                        }
                        usage(Usage.JAVA_API) {
                            fromResolutionOf("runtimeClasspath")
                        }
                    }
                    pom {
                        withXml {
                            val sb = asString()
                            var s = sb.toString()
                            // <scope>compile</scope> is Maven default, so delete it
                            s = s.replace("<scope>compile</scope>", "")
                            // Cut <dependencyManagement> because all dependencies have the resolved versions
                            s = s.replace(
                                Regex(
                                    "<dependencyManagement>.*?</dependencyManagement>",
                                    RegexOption.DOT_MATCHES_ALL
                                ),
                                ""
                            )
                            sb.setLength(0)
                            sb.append(s)
                            // Re-format the XML
                            asNode()
                        }
                        name.set(
                            (project.findProperty("artifact.name") as? String) ?: "Calcite ${project.name.capitalize()}"
                        )
                        description.set(project.description ?: "Calcite ${project.name.capitalize()}")
                        inceptionYear.set("2012")
                        url.set("https://calcite.apache.org")
                        licenses {
                            license {
                                name.set("The Apache License, Version 2.0")
                                url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                                comments.set("A business-friendly OSS license")
                                distribution.set("repo")
                            }
                        }
                        issueManagement {
                            system.set("Jira")
                            url.set("https://issues.apache.org/jira/browse/CALCITE")
                        }
                        mailingLists {
                            mailingList {
                                name.set("Apache Calcite developers list")
                                subscribe.set("dev-subscribe@calcite.apache.org")
                                unsubscribe.set("dev-unsubscribe@calcite.apache.org")
                                post.set("dev@calcite.apache.org")
                                archive.set("https://lists.apache.org/list.html?dev@calcite.apache.org")
                            }
                        }
                        scm {
                            connection.set("scm:git:https://gitbox.apache.org/repos/asf/calcite.git")
                            developerConnection.set("scm:git:https://gitbox.apache.org/repos/asf/calcite.git")
                            url.set("https://github.com/apache/calcite")
                            tag.set("HEAD")
                        }
                    }
                }
            }
        }
    }
}
