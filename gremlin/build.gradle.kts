dependencies {
    api(project(":core"))
    api(project(":linq4j"))
    api("com.fasterxml.jackson.core:jackson-core")
    api("joda-time:joda-time")
    api("org.apache.calcite.avatica:avatica-core")
    api("org.checkerframework:checker-qual")
    api("org.slf4j:slf4j-api")

    implementation("com.fasterxml.jackson.core:jackson-databind")
    implementation("com.google.guava:guava")
    implementation("org.apache.commons:commons-lang3")
    implementation("org.apache.tinkerpop:gremlin-core:3.7.1")
    implementation("org.apache.tinkerpop:gremlin-driver:3.7.1")
    implementation("org.apache.tinkerpop:gremlin-console:3.7.1")

    implementation("org.apache.tinkerpop:tinkergraph-gremlin:3.7.1")
    implementation("org.apache.tinkerpop:gremlin-groovy:3.7.1")
    implementation("org.projectlombok:lombok:1.16.10")

    testImplementation(project(":core", "testClasses"))
    testImplementation("org.mockito:mockito-core")
    testImplementation("org.apache.tinkerpop:gremlin-test:3.4.8")
    testRuntimeOnly("org.slf4j:slf4j-log4j12")
}
