plugins {
    `java-library`
    `maven-publish`
    jacoco
}


repositories {
    mavenCentral()
    mavenLocal()
}


dependencies {
    // Use JUnit Jupiter for testing.
    testImplementation("junit:junit:4.13.2")

    // This dependency is used internally, and not exposed to consumers on their own compile classpath.
    implementation("it.unimi.dsi:fastutil:8.5.8")
    implementation("org.lz4:lz4-java:1.8.0")
    implementation("com.google.guava:guava:31.1-jre")
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            groupId = "io.viktoriadb"
            artifactId = "viktoriadb"
            version = "1.0-SNAPSHOT"

            from(components["java"])
        }
    }
}

tasks.compileTestJava {
    options.compilerArgs.add("--add-modules")
    options.compilerArgs.add("jdk.incubator.foreign")
}

tasks.withType<Test>().all {
    jvmArgs("--add-modules", "jdk.incubator.foreign", "-XX:+HeapDumpOnOutOfMemoryError")
    testLogging.events.add(org.gradle.api.tasks.testing.logging.TestLogEvent.FAILED)
    testLogging.events.add(org.gradle.api.tasks.testing.logging.TestLogEvent.PASSED)
}

tasks.jacocoTestReport {
    dependsOn(tasks.test)
}