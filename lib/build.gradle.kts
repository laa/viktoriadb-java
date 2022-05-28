/*
 * This file was generated by the Gradle 'init' task.
 *
 * This generated file contains a sample Java library project to get you started.
 * For more details take a look at the 'Building Java & JVM projects' chapter in the Gradle
 * User Manual available at https://docs.gradle.org/7.4.1/userguide/building_java_projects.html
 */

plugins {
    // Apply the java-library plugin for API and implementation separation.
    `java-library`
}


repositories {
    // Use Maven Central for resolving dependencies.
    mavenCentral()
}


dependencies {
    // Use JUnit Jupiter for testing.
    testImplementation("junit:junit:4.13.2")

    // This dependency is used internally, and not exposed to consumers on their own compile classpath.
    implementation("it.unimi.dsi:fastutil:8.5.8")
    implementation("org.lz4:lz4-java:1.8.0")
    implementation("com.google.guava:guava:31.1-jre")
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