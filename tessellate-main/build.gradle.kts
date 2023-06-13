/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

import org.jreleaser.model.Active
import org.jreleaser.model.Distribution
import org.jreleaser.model.Stereotype
import java.io.FileInputStream
import java.util.*

/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

plugins {
    java
    application
    `java-test-fixtures`
    id("org.jreleaser") version "1.6.0"
}

val versionProperties = Properties().apply {
    load(FileInputStream(File(rootProject.rootDir, "version.properties")))
}

val release = false;
val buildNumber = System.getenv("GITHUB_RUN_NUMBER") ?: "dev"
val wipReleases = "wip-${buildNumber}"

version = if (release)
    "${versionProperties["release.major"]}-${versionProperties["release.minor"]}"
else "${versionProperties["release.major"]}-${wipReleases}"


repositories {
    mavenCentral()

    maven {
        url = uri("https://maven.pkg.github.com/cwensel/*")
        name = "github"
        credentials(PasswordCredentials::class) {
            username = property("githubUsername") as String
            password = property("githubPassword") as String
        }
        content {
            includeVersionByRegex("net.wensel", "cascading-.*", ".*-wip-.*")
        }
    }

    mavenLocal() {
        content {
            includeVersionByRegex("net.wensel", "cascading-.*", ".*-wip-dev")
        }
    }
}

var integrationTestImplementation = configurations.create("integrationTestImplementation")

val jupiter = "5.9.1"

dependencies {
    implementation("com.google.guava:guava:31.1-jre")

    implementation("org.jetbrains:annotations:24.0.0")
    implementation("info.picocli:picocli:4.7.1")

    implementation("org.slf4j:slf4j-api:2.0.7")
    implementation("org.slf4j:slf4j-jdk14:2.0.7")

    val cascading = "4.6.0-wip-8"
    implementation("net.wensel:cascading-core:$cascading")
    implementation("net.wensel:cascading-nested-json:$cascading")
    implementation("net.wensel:cascading-local:$cascading")
    implementation("net.wensel:cascading-local-hadoop3-io:$cascading")
    implementation("net.wensel:cascading-hadoop3-parquet:$cascading")
    implementation("net.wensel:cascading-hadoop3-io:$cascading")

    val parquet = "1.13.1"
    implementation("org.apache.parquet:parquet-common:$parquet")
    implementation("org.apache.parquet:parquet-column:$parquet")
    implementation("org.apache.parquet:parquet-hadoop:$parquet")

    val hadoop3Version = "3.3.4"
    implementation("org.apache.hadoop:hadoop-mapreduce-client-core:$hadoop3Version")
    implementation("org.apache.hadoop:hadoop-common:$hadoop3Version")
    implementation("org.apache.hadoop:hadoop-aws:$hadoop3Version")

    val jackson = "2.14.2"
    implementation("com.fasterxml.jackson.core:jackson-core:$jackson")
    implementation("com.fasterxml.jackson.core:jackson-databind:$jackson")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-joda:$jackson")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:$jackson")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jdk8:$jackson")

    testImplementation("net.wensel:cascading-core:$cascading:tests")

    // https://github.com/hosuaby/inject-resources
    val injectResources = "0.3.3"
    testImplementation("io.hosuaby:inject-resources-core:$injectResources")
    testImplementation("io.hosuaby:inject-resources-junit-jupiter:$injectResources")

    // https://github.com/webcompere/system-stubs
    val systemStubs = "2.0.2"
    testImplementation("uk.org.webcompere:system-stubs-core:$systemStubs")
    testImplementation("uk.org.webcompere:system-stubs-jupiter:$systemStubs")
    testImplementation("org.mockito:mockito-inline:5.1.1")

    // https://mvnrepository.com/artifact/software.amazon.awssdk
    val awsSdk = "2.20.69"
    integrationTestImplementation("software.amazon.awssdk:s3:$awsSdk")

    val testContainers = "1.18.3"
    integrationTestImplementation("org.testcontainers:testcontainers:$testContainers")
    integrationTestImplementation("org.testcontainers:junit-jupiter:$testContainers")
    integrationTestImplementation("org.testcontainers:localstack:$testContainers")
    // https://github.com/testcontainers/testcontainers-java/issues/1442#issuecomment-694342883
    integrationTestImplementation("com.amazonaws:aws-java-sdk-s3:1.12.13")

    testFixturesImplementation("org.jetbrains:annotations:24.0.0")
    testFixturesImplementation("org.junit.jupiter:junit-jupiter-api:$jupiter")
}

testing {
    suites {
        val test by getting(JvmTestSuite::class) {
            useJUnitJupiter(jupiter)
        }
        val integrationTest by registering(JvmTestSuite::class) {
            dependencies {
                implementation(project())
            }

            targets {
                all {
                    testTask.configure {
                        shouldRunAfter(test)
                    }
                }
            }
        }
    }
}

configurations["integrationTestRuntimeOnly"].extendsFrom(configurations.runtimeOnly.get())
configurations["integrationTestImplementation"].extendsFrom(configurations.testImplementation.get())

tasks.named("check") {
    dependsOn(testing.suites.named("integrationTest"))
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(11))
    }
}

application {
    applicationName = "tess"
    mainClass.set("io.clusterless.tessellate.Main")
}

jreleaser {
    project {
        description.set("Tessellate is tool for parsing and partitioning data.")
        authors.add("Chris K Wensel")
        copyright.set("Chris K Wensel")
        license.set("MPL-2.0")
        stereotype.set(Stereotype.CLI)

        links {
            homepage.set("https://github.com/ClusterlessHQ")
        }
        inceptionYear.set("2023")
        gitRootSearch.set(true)
    }

    signing {
        armored.set(true)
        enabled.set(true)
        active.set(Active.ALWAYS)
        verify.set(false)
    }

    release {
        github {
            overwrite.set(true)
            sign.set(true)
            repoOwner.set("ClusterlessHQ")
            name.set("tessellate")
            username.set("cwensel")
            branch.set("wip-1.0")
            changelog.enabled.set(false)
            milestone.close.set(false)
        }
    }

    distributions {
        create("tess") {
            distributionType.set(Distribution.DistributionType.JAVA_BINARY)
            artifact {
                path.set(file("build/distributions/{{distributionName}}-{{projectVersion}}.zip"))
            }
        }
    }
}

tasks.register("release") {
    dependsOn("jreleaserConfig")
    dependsOn("jreleaserAssemble")
    dependsOn("jreleaserRelease")
}
