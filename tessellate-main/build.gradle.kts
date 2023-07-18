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

plugins {
    java
    application
    `java-test-fixtures`
    id("org.jreleaser") version "1.7.0"
}

val versionProperties = Properties().apply {
    load(FileInputStream(File(rootProject.rootDir, "version.properties")))
}

val buildRelease = false
val buildNumber = System.getenv("GITHUB_RUN_NUMBER") ?: "dev"
val wipReleases = "wip-${buildNumber}"

version = if (buildRelease)
    "${versionProperties["release.major"]}-${versionProperties["release.minor"]}"
else "${versionProperties["release.major"]}-${wipReleases}"

repositories {
    mavenCentral()

    maven {
        url = uri("https://maven.pkg.github.com/cwensel/*")
        name = "github"
        credentials(PasswordCredentials::class) {
            username = (project.findProperty("githubUsername") ?: System.getenv("USERNAME")) as? String
            password = (project.findProperty("githubPassword") ?: System.getenv("GITHUB_TOKEN")) as? String
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
    implementation("info.picocli:picocli:4.7.4")

    implementation("org.slf4j:slf4j-api:2.0.7")
    implementation("ch.qos.logback:logback-classic:1.4.8")
    implementation("ch.qos.logback:logback-core:1.4.8")

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

    // required by hadoop in java 9+
    implementation("javax.xml.bind:jaxb-api:2.3.0")

    // the bundle is too large, so we only include the s3 and dynamodb dependencies
    implementation("com.amazonaws:aws-java-sdk-s3:1.12.487")
    implementation("com.amazonaws:aws-java-sdk-dynamodb:1.12.487")

    val jackson = "2.14.2"
    implementation("com.fasterxml.jackson.core:jackson-core:$jackson")
    implementation("com.fasterxml.jackson.core:jackson-databind:$jackson")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-joda:$jackson")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:$jackson")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jdk8:$jackson")

    implementation("org.fusesource.jansi:jansi:2.4.0")
    implementation("com.github.hal4j:uritemplate:1.3.1")

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

//     https://mvnrepository.com/artifact/software.amazon.awssdk
    val awsSdk2 = "2.20.69"
    integrationTestImplementation("software.amazon.awssdk:s3:$awsSdk2")

    val testContainers = "1.18.3"
    integrationTestImplementation("org.testcontainers:testcontainers:$testContainers")
    integrationTestImplementation("org.testcontainers:junit-jupiter:$testContainers")
    integrationTestImplementation("org.testcontainers:localstack:$testContainers")
    // https://github.com/testcontainers/testcontainers-java/issues/1442#issuecomment-694342883
    integrationTestImplementation("com.amazonaws:aws-java-sdk-s3:1.12.13")

    testFixturesImplementation("org.jetbrains:annotations:24.0.0")
    testFixturesImplementation("org.junit.jupiter:junit-jupiter-api:$jupiter")

    configurations.configureEach {
        exclude(group = "org.slf4j", module = "slf4j-log4j12")
        exclude(group = "org.slf4j", module = "slf4j-reload4j")
        exclude(group = "log4j", module = "log4j")
        exclude(group = "ch.qos.reload4j", module = "reload4j")
    }

    configurations {
        implementation.configure {
            exclude(group = "com.amazonaws", module = "aws-java-sdk-bundle")
            exclude(group = "org.apache.directory.server")
            exclude(group = "org.apache.curator")
            exclude(group = "org.apache.avro")
            exclude(group = "org.apache.hadoop", module = "hadoop-yarn-api")
            exclude(group = "org.apache.hadoop", module = "hadoop-yarn-core")
            exclude(group = "org.apache.hadoop", module = "hadoop-yarn-client")
            exclude(group = "org.apache.hadoop", module = "hadoop-yarn-common")
            exclude(group = "org.apache.kerby")
            exclude(group = "com.google.protobuf")
            exclude(group = "com.google.inject.extensions", module = "guice-servlet")
            exclude(group = "com.sun.jersey")
            exclude(group = "com.sun.jersey.contribs")
            exclude(group = "org.eclipse.jetty")
            exclude(group = "org.eclipse.jetty.websocket")
            exclude(group = "org.apache.zookeeper")
            exclude(group = "commons-cli")
            exclude(group = "com.jcraft")
            exclude(group = "com.nimbusds")
            exclude(group = "io.netty")
            exclude(group = "javax.servlet", module = "javax.servlet-api")
        }
    }
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

distributions {
    main {
        distributionBaseName.set("tessellate")
    }
}

jreleaser {
    dryrun.set(false)

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
        active.set(Active.ALWAYS)
        verify.set(false)
    }

    release {
        github {
            overwrite.set(true)
            sign.set(false)
            repoOwner.set("ClusterlessHQ")
            name.set("tessellate")
            username.set("cwensel")
            branch.set("wip-1.0")
            changelog.enabled.set(false)
            milestone.close.set(false)
        }
    }

    distributions {
        create("tessellate") {
            distributionType.set(Distribution.DistributionType.JAVA_BINARY)
            executable {
                name.set("tess")
            }
            artifact {
                path.set(file("build/distributions/{{distributionName}}-{{projectVersion}}.zip"))
            }
        }
    }

    packagers {
        docker {
            active.set(Active.ALWAYS)
            repository {
                repoOwner.set("clusterless")
                name.set("tessellate")
                tagName.set("{{projectVersion}}")
            }

            registries {
                create("DEFAULT") {
                    externalLogin.set(true)
                    repositoryName.set("clusterless")
                }
            }

            imageName("{{owner}}/{{distributionName}}:{{projectVersion}}")

            if (buildRelease) {
                imageName("{{owner}}/{{distributionName}}:{{projectVersionMajor}}")
                imageName("{{owner}}/{{distributionName}}:{{projectVersionMajor}}.{{projectVersionMinor}}")
                imageName("{{owner}}/{{distributionName}}:latest")
            } else {
                imageName("{{owner}}/{{distributionName}}:latest-wip")
            }

        }
    }
}

tasks.register("release") {
    dependsOn("distZip")
    dependsOn("jreleaserRelease")
// disable until 1.8.0 is released
//    dependsOn("jreleaserPublish")
}
