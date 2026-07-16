plugins {
    kotlin("multiplatform") version "2.4.0"
    signing
    id("com.vanniktech.maven.publish") version "0.37.0"
}
group = "site.kodev"
version = "1.0.0"
signing {
    useGpgCmd()
    sign(publishing.publications)
}
mavenPublishing{
    publishToMavenCentral()
    signAllPublications()
    coordinates(group.toString(), "kotlin-pubsub",version.toString())
    pom {
        name.set("Kotlin Pub-Sub")
        description.set("A Simple, Thread safe, Kotlin runtime PubSub supporting multiple publishers and subscribers on a topic")
        inceptionYear.set("2026")
        url = "https://github.com/KoDev-Studios/kotlin-pubsub"
        licenses {
            license {
                name = "The Apache License 2.0"
                url = "https://www.apache.org/licenses/LICENSE-2.0.txt"
                distribution = "https://www.apache.org/licenses/LICENSE-2.0.txt"
            }
        }
        developers {
            developer {
                id = "balpreet"
                name = "Balpreet Singh"
                url = "https://github.com/Balpreet-afk"
                email = "balpreet@kodev.site"
                organization = "Kodev Studios"
                organizationUrl = "https://kodev.site"
            }
        }
        scm{
            url = "https://github.com/KoDev-Studios/kotlin-pubsub"
            connection = "scm:git:git://github.com/KoDev-Studios/kotlin-pubsub.git"
            developerConnection = "scm:git:ssh://github.com/KoDev-Studios/kotlin-pubsub.git"
        }
    }
}


repositories {
    mavenCentral()
}

kotlin {
    jvm()
    sourceSets {
        commonMain.dependencies {
            implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.11.0")
        }
        commonTest.dependencies {
            implementation(kotlin("test"))
            implementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.10.2")

        }
    }
}
