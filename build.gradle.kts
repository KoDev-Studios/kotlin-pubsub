plugins {
    kotlin("multiplatform") version "2.4.0"
}

group = "site.kodev"
version = "1.0.0"

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
