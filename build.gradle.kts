import org.gradle.api.file.DuplicatesStrategy
import org.gradle.jvm.tasks.Jar

plugins {
    kotlin("jvm") version "2.0.21"
    application
}

group = "pt.unl.fct.compar"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

val sparkVersion = "3.5.3"
val scalaBinaryVersion = "2.12"

dependencies {
    // Kotlin Standard Library (incluída no JAR final para correr nos workers do Spark)
    implementation(kotlin("stdlib"))

    // Apache Spark Core & SQL (fornecidos pelo runtime do spark-submit no cluster)
    compileOnly("org.apache.spark:spark-core_$scalaBinaryVersion:$sparkVersion")
    compileOnly("org.apache.spark:spark-sql_$scalaBinaryVersion:$sparkVersion")
}

kotlin {
    jvmToolchain(17)
}

application {
    mainClass.set("compar.MainKt")
}

// Permite correr localmente com `./gradlew run --args="--master local[*] ..."` incluindo as libs compileOnly do Spark
sourceSets {
    main {
        runtimeClasspath += configurations.compileOnly.get()
    }
}

// Empacota apenas o nosso código + kotlin-stdlib num JAR pronto para `spark-submit`
tasks.named<Jar>("jar") {
    archiveBaseName.set("nyc-taxi-analytics")
    archiveVersion.set("")
    manifest {
        attributes["Main-Class"] = "compar.MainKt"
    }
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    from(
        configurations.runtimeClasspath.get()
            .filter { it.name.endsWith(".jar") }
            .map { zipTree(it) }
    )
}
