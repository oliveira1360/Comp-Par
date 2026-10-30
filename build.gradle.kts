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

val sparkRunner by configurations.creating

dependencies {
    // Kotlin Standard Library (included in the final JAR to run on the Spark workers)
    implementation(kotlin("stdlib"))

    // Apache Spark Core & SQL (provided by the spark-submit runtime on the cluster)
    compileOnly("org.apache.spark:spark-core_$scalaBinaryVersion:$sparkVersion")
    compileOnly("org.apache.spark:spark-sql_$scalaBinaryVersion:$sparkVersion")

    // Full classpath (including runtime dependencies such as parquet-jackson) for local `./gradlew run`
    sparkRunner("org.apache.spark:spark-core_$scalaBinaryVersion:$sparkVersion")
    sparkRunner("org.apache.spark:spark-sql_$scalaBinaryVersion:$sparkVersion")
}

kotlin {
    jvmToolchain(17)
}

application {
    mainClass.set("compar.MainKt")
}

// Allows running locally with `./gradlew run --args="--master local[*] ..."`, including the Spark runtime libs
tasks.named<JavaExec>("run") {
    classpath += sparkRunner
    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.net=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
    )
}

// Packages only our code + kotlin-stdlib into a JAR ready for `spark-submit`
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
