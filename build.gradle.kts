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

/**
 * Runs isolated Spark jobs on the Docker cluster for each requested version and prints a unified comparison table.
 *
 * Examples:
 *   ./gradlew benchmark -Pm2=v5,v6,v7 -Psample=0.1 -Pdata="/data/yellow_tripdata_*.parquet"
 *   ./gradlew benchmark -Pm2=all -Psample=0.1
 *   ./gradlew benchmark -Pm1=v2,v3 -Pops=M1
 */
tasks.register("benchmark") {
    group = "verification"
    description = "Compiles the JAR once and runs isolated cluster benchmarks for a list of operation versions."
    dependsOn("jar")

    doLast {
        fun parseVersions(prop: String, default: String, allVersions: List<String>): List<String> {
            val raw = (project.findProperty(prop)?.toString() ?: default).trim()
            if (raw.equals("all", ignoreCase = true)) return allVersions
            return raw.split(",").map { it.trim() }.filter { it.isNotEmpty() }
        }

        val m1List = parseVersions("m1", "v3", listOf("v0", "v1", "v2", "v3"))
        val m2List = parseVersions("m2", "v7", listOf("v0", "v1", "v2", "v3", "v4", "v5", "v6", "v7"))
        val m3List = parseVersions("m3", "v0", listOf("v0", "v1"))
        val o1List = parseVersions("o1", "v0", listOf("v0", "v1"))
        val o4List = parseVersions("o4", "v0", listOf("v0", "v1"))

        val explicitOps = project.findProperty("ops")?.toString()?.trim()
        val inferredOps = buildList {
            if (project.hasProperty("m1")) add("M1")
            if (project.hasProperty("m2")) add("M2")
            if (project.hasProperty("m3")) add("M3")
            if (project.hasProperty("o1")) add("O1")
            if (project.hasProperty("o4")) add("O4")
        }
        val ops = explicitOps ?: if (inferredOps.isNotEmpty()) inferredOps.joinToString(",") else "M1,M2,M3,O1,O4"
        val data = project.findProperty("data")?.toString() ?: "/data/yellow_tripdata_2015-*.parquet"
        val sample = project.findProperty("sample")?.toString()
        val runsPerConfig = project.findProperty("runs")?.toString()?.toIntOrNull()?.coerceAtLeast(1) ?: 1

        data class RunConfig(val m1: String, val m2: String, val m3: String, val o1: String, val o4: String, val runIdx: Int) {
            fun label(): String {
                val base = buildList {
                    if (m1List.size > 1 || ops == "M1") add("M1:$m1")
                    if (m2List.size > 1 || "M2" in ops) add("M2:$m2")
                    if (m3List.size > 1 || "M3" in ops) add("M3:$m3")
                    if (o1List.size > 1 || "O1" in ops) add("O1:$o1")
                    if (o4List.size > 1 || "O4" in ops) add("O4:$o4")
                }.joinToString("+").ifEmpty { "M1:$m1+M2:$m2" }
                return if (runsPerConfig > 1) "$base#r$runIdx" else base
            }
        }

        val configs = mutableListOf<RunConfig>()
        for (v1 in m1List) for (v2 in m2List) for (v3 in m3List) for (vo1 in o1List) for (vo4 in o4List) {
            for (r in 1..runsPerConfig) {
                configs.add(RunConfig(v1, v2, v3, vo1, vo4, r))
            }
        }

        val logDir = file("benchmarks/logs").apply { mkdirs() }
        var tableHeaders: List<String>? = null
        val collectedGroups = mutableListOf<List<List<String>>>()

        println("\n================================================================================")
        println("  Isolated Spark Cluster Benchmark (${configs.size} run(s))")
        println("  Operations: $ops | Data: $data" + (if (sample != null) " | Sample: $sample" else ""))
        println("================================================================================")

        fun parseCells(line: String): List<String> =
            line.trim().removeSurrounding("|").split("|").map { it.trim() }

        configs.forEachIndexed { idx, cfg ->
            val label = cfg.label()
            val safeLabel = label.replace(":", "").replace("+", "_").replace("#", "_")
            println("\n>>> [${idx + 1}/${configs.size}] Running isolated spark-submit for [$label] ...")

            val cmd = mutableListOf(
                "docker", "exec", "spark-master",
                "/opt/spark/bin/spark-submit",
                "--class", "compar.MainKt",
                "--master", "spark://spark-master:7077",
                "--deploy-mode", "client",
                "/apps/nyc-taxi-analytics.jar",
                "--data", data,
                "--lookup", "/data/taxi_zone_lookup.csv",
                "--output", "/output",
                "--export-metrics", "/output/benchmarks/profiling_${safeLabel}.json",
                "--profile",
                "--ops", ops,
                "--m1", cfg.m1,
                "--m2", cfg.m2,
                "--m3", cfg.m3,
                "--o1", cfg.o1,
                "--o4", cfg.o4
            )
            if (sample != null) {
                cmd.addAll(listOf("--sample", sample))
            }

            val proc = ProcessBuilder(cmd).redirectErrorStream(true).start()
            val outputLines = mutableListOf<String>()
            proc.inputStream.bufferedReader().useLines { lines ->
                lines.forEach { line ->
                    outputLines.add(line)
                    if (line.startsWith("[M") || line.startsWith("[O") || line.startsWith("Total pipeline")) {
                        println("    $line")
                    }
                }
            }
            val exitCode = proc.waitFor()
            File(logDir, "gradle_bench_${safeLabel}.log").writeText(outputLines.joinToString("\n"))

            if (exitCode != 0) {
                println("    [ERROR] Run [$label] exited with code $exitCode (see benchmarks/logs/gradle_bench_${safeLabel}.log)")
            } else {
                val headerIdx = outputLines.indexOfLast { it.contains("run") && it.contains("wall_time_s") }
                if (headerIdx >= 0 && headerIdx + 2 < outputLines.size) {
                    if (tableHeaders == null) {
                        tableHeaders = parseCells(outputLines[headerIdx])
                    }
                    val runGroup = mutableListOf<List<String>>()
                    for (i in (headerIdx + 2) until outputLines.size) {
                        val row = outputLines[i].trim()
                        if (!row.startsWith("|") && !row.contains(" | ")) break
                        if (row.startsWith("+")) break
                        val cells = parseCells(row).toMutableList()
                        if (cells.isNotEmpty()) {
                            cells[0] = "${cells[0]} ($label)"
                            runGroup.add(cells)
                        }
                    }
                    if (runGroup.isNotEmpty()) {
                        collectedGroups.add(runGroup)
                    }
                }
            }
        }

        val headers = tableHeaders
        if (headers != null && collectedGroups.isNotEmpty()) {
            println("\n================================================================================")
            println("  Consolidated Benchmark Comparison (Isolated Runs)")
            println("================================================================================")
            val allRows = collectedGroups.flatten()
            val byOp = allRows.groupBy { it.firstOrNull()?.substringBefore(" (") ?: "other" }
            val widths = headers.indices.map { c ->
                maxOf(headers[c].length, allRows.maxOfOrNull { it.getOrNull(c)?.length ?: 0 } ?: 0)
            }
            val rowSeparator = widths.joinToString("-+-", "+-", "-+") { "-".repeat(it + 2) }
            val headerSeparator = widths.joinToString("=+=", "+=", "=+") { "=".repeat(it + 2) }
            fun formatRow(cells: List<String>): String =
                cells.mapIndexed { i, cell -> " " + cell.padEnd(widths[i]) + " " }.joinToString(" | ", "| ", " |")

            println(rowSeparator)
            println(formatRow(headers))
            println(headerSeparator)
            val opGroups = byOp.values.toList()
            opGroups.forEachIndexed { idx, opRows ->
                opRows.forEach { r -> println(formatRow(r)) }
                if (idx < opGroups.lastIndex) {
                    println(rowSeparator)
                }
            }
            println(rowSeparator)
        }
    }
}

/**
 * Writes the runtime classpath and the Java 17 launcher used by scripts/local_bench (local[*] runs in fresh JVMs).
 * Usage: ./gradlew localBenchClasspath
 */
tasks.register("localBenchClasspath") {
    group = "verification"
    description = "Writes build/local-bench/classpath.txt and java.txt for scripts/local_bench."
    dependsOn("classes")
    doLast {
        val cp = sourceSets["main"].output.classesDirs.files + sourceSets["main"].output.resourcesDir!! +
            configurations.runtimeClasspath.get().files + sparkRunner.files
        val java = javaToolchains.launcherFor { languageVersion.set(JavaLanguageVersion.of(17)) }
            .get().executablePath.asFile
        val dir = layout.buildDirectory.dir("local-bench").get().asFile.apply { mkdirs() }
        File(dir, "classpath.txt").writeText(cp.joinToString(File.pathSeparator))
        File(dir, "java.txt").writeText(java.absolutePath)
    }
}
