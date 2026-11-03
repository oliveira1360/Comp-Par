# ComPar 26/27 — Project 1: Large-Scale Analytics with Spark (Kotlin)

Pipeline de análise distribuída em **Kotlin + Apache Spark** sobre os registos de viagens dos táxis de Nova Iorque (NYC TLC Yellow Taxi), com foco em engenharia de performance, perfilagem (*profiling*) e escalabilidade.

---

## 📁 Estrutura do Projeto

* `build.gradle.kts` / `settings.gradle.kts`: Configuração de build Gradle (Kotlin DSL, JDK 17, Spark 3.5.3).
* `src/main/kotlin/compar/Main.kt`: Pipeline principal Spark em Kotlin executável via `spark-submit` ou `./gradlew run`.
* `src/main/kotlin/compar/ops/`: Operações versionadas. `Contracts.kt` define as interfaces (`DataLoader` para o M1, `Analysis` para M2–O4) e cada operação tem o seu package (`m1/`, `m2/`, `m3/`, `o1/`, `o4/`) com uma classe por versão (`V0Baseline` = baseline gerado por IA; cada versão N parte da N-1).
* `src/main/kotlin/compar/profiler/SparkProfiler.kt`: Utilitário em Kotlin para recolha de métricas de execução (tarefas, tempos, shuffles, spills, GC) via REST API do Spark.
* `notebooks/exploration.ipynb`: Notebook Jupyter para exploração interativa de dados e prototipagem.
* `docker/`: Configurações Docker (`Dockerfile` e `docker-compose.yml`) com Spark Master, Workers e JupyterLab.
* `scripts/`: Scripts Bash para automação do cluster, compilação e submissão de jobs.
* `data/`: Diretoria local para datasets Parquet e CSVs de lookup (ignorada no Git).

---

## 📊 Datasets e Fontes

Os dados são descarregados localmente e **não devem ser enviados para o repositório Git**:
1. **Tabela de Zonas:**
   * Ficheiro: `data/taxi_zone_lookup.csv`
   * URL: `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`
2. **Viagens dos Táxis Amarelos (Yellow Taxi Trip Records):**
   * Formato: Ficheiros mensais em Parquet (`yellow_tripdata_YYYY-MM.parquet`).
   * Exemplo de teste (Janeiro 2024): `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet`
   * Portal oficial: NYC TLC Website (`https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page`).

---

## 🚀 Como Compilar, Iniciar e Gerir o Cluster

### 1. Compilar o JAR (Gradle)
```bash
./gradlew jar
```
O ficheiro `build/libs/nyc-taxi-analytics.jar` será gerado (incluindo a `kotlin-stdlib` para correr diretamente nos workers do Spark).

### 2. Iniciar o Cluster Docker
Certifica-te de que o **Docker Desktop** está em execução.

```bash
# Iniciar com 2 workers (por defeito):
./scripts/start_cluster.sh

# Ou especificar o número de workers desejado (ex: 3):
./scripts/start_cluster.sh 3
```

### 3. Aceder às Interfaces Web
* **Spark Master UI:** [http://localhost:8080](http://localhost:8080)
* **JupyterLab:** [http://localhost:8888/?token=compar](http://localhost:8888/?token=compar)
* **Spark Application UI:** [http://localhost:4040](http://localhost:4040) (ativo durante a execução de jobs)

### 4. Verificar o Estado dos Contentores
```bash
./scripts/status_cluster.sh
```

### 5. Executar o Pipeline Spark em Kotlin (Submissão de Jobs)
O script `submit_job.sh` compila automaticamente o JAR com Gradle e submete-o ao `spark-master`:

```bash
# Execução simples:
./scripts/submit_job.sh

# Execução com amostragem de 10% e com o profiler ativado:
./scripts/submit_job.sh --sample 0.1 --profile
```

Também podes correr localmente fora do Docker usando o Gradle:
```bash
./gradlew run --args="--master local[*] --sample 0.1 --profile"
```

#### Escolher operações e versões
As operações e a versão de cada uma escolhem-se na linha de comandos (por defeito: todas as operações, com M1 v3, M2 v7 e as restantes em v0):

```bash
# pipeline otimizado (ALL_best do relatório)
./scripts/submit_job.sh --ops M1,M2,M3,O1,O4 --m1 v3 --m2 v7 --m3 v3 --o1 v7 --o4 v0 --profile
```

Opções: `--data <dir>`, `--lookup <csv>`, `--output <dir>`, `--master <url>`, `--sample <fração>`, `--ops M1,M2,...`,
`--m1/--m2/--m3/--o1/--o4 vN`, `--profile` e `--export-metrics <ficheiro.json>` (métricas do profiler por operação e por stage).

Para adicionar uma versão: copiar a versão anterior da operação para uma classe nova (ex: `ops/o1/O1V8.kt`), documentar no KDoc o
bottleneck e a alteração, e acrescentá-la ao `when` da operação em `Main.kt`.

### 6. Parar o Cluster
```bash
./scripts/stop_cluster.sh
```

### 7. Executar Benchmarks no Cluster Docker
Para comparar o desempenho entre diferentes versões ou pipelines completos num ambiente isolado:

```bash
# Comparar todas as versões de M2 no cluster Docker:
./scripts/benchmark.sh --m2 all --sample 0.1

# Comparar todas as versões de M3:
./scripts/benchmark.sh --m3 all --sample 0.1

# Comparar todas as versões de O4:
./scripts/benchmark.sh --o4 all --sample 0.1

# Correr o pipeline completo com as melhores versões:
./scripts/submit_job.sh --ops M1,M2,M3,O1,O4 --m1 v3 --m2 v7 --m3 v3 --o1 v7 --o4 v6 --profile
```

Os resultados de dados Parquet e os ficheiros JSON com as métricas são gravados em `output/` organizados por operação (ex: `output/M3Outputs/`, `output/M3Metrics/`).
Para visualizar os gráficos e tabelas consolidadas de desempenho, abre o notebook [`notebooks/benchmarks_report.ipynb`](notebooks/benchmarks_report.ipynb) no Jupyter.

---

## 📖 Guia Completo e Referência
Consulta o documento detalhado [`docs/GUIA_WORKFLOW.md`](docs/GUIA_WORKFLOW.md) para:
* Explicação detalhada de todas as opções de linha de comando (`--sample`, `--profile`, etc.).
* Estrutura interna dos ficheiros Parquet em `output/`.
* Guia de configuração passo a passo do Jupyter no IntelliJ IDEA.

