# ComPar 26/27 — Project 1: Large-Scale Analytics with Spark (Kotlin)

Pipeline de análise distribuída em **Kotlin + Apache Spark** sobre os registos de viagens dos táxis de Nova Iorque (NYC TLC Yellow Taxi), com foco em engenharia de performance, perfilagem (*profiling*) e escalabilidade.

---

## 📁 Estrutura do Projeto

* `build.gradle.kts` / `settings.gradle.kts`: Configuração de build Gradle (Kotlin DSL, JDK 17, Spark 3.5.3).
* `src/main/kotlin/compar/Main.kt`: Pipeline principal Spark em Kotlin executável via `spark-submit` ou `./gradlew run`.
* `src/main/kotlin/compar/SparkProfiler.kt`: Utilitário em Kotlin para recolha de métricas de execução (tarefas, tempos, shuffles, spills) via REST API do Spark.
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

### 6. Parar o Cluster
```bash
./scripts/stop_cluster.sh
```

---

## 📖 Guia Completo e Referência
Consulta o documento detalhado [`docs/GUIA_WORKFLOW.md`](docs/GUIA_WORKFLOW.md) para:
* Explicação detalhada de todas as opções de linha de comando (`--sample`, `--profile`, etc.).
* Estrutura interna dos ficheiros Parquet em `output/`.
* Guia de configuração passo a passo do Jupyter no IntelliJ IDEA.

