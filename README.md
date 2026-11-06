# ComPar 26/27 — Project 1: Large-Scale Analytics with Spark (Kotlin)

Pipeline de análise distribuída em **Kotlin + Apache Spark** sobre os registos de viagens dos táxis de Nova Iorque (NYC TLC Yellow Taxi), com foco em engenharia de performance, perfilagem (*profiling*) e escalabilidade.

---

## 📁 Estrutura do Projeto

* `build.gradle.kts` / `settings.gradle.kts`: Configuração de build Gradle (Kotlin DSL, JDK 17; compila contra a API do Spark 3.5.3 e corre no cluster com Spark 4.2.0/4.2.1).
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
   * **Dados usados no relatório: os 12 meses de 2015** (2.0 GB, 146 039 231 viagens), descarregados para `data/`:
     ```bash
     for m in 01 02 03 04 05 06 07 08 09 10 11 12; do
       curl -fL -o data/yellow_tripdata_2015-$m.parquet https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2015-$m.parquet
     done
     ```
   * Versão usada: ficheiros obtidos do CDN da TLC em novembro de 2026; os 12 ficheiros de 2015 somam 2 027 058 112 bytes
     (`yellow_tripdata_2015-01.parquet`: 175 325 767 bytes).
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
# pipeline otimizado (melhor versão de cada operação no relatório)
./scripts/submit_job.sh --ops M1,M2,M3,O1,O4 --m1 v3 --m2 v4 --m3 v3 --o1 v6 --o4 v6 --profile
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
./scripts/submit_job.sh --ops M1,M2,M3,O1,O4 --m1 v3 --m2 v4 --m3 v3 --o1 v6 --o4 v6 --profile
```

Cada versão gera os dados em `output/outputs/<op>/<versão>/` e as métricas do profiler em
`output/metrics/<op>/profiling_<versão>.json` (com `--runs N`: `profiling_<versão>_r<N>.json`).
Para visualizar os gráficos e tabelas consolidadas de desempenho, abre o notebook [`notebooks/benchmarks_report.ipynb`](notebooks/benchmarks_report.ipynb) no Jupyter.

### 8. Reproduzir os resultados do relatório

**Ambientes usados no relatório** (as mesmas corridas nas duas máquinas):
* **Windows** (números principais, Figuras 2 e 3): Docker Desktop em Windows 11, portátil Intel Core Ultra 7 256V
  (4 P-cores + 4 E-cores), 16 GB de RAM, NVMe SSD; imagem `spark:python3` com **Spark 4.2.0**.
* **Linux** (coluna "Linux" da Tabela 2 e curvas "Linux" da Figura 4): Docker Engine sem VM em Fedora Linux 45,
  portátil Intel Core Ultra 7 155H (6 P-cores + 8 E-cores + 2 LP-cores), 32 GB de RAM, NVMe SSD; **Spark 4.2.1**.
  

Em ambas: cluster standalone de `docker/docker-compose.yml` (Scala 2.13, OpenJDK 21), 1 master e
**2 workers com 2 cores e 2 GB cada**, cada contentor limitado a 2 CPUs (`./scripts/start_cluster.sh 2 2G 2`). O jar é compilado com JDK 17 e
Kotlin 2.0.21. **Configuração não-default do Spark:** `spark.executor.memory=2g` (definida em `src/main/kotlin/compar/util/SparkUtils.kt`;
o default é 1g). O mesmo ficheiro define `spark.driver.memory=2g`, que em client mode não tem efeito, e
`spark.sql.adaptive.enabled=true`, que já é o default.

Cada execução do benchmark corresponde a este `spark-submit`, lançado dentro do contentor `spark-master`
(o driver corre no master, em client mode):
```bash
docker exec spark-master /opt/spark/bin/spark-submit --class compar.MainKt \
  --master spark://spark-master:7077 --deploy-mode client /apps/nyc-taxi-analytics.jar \
  --data "/data/yellow_tripdata_2015-*.parquet" --lookup /data/taxi_zone_lookup.csv \
  --output /output/outputs/<op>/<versão> --export-metrics /output/metrics/<op>/profiling_<versão>.json \
  --profile --ops <OP> --m1 v3 --<op> <versão>
```

Passos:
```bash
./scripts/start_cluster.sh 2                    # cluster com 2 workers
./scripts/benchmark.sh --all --runs 3           # Secções 3-4: todas as versões de M1, M2, M3, O1 e O4, 3 corridas cada (~1h30)
./scripts/cluster_bench/scaling_2015.sh         # Secção 5: pipeline otimizado com 100/20/10 % e 1/2/3 workers (~40 min)
```
O relatório usa a mediana das 3 corridas. A análise de escalabilidade usa M1 v2, escolhida antes das 3 corridas
mostrarem que a v3 é mais rápida (é o que `scaling_2015.sh` corre).
O benchmark corre cada versão num `spark-submit` novo, as operações analíticas depois de M1 v3, com 20 s de pausa entre
operações. Os gráficos e tabelas do relatório saem do notebook `notebooks/benchmarks_report.ipynb`
(abrir em http://localhost:8888/?token=compar e fazer *Run All*); os gráficos ficam em `output/plots/`.
O relatório está em `docs/report.tex` (compilar com `pdflatex report && bibtex report && pdflatex report && pdflatex report`).

---

## 📖 Guia Completo e Referência
Consulta o documento detalhado [`docs/GUIA_WORKFLOW.md`](docs/GUIA_WORKFLOW.md) para:
* Explicação detalhada de todas as opções de linha de comando (`--sample`, `--profile`, etc.).
* Estrutura interna dos ficheiros Parquet em `output/`.
* Guia de configuração passo a passo do Jupyter no IntelliJ IDEA.

