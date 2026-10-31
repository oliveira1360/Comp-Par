#!/usr/bin/env bash
# Builds the Kotlin project (with Gradle) and submits the JAR to the Spark Master inside Docker
# Usage: ./scripts/submit_job.sh [--help] [--skip-build] [pipeline arguments...]

set -e

# Stops Git Bash (MSYS) from converting container paths (/opt, /apps, /data) to Windows paths
export MSYS_NO_PATHCONV=1

cd "$(dirname "$0")/.." || exit 1

show_help() {
    cat << 'EOF'
================================================================================
  NYC Yellow Taxi Analytics - Submissão de Job Spark (scripts/submit_job.sh)
================================================================================

Uso:
  ./scripts/submit_job.sh [opções do script] [argumentos do pipeline...]

Opções do Script:
  -h, --help, help        Mostra esta mensagem de ajuda detalhada e termina.
  --skip-build, --no-build Não compila o JAR com Gradle antes de submeter
                          (útil para testes rápidos se o JAR já estiver compilado).

Argumentos do Pipeline Kotlin (MainKt):
  --ops <M1,M2,...>       Lista de operações a executar separadas por vírgula.
                          Operações disponíveis: M1, M2, M3, O1, O4
                          Default: M1,M2,M3,O1,O4
                          Exemplo só M1:         --ops M1
                          Exemplo M1 e O1:       --ops M1,O1
                          Exemplo M1, M2 e M3:   --ops M1,M2,M3

  --sample <fraction>     Fração de amostragem dos dados (entre 0.0 e 1.0).
                          Default: 1.0 (processa o dataset completo)
                          Exemplo 0.1% (~3k):    --sample 0.001
                          Exemplo 1%:            --sample 0.01
                          Exemplo 10%:           --sample 0.1

  --profile               Ativa o SparkProfiler (mede tempos de stages, shuffle e memória).
  --data <path>           Caminho para ficheiro(s) Parquet de entrada no Docker.
                          Default: /data/yellow_tripdata_2015-01.parquet
                          Suporta padrões glob, ex: "/data/yellow_tripdata_2015-*.parquet"

  --lookup <path>         Caminho para o ficheiro taxi_zone_lookup.csv.
                          Default: /data/taxi_zone_lookup.csv

  --output <path>         Diretoria de destino para os resultados Parquet no Docker.
                          Default: /output

  --export-metrics <path> Caminho JSON para exportar o relatório do profiler.
                          Default: /output/profiling_report.json

  --master <url>          URL do Spark Master (default: spark://spark-master:7077).

Exemplos Práticos:
  1. Teste super rápido apenas do M1 com 0.1% dos dados:
     ./scripts/submit_job.sh --sample 0.001 --ops M1

  2. Teste do M1 e O1 com profiler ativado e 1% dos dados:
     ./scripts/submit_job.sh --sample 0.01 --profile --ops M1,O1

  3. Executar as 3 operações obrigatórias (M1, M2, M3) no mês 01:
     ./scripts/submit_job.sh --ops M1,M2,M3

  4. Executar com profiling sobre todos os ficheiros de 2024:
     ./scripts/submit_job.sh --data "/data/yellow_tripdata_2024-*.parquet" --profile

  5. Submeter sem recompilar o JAR (mais rápido em testes iterativos):
     ./scripts/submit_job.sh --skip-build --sample 0.01 --ops M1

Dica (Execução Local no Host sem Docker):
  Podes correr diretamente no teu computador via Gradle:
     ./gradlew run --args="--master local[*] --sample 0.01 --profile --ops M1"
================================================================================
EOF
}

SKIP_BUILD=false
PASSED_ARGS=()

for arg in "$@"; do
    case "$arg" in
        -h|--help|help)
            show_help
            exit 0
            ;;
        --skip-build|--no-build)
            SKIP_BUILD=true
            ;;
        *)
            PASSED_ARGS+=("$arg")
            ;;
    esac
done

# Ensures JDK 17 is used if the system JDK is Java 25+
if [ -d "$HOME/.jdks/temurin-17" ] && [ -z "$JAVA_HOME" ]; then
    export JAVA_HOME="$HOME/.jdks/temurin-17"
    export PATH="$JAVA_HOME/bin:$PATH"
fi

JAR_PATH="build/libs/nyc-taxi-analytics.jar"

if [ "$SKIP_BUILD" = true ]; then
    if [ ! -f "$JAR_PATH" ]; then
        echo "Aviso: '$JAR_PATH' não encontrado. A compilar com Gradle..."
        SKIP_BUILD=false
    else
        echo "A ignorar compilação com Gradle (--skip-build ativo)..."
    fi
fi

if [ "$SKIP_BUILD" = false ]; then
    if [ -x "./gradlew" ]; then
        GRADLE_OUTPUT=$(./gradlew jar --quiet 2>&1) || {
            echo "Erro na compilação Gradle:"
            echo "$GRADLE_OUTPUT"
            exit 1
        }
    else
        GRADLE_OUTPUT=$(./gradlew.bat jar --quiet 2>&1) || {
            echo "Erro na compilação Gradle:"
            echo "$GRADLE_OUTPUT"
            exit 1
        }
    fi
fi

if ! docker ps --format '{{.Names}}' | grep -qx spark-master; then
    echo "================================================================================"
    echo "Erro: O contentor 'spark-master' não está a correr!"
    echo "Inicia o cluster Spark antes de submeter o job:"
    echo "  ./scripts/start_cluster.sh"
    echo "  (ou: docker compose -f docker/docker-compose.yml up -d)"
    echo "================================================================================"
    exit 1
fi

TTY_FLAG="-i"
if [ -t 0 ]; then
    TTY_FLAG="-it"
fi

DEFAULT_ARGS=()
HAS_DATA=false
HAS_LOOKUP=false
HAS_OUTPUT=false

for arg in "${PASSED_ARGS[@]}"; do
    if [[ "$arg" == "--data" ]]; then HAS_DATA=true; fi
    if [[ "$arg" == "--lookup" ]]; then HAS_LOOKUP=true; fi
    if [[ "$arg" == "--output" ]]; then HAS_OUTPUT=true; fi
done

if [ "$HAS_DATA" = false ]; then
    DEFAULT_ARGS+=(--data "/data/yellow_tripdata_2015-01.parquet")
fi

if [ "$HAS_LOOKUP" = false ]; then
    DEFAULT_ARGS+=(--lookup "/data/taxi_zone_lookup.csv")
fi

if [ "$HAS_OUTPUT" = false ]; then
    DEFAULT_ARGS+=(--output "/output")
fi

echo "A submeter job Spark (Kotlin) no spark-master..."

docker exec $TTY_FLAG spark-master /opt/spark/bin/spark-submit \
    --class compar.MainKt \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    /apps/nyc-taxi-analytics.jar \
    "${DEFAULT_ARGS[@]}" \
    "${PASSED_ARGS[@]}"
