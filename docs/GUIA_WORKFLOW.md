# 📘 Guia de Execução, Fluxo de Trabalho e Referência

Este documento é o guia prático do projeto **ComPar 26/27 (Análise de Táxis de NYC com Apache Spark em Kotlin)**. Aqui encontras o passo a passo completo, explicação de argumentos, comandos essenciais e como consultar dados.

---

## 🎯 O que fazem as flags `--sample` e `--profile`?

Quando executas:
```bash
./scripts/submit_job.sh --sample 0.1 --profile
```

### 1. `--sample 0.1` (Amostragem de Dados)
* **O que faz**: Em vez de ler os ~3 milhões de registos de um mês completo, o Spark seleciona uma fração aleatória de **10%** dos dados (`0.1`).
* **Para que serve**:
  * **Desenvolvimento e Testes Rápidos**: Um mês inteiro pode demorar 1 a 2 minutos a processar; com `0.1` corre em ~15 a 25 segundos.
  * **Evita estouros de memória (OOM)** enquanto estás a afinar a lógica ou a testar transformações.
  * Podes usar valores como `--sample 0.01` (1% dos dados, ultra-rápido para testes de sintaxe) ou `--sample 0.5` (50%).
  * **Em produção / entrega**: Remove esta flag para processar 100% dos dados.

### 2. `--profile` (Perfilagem de Performance / SparkProfiler)
* **O que faz**: Ativa o [`SparkProfiler.kt`](../src/main/kotlin/compar/SparkProfiler.kt), que regista um *listener* interno no Spark para medir cada etapa da computação.
* **Métricas recolhidas e impressas na consola**:
  * **Duração de cada operação** (ex.: M1 Ingestão, M2 Procura Temporal, M3 Estatísticas de Zona).
  * **Volume de Shuffle**: Bytes e registos transferidos pela rede entre executores (`Shuffle Read/Write`).
  * **Spill para disco**: Deteta se os dados não couberam na RAM dos workers e foram descarregados para disco (`Spill to Disk`).
  * **Memória de pico de execução**: Pico de memória utilizado por cada executor.
* **Para que serve**:
  * É a ferramenta central para a avaliação do projeto: identificar estrangulamentos (*bottlenecks*), avaliar o impacto de otimizações (ex.: *broadcast joins*, particionamento) e recolher dados concretos para o relatório final.

---

## 🚀 Fluxo de Trabalho Passo a Passo

### 1. Descarregar Dados
Descarrega os ficheiros Parquet mensais do TLC de NYC e a tabela de zonas:
```bash
# Descarrega Janeiro a Março de 2024 para a pasta data/
./scripts/download_data.sh 2024 1 3
```

### 2. Iniciar o Cluster Docker
Inicia o Spark Master, Workers e JupyterLab em segundo plano:
```bash
# Iniciar com 2 workers e 2GB de RAM por worker (valores por omissão):
./scripts/start_cluster.sh

# Ou configurar workers e memória personalizados:
./scripts/start_cluster.sh 2 3G    # 2 workers com 3GB de RAM cada
./scripts/start_cluster.sh 3 4G    # 3 workers com 4GB de RAM cada
```

### 3. Verificar o Estado do Cluster
```bash
./scripts/status_cluster.sh
```

Interfaces Web de monitorização (abrir no browser):
* **Spark Master UI**: [http://localhost:8080](http://localhost:8080) (lista workers ativos, CPU e RAM livre).
* **Spark Application UI**: [http://localhost:4040](http://localhost:4040) (visível enquanto um job está a correr; mostra o DAG, stages e tarefas).
* **JupyterLab**: [http://localhost:8888/?token=compar](http://localhost:8888/?token=compar)

---

### 4. Executar Jobs Spark

O script [`scripts/submit_job.sh`](submit_job.sh) compila automaticamente o Kotlin com Gradle e submete o JAR ao Master do Spark:

```bash
# 1. Teste rápido (1% dos dados com relatório de perfilagem):
./scripts/submit_job.sh --sample 0.01 --profile

# 2. Execução de validação (10% dos dados):
./scripts/submit_job.sh --sample 0.1 --profile

# 3. Execução completa (100% dos dados de Janeiro de 2024):
./scripts/submit_job.sh --profile

# 4. Executar com múltiplos meses (ex: Janeiro a Março):
./scripts/submit_job.sh --data "/data/yellow_tripdata_2024-*.parquet" --profile
```

---

### 5. Configurar o Notebook no IntelliJ IDEA

Para não precisares de usar o browser para o Jupyter:
1. No IntelliJ, abre o ficheiro [`notebooks/exploration.ipynb`](../notebooks/exploration.ipynb).
2. No canto superior direito do editor, clica no seletor de Kernel / Servidor Jupyter.
3. Escolhe **Configured Server** (ou *Existing Jupyter Server*) e cola o URL:
   ```text
   http://localhost:8888/?token=compar
   ```
4. Agora podes correr as células diretamente no IntelliJ!

---

### 6. Como Consultar os Dados de Saída (`output/`)

O Spark grava os dados de saída em **Parquet particionado** na pasta local [`output/`](../output/):

```text
output/
└── m2_temporal_demand/
    ├── pickup_borough=Bronx/
    │   └── part-00000-....snappy.parquet
    ├── pickup_borough=Brooklyn/
    ├── pickup_borough=Manhattan/
    ├── pickup_borough=Queens/
    ├── pickup_borough=Staten Island/
    └── _SUCCESS
```

#### No Notebook (Python / Pandas):
No [`exploration.ipynb`](../notebooks/exploration.ipynb), corre:
```python
import pandas as pd

# Lê todas as partições num único DataFrame
df = pd.read_parquet('/output/m2_temporal_demand')

# Visualizar as primeiras linhas
print(df.head())

# Exemplo de gráfico de procura em Manhattan por hora
manhattan = df[df['pickup_borough'] == 'Manhattan']
manhattan.groupby('hour_of_day')['total_trips'].sum().plot(kind='bar', title='Procura em Manhattan')
```

---

### 7. Parar o Cluster
Quando acabares a sessão de trabalho:
```bash
./scripts/stop_cluster.sh
```

---

## 📌 Tabela de Referência Rápida de Comandos

| Comando | Descrição |
| :--- | :--- |
| `./scripts/start_cluster.sh [N] [RAM]` | Arranca o cluster com $N$ workers e limite de RAM (default: 2 workers, 2G). |
| `./scripts/stop_cluster.sh` | Desliga todos os contentores do cluster. |
| `./scripts/status_cluster.sh` | Lista os contentores e portas ativas. |
| `./scripts/download_data.sh [ANO] [M_INI] [M_FIM]` | Descarrega Parquets oficiais para a pasta `data/`. |
| `./scripts/submit_job.sh [FLAGS]` | Compila Kotlin e submete o job no cluster Spark. |
| `./gradlew run --args="--master local[*] ..."` | Corre o pipeline localmente sem passar pelo cluster Docker. |
