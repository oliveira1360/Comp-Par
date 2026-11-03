# Resultados de desempenho — todas as versões (execução local)

Material de base para o relatório: cada operação (M1, M2, M3, O1, O4) com todas as suas versões, o que cada
uma muda e o que foi medido. Todos os números vêm da mesma bateria de testes (3 de novembro de 2026, 00:34–00:58),
corrida com `scripts/local_bench/matrix.sh` e agregada com `scripts/local_bench/analyze.sh` (`compar.bench.AnalyzeKt`).

> **Ainda não está aqui:** os resultados no cluster (Docker Spark ou cluster do DI), que o enunciado exige.
> Estes resultados são todos em `local[*]` numa única máquina (ver §1 e §10).

## Resumo

| Operação | v0 (s) | melhor versão | melhor (s) | speedup | Principal ganho |
|---|---|---|---|---|---|
| M1 | 11.27 | v3 | 3.14 | **3.59×** | 1 leitura em vez de 3; leitura colunar direta, sem shuffle |
| M2 | 6.21 | v4 | 5.88 | 1.06× | nenhum significativo: a operação é limitada pela leitura do Parquet |
| M3 | 12.52 | v3 | 4.71 | **2.66×** | 1 leitura em vez de 2; windows no driver em vez de 4 shuffles |
| O1 | 12.90 | v5–v7 | 7.88–7.96 | **1.64×** | window → groupBy + broadcast: shuffle de 424 MB → 2,4 MB, spill de 1,7 GB → 0 |
| O4 | 13.49 | v0 (só baseline) | — | — | por otimizar (v1 é um esqueleto TODO) |
| **Pipeline completo** | **63.54** | ALL_best | **28.06** | **2.26×** | soma dos tempos das 5 operações numa só execução |

Tempos = mediana de 3 execuções do *wall time* da operação, cada uma num JVM novo.

---

## 1. Ambiente de teste

**Hardware**

| | |
|---|---|
| Máquina | Lenovo 83JX (portátil) |
| CPU | Intel Core Ultra 7 256V — 8 cores / 8 threads (4 de desempenho + 4 de eficiência, sem hyper-threading), 2,2 GHz base, L2 14 MB, L3 12 MB |
| RAM | 16 GB LPDDR5X-8533 (≈ 15,9 GB visíveis) |
| Disco | SSD NVMe SK Hynix 1 TB |
| Sistema | Windows 11 Home 10.0.26200, plano de energia "Equilibrado" |

**Software**

| | |
|---|---|
| JVM | Eclipse Temurin OpenJDK 17.0.20.1 (toolchain do Gradle) |
| Linguagem | Kotlin 2.0.21 (acordado com o professor em vez de PySpark) |
| Spark | 3.5.3 (Scala 2.12), Hadoop client 3.3.4, Parquet 1.13.1, zstd-jni 1.5.5-4 |
| Modo | `local[*]` → 8 threads de execução, `defaultParallelism` = 8 |
| Heap | `-Xmx4g` (driver e executores são o mesmo JVM em modo local; `spark.driver.memory=2g` do código não tem efeito aqui) |
| Configuração não-default | `spark.sql.adaptive.enabled=true` (já é default no 3.5), `spark.ui.showConsoleProgress=false`; restantes valores default (ex.: `spark.sql.shuffle.partitions=200`, `spark.sql.autoBroadcastJoinThreshold=10MB`) |
| Código | commit `00eca01`; a única alteração local era um comentário removido em `M3V3.kt` (e o esqueleto `M3V4.kt`, não usado) |

**Dados**

- NYC Yellow Taxi 2024, 12 ficheiros mensais Parquet, 662 MB no total, **41 169 720 registos**.
- Depois da limpeza (M1 v1–v3): 35 274 802 registos válidos (85,68 %).
- `taxi_zone_lookup.csv`: 265 linhas, ≈ 12 KB.

## 2. Metodologia

- **Um JVM novo por execução**: nada de cache do Spark, JIT ou memória passa de um teste para o outro.
  (A cache de ficheiros do sistema operativo fica quente; com 662 MB e 16 GB de RAM, todos os testes leem do mesmo estado.)
- **3 rondas**; em cada ronda a ordem das 27 configurações roda 7 posições, para nenhuma versão ficar sempre em primeiro ou em último.
  Valor reportado: mediana, com mínimo e máximo.
- **Cada operação de análise corre depois do M1 v3** (`--ops M1,X --m1 v3`), porque usa o DataFrame limpo do M1 como entrada.
  O tempo reportado é só o da operação X (o profiler mede cada operação em separado).
  As operações não fazem cache da entrada, por isso cada uma volta a ler o Parquet.
- **ALL_v0** corre o pipeline completo com todas as baselines; **ALL_best** com as melhores versões
  (`--m1 v3 --m2 v7 --m3 v3 --o1 v7 --o4 v0`).
- **Métricas**: `SparkProfiler` (API REST da UI do Spark), exportadas em JSON com `--export-metrics`.
  *wall* = tempo da operação; *stages/tasks* = contagem; *CPU* e *GC* = soma em todas as tasks;
  *input* = bytes lidos pelas tasks; *shuffle R/W* = bytes lidos/escritos no shuffle; *spill* = memória + disco;
  *peak exec. mem* = maior valor de `peakExecutionMemory` entre os stages.
  Atenção: esse valor por stage é a **soma** dos picos das tasks do stage (ver M3 v1).
- **Windows sem `winutils.exe`**: listar uma pasta e escrever Parquet usam a biblioteca nativa do Hadoop.
  Só no benchmark, `run1.sh` troca o sistema de ficheiros local por `compar.bench.NoNativeLocalFs`
  (operações de permissões vazias, listagem com `java.io`). O código das operações não muda.
- **Correção**: as saídas de todas as versões são comparadas com a v0 (§8).
- **Variação entre baterias**: corremos a bateria completa três vezes na mesma máquina, com o mesmo código.
  O pipeline completo deu 58,0 / 71,2 / 63,5 s com as baselines e 26,3 / 30,8 / 28,1 s com as melhores versões
  (speedup 2,20 / 2,31 / 2,26×). O tempo de CPU muda na mesma proporção, o que aponta para a frequência do processador
  (temperatura ou energia) e não para o código. A ordem das versões e as conclusões são as mesmas nas três.
  Diferenças abaixo de ≈ 5–7 % entre versões estão dentro do ruído. Este documento usa a terceira bateria.

**Reproduzir**

```bash
./scripts/local_bench/matrix.sh
```

```bash
./scripts/local_bench/analyze.sh
```

Resultados em `build/local-bench/` (`summary.md`, `summary.csv`, `env.txt`, logs e JSON de cada execução).
Variáveis opcionais: `ROUNDS`, `ONLY` (regex de configurações, ex. `'^M3_'`), `BENCH_DATA`, `BENCH_HEAP`, `BENCH_OUT`.
A bateria completa (27 configurações × 3 rondas = 81 execuções) demora ≈ 25 min.

---

## 3. M1 — Ingestão e limpeza

| versão | wall (s) mediana [min–max] | vs v0 | vs anterior | stages | tasks | CPU (s) | GC (s) | input (MB) | shuffle R/W (MB) | spill (MB) | peak exec. mem (MB) |
|---|---|---|---|---|---|---|---|---|---|---|---|
| v0 | 11.27 [11.25–11.67] | 1.00× | — | 10 | 76 | 38.5 | 1.9 | 1089.9 | 0.0 / 0.0 | 0.0 | 17.0 |
| v1 | 5.65 [5.62–5.67] | 1.99× | 1.99× | 4 | 26 | 10.6 | 0.6 | 539.1 | 0.0 / 0.0 | 0.0 | 17.0 |
| v2 | 5.22 [4.95–5.29] | 2.16× | 1.08× | 4 | 26 | 12.1 | 0.7 | 539.1 | 0.0 / 0.0 | 0.0 | 17.0 |
| v3 | 3.14 [3.13–3.21] | 3.59× | 1.66× | 2 | 13 | 6.9 | 0.5 | 0.0 ¹ | 0.0 / 0.0 | 0.0 | 0.0 ¹ |

¹ A v3 lê os ficheiros diretamente nas tasks, fora do `FileScan` do Spark, por isso o Spark não conta esses bytes nem a memória.
Os mesmos 12 ficheiros são lidos.

### v0 — baseline
Faz cast explícito de todas as colunas, conta os registos inválidos com `count()` separados e calcula as estatísticas por ano.
São **3 leituras completas** dos dados (10 stages, 1,09 GB lidos, 38,5 s de CPU).
Tem ainda um erro de correção: converte `timestamp_ntz` para `timestamp` com o fuso de Lisboa.
Na mudança de hora de 31/03 (01:00–02:00 não existe em Lisboa) algumas horas mudam e **493 viagens** passam a ter `dropoff <= pickup`
(35 274 309 válidos em vez de 35 274 802).

### v1 — uma só leitura e cast só onde é preciso
- **Gargalo:** 3 leituras completas; casts em todas as linhas mesmo quando o tipo já serve.
- **Alteração:** uma única agregação calcula o total, os 5 critérios e os válidos; o schema é verificado uma vez e só as colunas
  que precisam são convertidas (com os dados de 2024: nenhuma).
- **Resultado:** 11,27 → 5,65 s (**1,99×**). Input 1090 → 539 MB, stages 10 → 4, CPU 38,5 → 10,6 s. Corrige também o erro do fuso.

### v2 — máscara de validade
- **Gargalo:** cada uma das 7 somas avalia a sua própria condição em todas as linhas.
- **Alteração:** cada linha recebe um inteiro com um bit por critério; agrega-se só por (ano, máscara), umas dezenas de grupos,
  e o driver calcula todos os contadores a partir daí.
- **Resultado:** 5,65 → 5,22 s (**1,08×**). O CPU total sobe ligeiramente (10,6 → 12,1 s) mas o tempo baixa:
  menos expressões por linha e uma agregação mais pequena.

### v3 — leitura colunar direta
- **Gargalo:** o Spark transforma cada batch colunar em linhas e faz um shuffle, quando o único custo inevitável é descodificar o Parquet.
- **Alteração:** as tasks leem os batches de colunas diretamente, constroem as máscaras em ciclos simples e contam por (dia, máscara)
  num array: sem linhas e sem shuffle. O ano é calculado uma vez por dia no driver. Com amostragem ou outros tipos de coluna usa a v2.
- **Resultado:** 5,22 → 3,14 s (**1,66×**). 2 stages, 13 tasks (uma por ficheiro + driver), CPU 6,9 s, shuffle 0.
  Medimos antes que só descodificar as colunas custa ≈ 3,1 s, por isso a v3 está praticamente nesse limite.

---

## 4. M2 — Procura temporal

| versão | wall (s) mediana [min–max] | vs v0 | vs anterior | stages | tasks | CPU (s) | GC (s) | input (MB) | shuffle R/W (MB) | spill (MB) | peak exec. mem (MB) |
|---|---|---|---|---|---|---|---|---|---|---|---|
| v0 | 6.21 [6.08–6.22] | 1.00× | — | 11 | 55 | 16.6 | 0.6 | 540.9 | 1.4 / 0.7 | 0.0 | 217.1 |
| v1 | 5.79 [5.79–6.00] | 1.07× | 1.07× | 11 | 55 | 15.3 | 0.6 | 540.9 | 1.4 / 0.7 | 0.0 | 217.1 |
| v2 | 5.98 [5.67–6.16] | 1.04× | 0.97× | 9 | 42 | 16.8 | 0.6 | 540.9 | 0.7 / 0.7 | 0.0 | 217.1 |
| v3 | 6.92 [6.72–7.05] | 0.90× | 0.86× | 13 | 78 | 20.0 | 1.0 | 540.9 | 8.8 / 8.8 | 0.0 | 281.0 |
| v4 | 5.88 [5.69–6.04] | 1.06× | 1.18× | 9 | 42 | 16.7 | 0.6 | 540.9 | 0.6 / 0.6 | 0.0 | 17.0 |
| v5 | 6.60 [6.46–6.64] | 0.94× | 0.89× | 9 | 42 | 20.6 | 0.8 | 540.9 | 0.6 / 0.6 | 0.0 | 17.0 |
| v6 | 6.57 [6.54–6.94] | 0.95× | 1.00× | 6 | 28 | 20.5 | 0.9 | 540.9 | 0.5 / 0.5 | 0.0 | 33.9 |
| v7 | 6.41 [6.31–6.51] | 0.97× | 1.02× | 5 | 27 | 20.7 | 0.8 | 540.9 | 0.5 / 0.5 | 0.0 | 33.9 |

**Conclusão geral:** todas as versões ficam entre 5,8 e 6,9 s. O M2 já é limitado pela leitura e descodificação do Parquet
(input igual em todas, 541 MB): o shuffle da v0 já era só 1,4 MB, por isso não havia muito para ganhar.
As v1, v2 e v4 estão empatadas dentro do ruído; a **v4** é a recomendada (sem join, 17 MB de memória, resultado correto).
As v5–v7 são 9–12 % mais lentas que a v4, com ≈ 23 % mais CPU, e têm um erro de correção (abaixo).

### v0 — baseline
Join com as zonas, `groupBy(borough, dia, hora)` com contagem e médias, `orderBy` e escrita particionada por borough.

### v1 — projeção de colunas, broadcast, uma só ação
- **Alteração:** `select` das 5 colunas, `broadcast` da tabela de zonas, remoção do `show()`.
- **Resultado:** 6,21 → 5,79 s (1,07×), mas **sem ganho atribuível**: input (541 MB), stages, tasks e shuffle são iguais aos da v0,
  e nas três baterias a v1 deu 1,00×, 1,02× e 1,07×. O Catalyst já fazia a projeção (column pruning) e já escolhia broadcast
  (12 KB < `autoBroadcastJoinThreshold` de 10 MB).

### v2 — escrita alinhada com as partições
- **Alteração:** `repartition(pickup_borough)` em vez do `orderBy` global antes de `partitionBy`.
- **Resultado:** 5,79 → 5,98 s (0,97×, dentro do ruído). Stages 11 → 9, shuffle 1,4 → 0,7 MB
  (deixa de haver o stage de amostragem do `orderBy`).

### v3 — chave de 17 bits e join depois da agregação
- **Alteração:** agrupa primeiro por (zona, dia, hora) num inteiro de 17 bits (até 44 184 grupos) e só depois faz o join e soma por borough.
- **Resultado:** 5,98 → 6,92 s (**0,86×, mais lento**). Shuffle 0,7 → 8,8 MB, stages 9 → 13: agrupar por zona cria 37× mais grupos
  do que por borough, e a segunda agregação custa mais do que o join que evita.

### v4 — chave de 11 bits por borough, sem join
- **Alteração:** a tabela de zonas é resolvida no driver (zona → borough 0..6); chave (borough, dia, hora) em 11 bits, no máximo 1176 grupos.
  Sem join e sem segunda agregação.
- **Resultado:** 6,92 → 5,88 s (1,18× face à v3). Peak de memória 217 → 17 MB.

### v5 — lookup por array e divisão adiada
- **Alteração:** `element_at` sobre um array de 263 posições em vez de `when … in`; duração em segundos com cast direto para `Long`,
  dividida por 60 só no fim.
- **Resultado:** 5,88 → 6,60 s (**0,89×, mais lento**), CPU 16,7 → 20,6 s.
- **Erro de correção:** `cast(timestamp_ntz → timestamp)` usa o fuso de Lisboa, enquanto o `unix_timestamp` da v0 não usa.
  As viagens que tocam a hora da mudança de hora de Lisboa (31/03: 6247 viagens; 27/10: 8766) ficam com ±60 min.
  **34 dos 959 grupos** têm `mean_trip_duration_min` diferente, até **7,7 %** (ex.: Bronx, domingo, 01h: 14,47 → 15,59 min).
  Os grupos afetados são domingo às 0h e 1h e vários de sábado: viagens que começam no sábado e acabam na madrugada de domingo
  (incluindo viagens de muitas horas que o M1 não filtra). Afeta também a v6 e a v7.

### v6 — uma só agregação e `coalesce(1)`
- **Alteração:** `coalesce(1)` em vez de `repartition` sobre ≤ 1176 linhas.
- **Resultado:** 6,60 → 6,57 s (1,00×), stages 9 → 6.

### v7 — leitura do CSV de zonas no driver
- **Alteração:** lê o CSV de zonas com I/O normal no driver, sem stages do Spark.
- **Resultado:** 6,57 → 6,41 s (1,02×), stages 6 → 5. Continua 9 % mais lenta que a v4 (CPU 20,7 vs 16,7 s).

**Sugestão:** juntar à v7 a duração da v4 (`unix_timestamp`, ou o fuso da sessão em UTC) corrige o erro
e deve recuperar o CPU perdido na v5.

---

## 5. M3 — Estatísticas por zona

| versão | wall (s) mediana [min–max] | vs v0 | vs anterior | stages | tasks | CPU (s) | GC (s) | input (MB) | shuffle R/W (MB) | spill (MB) | peak exec. mem (MB) |
|---|---|---|---|---|---|---|---|---|---|---|---|
| v0 | 12.52 [12.04–12.77] | 1.00× | — | 24 | 112 | 46.4 | 1.8 | 1081.8 | 1.3 / 1.3 | 0.0 | 17.0 |
| v1 | 10.09 [9.95–10.16] | 1.24× | 1.24× | 38 | 734 | 27.9 | 1.0 | 541.6 | 1.5 / 1.3 | 0.0 | 3368.5 ² |
| v2 | 7.69 [7.55–7.77] | 1.63× | 1.31× | 38 | 734 | 15.8 | 0.6 | 541.6 | 1.3 / 1.1 | 0.0 | 3368.5 ² |
| v3 | 4.71 [4.66–4.74] | 2.66× | 1.63× | 6 | 28 | 12.2 | 0.5 | 540.9 | 0.3 / 0.3 | 0.0 | 17.0 |

² Soma dos picos das 200 tasks de um stage (200 × ≈ 16,8 MB). Não é memória ocupada ao mesmo tempo (ver v1).

### v0 — baseline
Agrega por (zona, mês) com média e desvio padrão de tarifa e distância e usa windows para o rank no borough,
a quota móvel de 3 meses e a flag de desvio. Faz `show()` e `count()` como ações separadas,
por isso tudo é lido e agregado **duas vezes** (1,08 GB de input, 24 stages).
Correção feita nesta versão e em todas: a média e o desvio móveis usam os 3 meses **anteriores** (`rowsBetween(-3, -1)`).
Com o mês atual incluído a flag nunca podia disparar: com 3 valores, |x − média| / s ≤ 1,155 < 1,5.

### v1 — uma leitura e broadcast join
- **Gargalo:** duas ações = duas leituras; join sem broadcast explícito.
- **Alteração:** `cache()` da tabela final (≈ 3 mil linhas) e `broadcast` das zonas.
- **Resultado:** 12,52 → 10,09 s (**1,24×**). Input 1082 → 542 MB, CPU 46,4 → 27,9 s.
  Efeito secundário: o resultado em cache fica com as 200 partições de `spark.sql.shuffle.partitions` (o AQE não as junta),
  por isso tasks 112 → 734. É daí que vem o peak de 3,4 GB, que é a soma das 200 tasks e não memória real.

### v2 — chave inteira de mês e agregação antes do join
- **Gargalo:** `date_format` cria uma string por cada uma das 35 M linhas, e borough e zona (strings) vão na chave de agrupamento.
- **Alteração:** agrupa por (`PULocationID`, `ano*100 + mês`), só inteiros; o join e a string "yyyy-MM" correm sobre ≈ 3 mil linhas.
- **Resultado:** 10,09 → 7,69 s (**1,31×**), CPU 27,9 → 15,8 s.

### v3 — windows no driver
- **Gargalo:** depois da agregação sobram ≈ 3 mil linhas, mas cada uma das window specs continua a precisar de shuffle e sort próprios,
  com tasks agendadas para muito poucos dados (38 stages).
- **Alteração:** as linhas agregadas são recolhidas (`collectAsList`) e o rank, a quota móvel e a flag (desvio com Welford)
  são calculados em ciclos no driver; o resultado volta com `createDataFrame` e schema explícito. Só fica o shuffle da agregação.
- **Resultado:** 7,69 → 4,71 s (**1,63×**). Stages 38 → 6, tasks 734 → 28, shuffle 0,3 MB, CPU 12,2 s.

Verificado: as 42 588 células do resultado são iguais nas 4 versões (no laboratório) e as 4 marcam **975 zonas-mês** (§8).

---

## 6. O1 — Deteção de anomalias de tarifa

| versão | wall (s) mediana [min–max] | vs v0 | vs anterior | stages | tasks | CPU (s) | GC (s) | input (MB) | shuffle R/W (MB) | spill (MB) | peak exec. mem (MB) |
|---|---|---|---|---|---|---|---|---|---|---|---|
| v0 | 12.90 [12.74–13.13] | 1.00× | — | 16 | 103 | 53.5 | 1.1 | 546.0 | 423.9 / 423.9 | 1719.3 | 2142.5 |
| v1 | 12.62 [12.55–12.82] | 1.02× | 1.02× | 16 | 103 | 52.2 | 1.5 | 546.0 | 423.9 / 423.9 | 1719.3 | 2142.5 |
| v2 | 12.55 [12.19–12.58] | 1.03× | 1.01× | 16 | 103 | 51.2 | 1.2 | 546.0 | 423.9 / 423.9 | 1719.3 | 2142.5 |
| v3 | 8.67 [8.54–8.90] | 1.49× | 1.45× | 14 | 81 | 29.0 | 1.2 | 1086.8 | 2.9 / 2.9 | 0.0 | 204.5 |
| v4 | 8.83 [8.71–9.08] | 1.46× | 0.98× | 14 | 81 | 29.7 | 1.2 | 1086.8 | 2.4 / 2.4 | 0.0 | 34.1 |
| v5 | 7.96 [7.94–7.99] | 1.62× | 1.11× | 13 | 80 | 24.0 | 0.9 | 1086.8 | 2.4 / 2.4 | 0.0 | 34.1 |
| v6 | 7.88 [7.87–7.93] | 1.64× | 1.01× | 13 | 80 | 22.2 | 1.1 | 1086.8 | 2.4 / 2.4 | 0.0 | 34.1 |
| v7 | 7.95 [7.91–8.08] | 1.62× | 0.99× | 13 | 80 | 22.9 | 0.9 | 1086.8 | 2.4 / 2.4 | 0.0 | 34.1 |

**Porque foi escolhida (opcional):** expõe o custo de uma window sem agregação parcial: todas as viagens passam pelo shuffle,
e há spill e skew nas zonas movimentadas.

### v0 — baseline
`fare_per_km`, média e desvio por (zona, hora) com uma window sobre todas as viagens, flag `> μ + 3σ`,
join com as zonas e taxa de anomalias por pagamento, borough e tipo de dia.
A window obriga a **enviar todas as viagens para o shuffle**: 424 MB de shuffle, **1,7 GB de spill**, peak 2,1 GB.

### v1 — projeção numa só select
- **Resultado:** 12,90 → 12,62 s (1,02×), **sem ganho**. Input (546 MB), stages e shuffle iguais à v0: o Catalyst já fazia o
  column pruning e juntava os `withColumn` (CollapseProject).

### v2 — broadcast explícito das zonas
- **Resultado:** 12,62 → 12,55 s (1,01×), **sem ganho**. Métricas iguais: o join já era BroadcastHashJoin automático
  (12 KB < 10 MB). O custo está todo na window.

### v3 — groupBy + broadcast em vez da window
- **Gargalo:** a window não tem agregação parcial (map-side combine): as 35 M viagens vão para o shuffle e as zonas movimentadas criam skew.
- **Alteração:** μ e σ por (zona, hora) com `groupBy` (≤ 6312 linhas, com agregação parcial) e broadcast desses limiares de volta às viagens.
- **Resultado:** 12,55 → 8,67 s (**1,45×**). Shuffle 424 → 2,9 MB (−99,3 %), spill 1,7 GB → 0, peak 2142 → 205 MB, CPU 51,2 → 29,0 s.
  Custo: duas leituras em vez de uma (input 546 → 1087 MB), que compensa largamente.

### v4 — chaves inteiras empacotadas
- **Alteração:** chave de bucket com 14 bits (zona + hora), borough resolvido no driver (sem join), chave de taxa com 8 bits
  (≤ 84 grupos), strings só nas linhas finais.
- **Resultado:** 8,67 → 8,83 s (0,98×, dentro do ruído). Peak 205 → 34 MB.

### v5 — sem joins: arrays O(1) e `coalesce(1)`
- **Alteração:** CSV de zonas lido no driver; limiares recolhidos para um `DoubleArray` de 67 KB indexado pela chave,
  substituindo o BroadcastHashJoin por `element_at`.
- **Resultado:** 8,83 → 7,96 s (**1,11×**), CPU 29,7 → 24,0 s.

### v6 — projeção fundida e acumuladores inteiros
- **Alteração:** cálculo da chave, lookup e flag dentro do próprio `agg`; só contagem e soma inteiras, percentagem no fim.
- **Resultado:** 7,96 → 7,88 s (1,01×), dentro do ruído; CPU 24,0 → 22,2 s.

### v7 — invariância de escala do limiar
- **Alteração:** o limiar μ + 3σ é invariante à escala (c = 1/1,60934 > 0), por isso compara `fare / distance` sem converter para km.
- **Resultado:** 7,88 → 7,95 s (0,99×), dentro do ruído; resultado idêntico.

As v4–v7 ganham juntas ≈ 10 % sobre a v3, quase tudo na v5; as v5, v6 e v7 estão empatadas. O grande salto é a v3.
O O1 está agora limitado pelas duas leituras do Parquet.

---

## 7. O4 — Classificação do tipo de viagem

| versão | wall (s) mediana [min–max] | stages | tasks | CPU (s) | GC (s) | input (MB) | shuffle R/W (MB) | spill (MB) | peak exec. mem (MB) |
|---|---|---|---|---|---|---|---|---|---|
| v0 | 13.49 [12.77–13.53] | 11 | 84 | 59.2 | 2.1 | 1152.5 | 584.9 / 584.9 | 0.0 | 204.5 |

Só existe a baseline (a `O4V1` é um esqueleto TODO). Frações de aeroporto, tarifa negociada e grupo por (zona, hora),
classificação de cada bucket, join de volta à tabela completa e estatísticas por tipo e mês.

**O que as métricas mostram:** 585 MB de shuffle e o maior CPU de todas as operações (59 s). É a mesma assinatura do O1 v0:
as viagens passam pelo shuffle para voltarem a juntar-se a uma tabela pequena (≤ 6312 buckets).
**Hipótese para a v1 (por verificar com `explain`):** fazer broadcast da classificação (como no O1 v3),
ou resolvê-la num array no driver (O1 v5), deve eliminar quase todo esse shuffle.

---

## 8. Correção dos resultados

Saídas da ronda 1 de cada versão comparadas com a v0 (`analyze.sh`):

| Operação | Resultado |
|---|---|
| M1 v1–v3 | iguais entre si: 41 169 720 lidos, **35 274 802** válidos. A v0 dá 35 274 309 por causa do erro de fuso (§3). |
| M2 v1–v4 | as 959 linhas iguais à v0 (diferença relativa máxima 5,1 × 10⁻¹⁵, arredondamento de vírgula flutuante na v3). |
| M2 v5–v7 | **34 das 959** linhas com `mean_trip_duration_min` diferente, até 7,7 % (erro de fuso, §4). |
| M3 v1–v3 | 975 zonas-mês marcadas em todas; as 42 588 células iguais (verificado no laboratório). As linhas do `show(20)` só mudam de ordem entre empates. |
| O1 v1–v7 | as 48 linhas iguais à v0 (diferença 0). |
| Execuções | as 81 terminaram com `exit=0` e nenhum log tem exceções. |

---

## 9. Pipeline completo

Tempo de cada operação dentro de uma só execução com as 5 operações (mediana de 3):

| Operação | ALL_v0 (s) | ALL_best (s) | speedup |
|---|---|---|---|
| M1 | 11.53 | 3.09 | 3.73× |
| M2 | 8.36 | 6.54 | 1.28× |
| M3 | 14.23 | 2.62 | 5.43× |
| O1 | 13.25 | 5.17 | 2.56× |
| O4 (v0 nos dois) | 16.17 | 10.61 | 1.52× |
| **Total** | **63.54 [62.21–65.76]** | **28.06 [27.47–28.50]** | **2.26×** |

Tempo total do processo (arranque do JVM e do Spark incluído): 67–71 s → 32–34 s.

Duas observações:
- No ALL_best as operações são mais rápidas do que isoladas (M3 v3: 2,62 vs 4,71 s; O1 v7: 5,17 vs 7,95 s),
  porque o JIT e os metadados do Spark já estão aquecidos pelas operações anteriores.
  Por isso as comparações entre versões usam as execuções isoladas.
- O O4 é o mesmo código nos dois casos e passa de 16,17 para 10,61 s: em ALL_v0 a entrada é o DataFrame do M1 v0,
  com casts e filtros mais caros, que volta a ser calculado a cada leitura porque não há cache.

---

## 10. Limitações e próximos passos

1. **Cluster (obrigatório):** falta correr no Docker Spark (`./gradlew benchmark`, que precisa do Docker Desktop ligado) e/ou no
   cluster do DI, e a análise de escalabilidade: amostra de 10–20 % contra dados completos, e 2 nós contra a alocação completa.
   A escalabilidade local (meses e cores) está no relatório LaTeX (`docs/report.tex`, secção 5).
2. **Planos de execução:** o plano físico do O1 v0 e da v3 está no relatório LaTeX (figura 1).
3. **O4 v1** e o **M3 V4** (esqueleto TODO) estão por fazer.
4. **M2 v5–v7:** corrigir o cálculo da duração (§4).
5. **Uma só máquina, 3 repetições:** diferenças abaixo de ≈ 5–7 % (M2 v1/v2/v6/v7, O1 v1/v2/v4/v6/v7) estão dentro do ruído,
   e os tempos absolutos variam até ≈ 20 % entre baterias (§2).
6. **Sistema de ficheiros do benchmark:** no Windows usa `NoNativeLocalFs`; afeta só a listagem e a escrita
   (permissões), não a leitura nem o cálculo.
7. As métricas *input* e *peak* da M1 v3 ficam a 0 porque a leitura não passa pelo `FileScan` do Spark.
