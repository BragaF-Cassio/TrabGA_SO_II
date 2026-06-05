# Escalonador Customizado de PODs (Kubernetes + Minikube)

Trabalho que implementa um **escalonador de PODs próprio** rodando sobre um cluster
Kubernetes local (Minikube). Diferente do escalonador padrão do Kubernetes — que
considera apenas **CPU e memória** — este leva em conta **4 métricas**: CPU, memória,
disco e latência de rede do worker.

A lógica de escalonamento é construída com o paradigma **produtor/consumidor** e
sincronização por **mutex**, em C.

---

## Como funciona

O programa modela o **nodo Master** (control-plane) como uma estrutura explícita
(`struct Master`) que centraliza o nome do nó control-plane, os **workers que ele
gerencia**, a **fila do Job Scheduler** e as estatísticas. Sobre ela atuam:

- **Produtor**: lê os jobs do arquivo `jobs.csv` e coloca cada POD numa fila
  compartilhada.
- **Consumidor** (o escalonador, *single-threaded*): retira um POD da fila por vez,
  escolhe o melhor worker pelo algoritmo de 4 métricas e aplica a alocação no cluster.
- **Mutex**: protege a fila de PODs e o estado de recursos dos workers contra
  condição de corrida entre as duas threads.
- PODs que não cabem em nenhum worker ficam com status **Pending** no cluster
  (reproduzindo a ideia do enunciado).

As capacidades de cada worker e os requisitos de cada job são valores **sintéticos**
(definidos no código e no `jobs.csv`) — são eles que o algoritmo usa para decidir a
alocação.

---

## Arquivos

| Arquivo             | Descrição                                                        |
|---------------------|------------------------------------------------------------------|
| `escalonador.c`     | Programa principal (produtor/consumidor + mutex + algoritmo).    |
| `jobs.csv`          | Lista de 15 jobs com CPU, memória e disco variados.              |
| `pod-template.yaml` | Molde do manifesto de POD usado para gerar os YAMLs.             |

---

## Pré-requisitos

- Minikube e kubectl instalados
- GCC (compilador C) com suporte a pthreads

---

## Como rodar

### 1. Subir o cluster (3 nós: 1 control-plane + 2 workers)

```bash
minikube start --nodes 3 --driver=docker
kubectl get nodes
```

> Confirme os nomes dos dois workers. Se forem diferentes de `minikube-m02` e
> `minikube-m03`, ajuste o array `workers[]` no início do `escalonador.c`.

### 2. Compilar

```bash
gcc -Wall -o escalonador escalonador.c -pthread
```

### 3. Executar

```bash
./escalonador
```

O programa imprime, POD a POD, onde cada um foi alocado e quanto de recurso sobrou
em cada worker, além da tabela final de ocupação.

### 4. Verificar a alocação no cluster

```bash
kubectl get pods -o wide
```

A coluna `NODE` mostra onde cada POD foi colocado. Os PODs pendentes aparecem com
status `Pending`.

### 5. Limpar (para rodar de novo)

```bash
kubectl delete pods -l app=trabalho-escalonador
```

---

## Exemplo de saída

```
=== Escalonador customizado (produtor/consumidor + mutex) ===

[ALOCADO ] Pod  1 -> minikube-m02  | livre apos: cpu=14 mem=28 disco=360
[ALOCADO ] Pod  2 -> minikube-m03  | livre apos: cpu= 7 mem=14 disco=180
...
[PENDENTE] Pod 11 (cpu=3 mem=6 disco=50) nao coube em nenhum worker

=== Estado final dos workers (ocupado/total) ===
minikube-m02  | cpu 16/16 | mem 32/32 | disco 280/400 | lat 5ms
minikube-m03  | cpu  7/ 8 | mem 14/16 | disco 110/200 | lat 20ms
```

---

## Algoritmo de escalonamento

Para cada POD, o escalonador percorre os workers e:

1. Descarta os que **não comportam** o POD em CPU, memória **ou** disco.
2. Entre os que comportam, escolhe o de **maior score**, onde o score favorece quem
   deixa mais recursos livres e penaliza maior latência:

   ```
   score = folga_cpu + folga_mem + folga_disco - (latencia / 100)
   ```

Se nenhum worker comporta o POD, ele é marcado como **Pending**.

Ao final, o programa também imprime um bloco de **estatísticas**: total de PODs
alocados e pendentes (com percentuais), número de PODs e ocupação de CPU, memória e
disco por worker, e a ocupação média de CPU no cluster.

---

## Comparação com o escalonador padrão do Kubernetes

O escalonador padrão do Kubernetes filtra os nós pelos recursos solicitados
(`requests` de CPU e memória contra o disponível no nó) e depois pontua os candidatos
basicamente por **CPU e memória**. Ele **não considera latência de rede entre os nós**
nem métricas customizadas como as usadas aqui.

| Aspecto                       | Escalonador padrão do K8s            | Solução proposta                          |
|-------------------------------|--------------------------------------|-------------------------------------------|
| Métricas na decisão           | CPU e memória                        | CPU, memória, disco e latência (4)        |
| Latência entre nós            | Não considera                        | Considera (penaliza no score)             |
| Disco como critério de score  | Não no comportamento básico          | Sim                                       |
| Como estender                 | Exige plugin/scheduler em Go         | Editar o cálculo do score no `escalonador.c` |
| Paradigma de execução         | Concorrência interna (goroutines)    | Produtor/consumidor *single-threaded* + mutex |

### Exemplo em que as decisões divergem

Suponha dois workers com a **mesma** folga de CPU e memória, mas:

- `minikube-m02`: latência 5 ms, mais disco livre
- `minikube-m03`: latência 20 ms, menos disco livre

O escalonador padrão tende a tratar os dois como equivalentes (decide só por CPU/memória)
e pode colocar o POD em qualquer um. A solução proposta escolhe **sempre o m02**, porque
o disco e a latência entram no score — exatamente a vantagem que o enunciado pede.

### Como comprovar o comportamento do padrão

Crie um POD **sem** `nodeName` (assim ele passa pelo escalonador padrão):

```bash
kubectl run teste-padrao --image=busybox --restart=Never -- sleep 3600
kubectl get pod teste-padrao -o wide
```

Observe que o POD foi colocado sem nenhuma referência a disco ou latência — o padrão
não enxerga as capacidades sintéticas que a nossa solução usa. Para remover depois:

```bash
kubectl delete pod teste-padrao
```

### Vídeo da apresentação

https://youtu.be/HYFDN4n_Ea8
