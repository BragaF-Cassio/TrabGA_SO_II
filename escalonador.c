#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>

#define MAX_FILA 64
#define NUM_WORKERS 2

/* ----------------------------------------------------------------
 *  Pod = um "job" a ser escalonado.
 *  Os valores de cpu/mem/disco sao SINTETICOS (capacidade
 *  computacional, como pede o enunciado), nao os requests reais
 *  do Kubernetes. Quem decide a alocacao e o nosso algoritmo.
 * ---------------------------------------------------------------- */
typedef struct {
    int id;
    int cpu;
    int mem;
    int disco;
} Pod;

/* ----------------------------------------------------------------
 *  Worker = um nodo do cluster com suas capacidades sinteticas.
 *  IMPORTANTE: confirme os nomes com `kubectl get nodes` e ajuste
 *  abaixo se forem diferentes na sua maquina.
 * ---------------------------------------------------------------- */
typedef struct {
    char nome[40];
    int cpu_total,  mem_total,  disco_total;
    int cpu_livre,  mem_livre,  disco_livre;
    int latencia;            /* ms (metrica extra, alem de cpu/mem) */
} Worker;

/* ----------------------------------------------------------------
 *  Master = estrutura do nodo control-plane.
 *  Centraliza tudo o que pertence ao Master: os workers que ele
 *  gerencia, a fila do Job Scheduler (PODs aguardando alocacao)
 *  e as estatisticas do escalonamento.
 * ---------------------------------------------------------------- */
typedef struct {
    char nome[40];                  /* nome do nodo control-plane */
    char papel[24];                 /* "control-plane" */

    Worker workers[NUM_WORKERS];    /* workers gerenciados pelo Master */
    int num_workers;

    /* Fila do Job Scheduler (buffer produtor/consumidor) */
    Pod fila[MAX_FILA];
    int ini, fim, qtd;
    int produtor_terminou;

    /* Estatisticas */
    int total_alocados;
    int total_pendentes;
    int pods_por_worker[NUM_WORKERS];
} Master;

Master master = {
    .nome  = "minikube",
    .papel = "control-plane",
    .workers = {
        /* nome           cpuT memT discoT  cpuL memL discoL  lat */
        { "minikube-m02",  16,  32,   400,   16,  32,   400,   5 },
        { "minikube-m03",   8,  16,   200,    8,  16,   200,  20 }
    },
    .num_workers       = NUM_WORKERS,
    .ini = 0, .fim = 0, .qtd = 0,
    .produtor_terminou = 0,
    .total_alocados    = 0,
    .total_pendentes   = 0,
    .pods_por_worker   = {0}
};

/* ---------------- Sincronizacao (protege os dados do Master) ----- */
pthread_mutex_t mtx_fila    = PTHREAD_MUTEX_INITIALIZER;  /* protege a fila    */
pthread_cond_t  tem_item    = PTHREAD_COND_INITIALIZER;   /* sinaliza: ha pod  */
pthread_cond_t  tem_espaco  = PTHREAD_COND_INITIALIZER;   /* sinaliza: ha vaga */
pthread_mutex_t mtx_workers = PTHREAD_MUTEX_INITIALIZER;  /* protege os workers */

/* ================================================================
 *  Mostra a definicao do nodo Master (criterio 2)
 * ================================================================ */
static void imprimir_master(void) {
    printf("=== Nodo Master (control-plane) ===\n");
    printf("Nome             : %s\n", master.nome);
    printf("Papel            : %s\n", master.papel);
    printf("Job Scheduler    : produtor/consumidor single-threaded + mutex\n");
    printf("Capacidade da fila: %d PODs\n", MAX_FILA);
    printf("Workers gerenciados: %d\n", master.num_workers);
    for (int i = 0; i < master.num_workers; i++) {
        Worker *w = &master.workers[i];
        printf("  - %-13s | cpu %2d | mem %2d | disco %3d | lat %dms\n",
               w->nome, w->cpu_total, w->mem_total, w->disco_total, w->latencia);
    }
    printf("\n");
}

/* ================================================================
 *  Auxiliares para gerar o YAML a partir do template
 * ================================================================ */
static char *ler_arquivo(const char *path) {
    FILE *f = fopen(path, "rb");
    if (!f) { perror("erro ao abrir pod-template.yaml"); exit(1); }
    fseek(f, 0, SEEK_END);
    long n = ftell(f);
    fseek(f, 0, SEEK_SET);
    char *buf = malloc(n + 1);
    fread(buf, 1, n, f);
    buf[n] = '\0';
    fclose(f);
    return buf;
}

/* substitui a 1a ocorrencia de "find" por "repl" (string nova) */
static char *substituir(const char *orig, const char *find, const char *repl) {
    char *pos = strstr(orig, find);
    if (!pos) return strdup(orig);
    size_t pre = pos - orig;
    size_t lenFind = strlen(find), lenRepl = strlen(repl);
    char *res = malloc(strlen(orig) - lenFind + lenRepl + 1);
    memcpy(res, orig, pre);
    memcpy(res + pre, repl, lenRepl);
    strcpy(res + pre + lenRepl, pos + lenFind);
    return res;
}

/* gera o manifesto e aplica no cluster com kubectl */
static void aplicar_no_cluster(Pod pod, const char *node, int pendente) {
    char nome[32];
    snprintf(nome, sizeof(nome), "pod-%d", pod.id);

    char placement[128];
    if (pendente)
        /* nenhum nodo tem este label -> o pod fica PENDING no cluster */
        strcpy(placement, "  nodeSelector:\n    indisponivel: \"verdadeiro\"");
    else
        snprintf(placement, sizeof(placement), "  nodeName: %s", node);

    char *tpl = ler_arquivo("pod-template.yaml");
    char *t1  = substituir(tpl, "__NOME__", nome);
    char *t2  = substituir(t1,  "__PLACEMENT__", placement);

    char arq[64];
    snprintf(arq, sizeof(arq), "%s.yaml", nome);
    FILE *f = fopen(arq, "w");
    fputs(t2, f);
    fclose(f);

    char cmd[128];
    snprintf(cmd, sizeof(cmd), "kubectl apply -f %s > /dev/null", arq);
    system(cmd);

    free(tpl); free(t1); free(t2);
}

/* ================================================================
 *  Algoritmo de escalonamento (roda dentro do consumidor)
 *  Metricas usadas: cpu, memoria, disco (restricao de cabimento)
 *                   + latencia (desempate). 4 metricas no total.
 * ================================================================ */
static void escalonar(Pod pod) {
    pthread_mutex_lock(&mtx_workers);

    int melhor = -1;
    double melhorScore = -1e9;

    for (int i = 0; i < master.num_workers; i++) {
        Worker *w = &master.workers[i];

        /* so e candidato se o pod CABE nas 3 dimensoes de recurso */
        if (w->cpu_livre   < pod.cpu)   continue;
        if (w->mem_livre   < pod.mem)   continue;
        if (w->disco_livre < pod.disco) continue;

        /* score: quanto mais folga sobra e quanto menor a latencia, melhor */
        double score =
              (double)(w->cpu_livre   - pod.cpu)   / w->cpu_total
            + (double)(w->mem_livre   - pod.mem)   / w->mem_total
            + (double)(w->disco_livre - pod.disco) / w->disco_total
            - (double)w->latencia / 100.0;

        if (score > melhorScore) { melhorScore = score; melhor = i; }
    }

    if (melhor < 0) {
        master.total_pendentes++;
        printf("[PENDENTE] Pod %2d (cpu=%d mem=%d disco=%d) nao coube em nenhum worker\n",
               pod.id, pod.cpu, pod.mem, pod.disco);
        aplicar_no_cluster(pod, NULL, 1);
    } else {
        Worker *w = &master.workers[melhor];
        w->cpu_livre   -= pod.cpu;
        w->mem_livre   -= pod.mem;
        w->disco_livre -= pod.disco;
        master.total_alocados++;
        master.pods_por_worker[melhor]++;
        printf("[ALOCADO ] Pod %2d -> %-13s | livre apos: cpu=%2d mem=%2d disco=%3d\n",
               pod.id, w->nome, w->cpu_livre, w->mem_livre, w->disco_livre);
        aplicar_no_cluster(pod, w->nome, 0);
    }

    pthread_mutex_unlock(&mtx_workers);
}

/* ================================================================
 *  PRODUTOR: le jobs.csv e coloca cada pod na fila do Master
 * ================================================================ */
static void *produtor(void *arg) {
    (void)arg;
    FILE *f = fopen("jobs.csv", "r");
    if (!f) { perror("erro ao abrir jobs.csv"); exit(1); }

    char linha[128];
    fgets(linha, sizeof(linha), f);          /* pula o cabecalho */

    Pod pod;
    while (fgets(linha, sizeof(linha), f)) {
        if (sscanf(linha, "%d,%d,%d,%d",
                   &pod.id, &pod.cpu, &pod.mem, &pod.disco) != 4)
            continue;

        pthread_mutex_lock(&mtx_fila);
        while (master.qtd == MAX_FILA)
            pthread_cond_wait(&tem_espaco, &mtx_fila);   /* espera vaga */
        master.fila[master.fim] = pod;
        master.fim = (master.fim + 1) % MAX_FILA;
        master.qtd++;
        pthread_cond_signal(&tem_item);                  /* avisa: ha pod */
        pthread_mutex_unlock(&mtx_fila);
    }
    fclose(f);

    pthread_mutex_lock(&mtx_fila);
    master.produtor_terminou = 1;
    pthread_cond_signal(&tem_item);   /* acorda o consumidor para ele encerrar */
    pthread_mutex_unlock(&mtx_fila);
    return NULL;
}

/* ================================================================
 *  CONSUMIDOR (escalonador single-threaded): retira da fila e aloca
 * ================================================================ */
static void *consumidor(void *arg) {
    (void)arg;
    while (1) {
        pthread_mutex_lock(&mtx_fila);
        while (master.qtd == 0 && !master.produtor_terminou)
            pthread_cond_wait(&tem_item, &mtx_fila);     /* espera chegar pod */

        if (master.qtd == 0 && master.produtor_terminou) {  /* acabou tudo */
            pthread_mutex_unlock(&mtx_fila);
            break;
        }

        Pod pod = master.fila[master.ini];
        master.ini = (master.ini + 1) % MAX_FILA;
        master.qtd--;
        pthread_cond_signal(&tem_espaco);                /* avisa: abriu vaga */
        pthread_mutex_unlock(&mtx_fila);

        escalonar(pod);                                  /* decide e aplica */
    }
    return NULL;
}

/* ================================================================ */
int main(void) {
    pthread_t tProd, tCons;

    printf("=== Escalonador customizado (produtor/consumidor + mutex) ===\n\n");

    imprimir_master();   /* mostra a estrutura do nodo Master */

    pthread_create(&tProd, NULL, produtor,   NULL);
    pthread_create(&tCons, NULL, consumidor, NULL);

    pthread_join(tProd, NULL);
    pthread_join(tCons, NULL);

    printf("\n=== Estado final dos workers (ocupado/total) ===\n");
    for (int i = 0; i < master.num_workers; i++) {
        Worker *w = &master.workers[i];
        printf("%-13s | cpu %2d/%2d | mem %2d/%2d | disco %3d/%3d | lat %dms\n",
               w->nome,
               w->cpu_total   - w->cpu_livre,   w->cpu_total,
               w->mem_total   - w->mem_livre,   w->mem_total,
               w->disco_total - w->disco_livre, w->disco_total,
               w->latencia);
    }

    /* ---------------- Estatisticas e analise (criterio 8) --------- */
    int total = master.total_alocados + master.total_pendentes;
    double soma_ocup_cpu = 0.0;

    printf("\n=== Estatisticas do escalonamento ===\n");
    printf("PODs processados : %d\n", total);
    printf("  Alocados       : %d (%.1f%%)\n",
           master.total_alocados,  total ? 100.0 * master.total_alocados  / total : 0.0);
    printf("  Pendentes      : %d (%.1f%%)\n",
           master.total_pendentes, total ? 100.0 * master.total_pendentes / total : 0.0);

    printf("\nOcupacao por worker:\n");
    for (int i = 0; i < master.num_workers; i++) {
        Worker *w = &master.workers[i];
        double ocup_cpu   = 100.0 * (w->cpu_total   - w->cpu_livre)   / w->cpu_total;
        double ocup_mem   = 100.0 * (w->mem_total   - w->mem_livre)   / w->mem_total;
        double ocup_disco = 100.0 * (w->disco_total - w->disco_livre) / w->disco_total;
        soma_ocup_cpu += ocup_cpu;
        printf("  %-13s : %d pods | CPU %5.1f%% | MEM %5.1f%% | DISCO %5.1f%%\n",
               w->nome, master.pods_por_worker[i], ocup_cpu, ocup_mem, ocup_disco);
    }
    printf("\nOcupacao media de CPU nos workers: %.1f%%\n",
           soma_ocup_cpu / master.num_workers);

    printf("\nVeja a alocacao real no cluster com:\n");
    printf("  kubectl get pods -o wide\n");
    return 0;
}
