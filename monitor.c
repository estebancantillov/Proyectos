/*
 * ============================================================
 * Pontificia Universidad Javeriana
 * Facultad de Ingeniería - Departamento de Ingeniería de Sistemas
 * Curso: Sistemas Operativos 2026-30
 * Proyecto: Sistema de Categorización Meteorológica
 * Archivo: monitor.c
 * Descripción: Monitor / Control de Categorización.
 *              Crea el named pipe, recibe mediciones de los agentes
 *              (formato texto CSV), las distribuye a buffers circulares
 *              por estación usando patrón productor/consumidor con
 *              semáforos POSIX. Hilos consumidores escriben el archivo
 *              consolidado. Al finalizar calcula promedios y emite la
 *              categoría meteorológica según la Tabla 2 del enunciado.
 * Autores: Alejandro Macías, Esteban Cantillo, Daniel Prieto, Jorge Simental
 * Fecha: Mayo 2026
 * Compilación: gcc -Wall -Wextra -pthread -o monitor monitor.c
 * Uso: ./monitor -b tamBuffer -p pipeNom
 * ============================================================
 */

#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <pthread.h>
#include <semaphore.h>
#include <sys/stat.h>

/* ─── Constantes ─── */
#define MAX_ESTACIONES       10
#define MAX_LINE            256
#define NOMBRE_CONSOLIDADO  "consolidado.csv"
#define UMBRAL_HORAS          2   /* horas consecutivas sin datos → alarma */

/* ─── Rangos aceptables (Tabla 1) ─── */
#define HUMEDAD_MIN   77
#define HUMEDAD_MAX  100
#define PRESION_MIN  740
#define PRESION_MAX  760
#define ROCIO_MIN      3
#define ROCIO_MAX     12

/*
 * Estructura de una medición. Misma que en agenteM.c.
 * Se usa internamente en el monitor para almacenar en los buffers.
 */
typedef struct {
    char estacion[8];
    int  humedad;
    int  rocio;
    int  presion;
    char hora[12];
} Medicion;

/* ════════════════════════════════════════════════════════════
 * Buffer circular por estación — patrón productor/consumidor
 * ════════════════════════════════════════════════════════════ */
typedef struct {
    Medicion       *datos;              /* Arreglo circular                */
    int             tam;                /* Capacidad del buffer            */
    int             inicio;             /* Índice del próximo a leer       */
    int             fin;                /* Índice del próximo a escribir   */
    int             count;              /* Elementos actualmente en buffer */
    sem_t           lleno;              /* Semáforo: espacios ocupados     */
    sem_t           vacio;              /* Semáforo: espacios libres       */
    pthread_mutex_t mutex;             /* Exclusión mutua sobre índices   */
    char            nombre[8];          /* Identificador de la estación    */
    int             activo;             /* 0 cuando ya no hay más datos    */
    /* Acumuladores para promedios finales */
    long            suma_hum;
    long            suma_rocio;
    long            suma_presion;
    int             total_mediciones;
    /* Para detección de datos faltantes */
    char            ultima_hora[12];
} BufferEstacion;

/* ─── Variables globales del monitor ─── */
static int              fd_pipe      = -1;
static int              tam_buffer   =  0;
static char             pipe_nom[256] = "";
static FILE            *archivo_cons  = NULL;
static pthread_mutex_t  mutex_archivo = PTHREAD_MUTEX_INITIALIZER;

static BufferEstacion   buffers[MAX_ESTACIONES];
static int              n_estaciones = 0;
static pthread_mutex_t  mutex_est    = PTHREAD_MUTEX_INITIALIZER;

static pthread_t hilo_recolector;
static pthread_t hilos_consumidor[MAX_ESTACIONES];

/* ─── Prototipos ─── */
static void  uso(const char *prog);
static void  inicializarMonitor(void);
static void  finalizarMonitor(void);
static int   buscarOCrearBuffer(const char *est);
static void  bufferProducir(BufferEstacion *b, const Medicion *m);
static int   bufferConsumir(BufferEstacion *b, Medicion *m);
static int   parsearLineaPipe(const char *linea, Medicion *m);
static void *hiloRecolectorFn(void *arg);
static void *hiloConsumidorFn(void *arg);
static const char *categorizar(double hum, double rocio, double presion);
static int   diferenciaHoras(const char *h_ant, const char *h_nueva);

/* ════════════════════════════════════════════════════════════
 * main
 * ════════════════════════════════════════════════════════════ */
int main(int argc, char *argv[]) {
    int opt;

    while ((opt = getopt(argc, argv, "b:p:")) != -1) {
        switch (opt) {
            case 'b':
                tam_buffer = atoi(optarg);
                if (tam_buffer <= 0) {
                    fprintf(stderr, "[monitor] Error: tamBuffer debe ser > 0\n");
                    exit(EXIT_FAILURE);
                }
                break;
            case 'p':
                strncpy(pipe_nom, optarg, sizeof(pipe_nom) - 1);
                break;
            default:
                uso(argv[0]);
                exit(EXIT_FAILURE);
        }
    }

    if (tam_buffer <= 0 || pipe_nom[0] == '\0') {
        fprintf(stderr, "[monitor] Error: faltan parámetros -b y/o -p\n");
        uso(argv[0]);
        exit(EXIT_FAILURE);
    }

    printf("... Control de Categorización Meteorológica!!!\n");
    fflush(stdout);

    inicializarMonitor();

    /* Lanzar el hilo recolector (productor) */
    if (pthread_create(&hilo_recolector, NULL, hiloRecolectorFn, NULL) != 0) {
        fprintf(stderr, "[monitor] Error al crear hilo recolector: %s\n", strerror(errno));
        exit(EXIT_FAILURE);
    }

    /* Esperar a que el recolector termine (EOF en el pipe) */
    pthread_join(hilo_recolector, NULL);

    /* Señalar a cada hilo consumidor que ya no llegarán más datos */
    pthread_mutex_lock(&mutex_est);
    for (int i = 0; i < n_estaciones; i++) {
        buffers[i].activo = 0;
        sem_post(&buffers[i].lleno);  /* Despertar consumidor bloqueado */
    }
    pthread_mutex_unlock(&mutex_est);

    /* Esperar a que todos los consumidores terminen */
    for (int i = 0; i < n_estaciones; i++) {
        pthread_join(hilos_consumidor[i], NULL);
    }

    finalizarMonitor();

    return EXIT_SUCCESS;
}

/* ════════════════════════════════════════════════════════════
 * uso
 * ════════════════════════════════════════════════════════════ */
static void uso(const char *prog) {
    fprintf(stderr,
        "Uso: %s -b tamBuffer -p pipeNom\n"
        "  -b tamBuffer   tamaño del buffer en número de registros\n"
        "  -p pipeNom     nombre del pipe nominal\n",
        prog);
}

/* ════════════════════════════════════════════════════════════
 * inicializarMonitor
 * Crea el FIFO, abre el archivo consolidado y el pipe.
 * ════════════════════════════════════════════════════════════ */
static void inicializarMonitor(void) {
    /* Crear el named pipe (FIFO) en el sistema de archivos */
    if (mkfifo(pipe_nom, 0666) < 0) {
        if (errno != EEXIST) {
            fprintf(stderr, "[monitor] Error al crear pipe '%s': %s\n",
                    pipe_nom, strerror(errno));
            exit(EXIT_FAILURE);
        }
    }

    /* Crear el archivo consolidado de salida */
    archivo_cons = fopen(NOMBRE_CONSOLIDADO, "w");
    if (!archivo_cons) {
        fprintf(stderr, "[monitor] Error al crear '%s': %s\n",
                NOMBRE_CONSOLIDADO, strerror(errno));
        exit(EXIT_FAILURE);
    }

    /* Abrir el pipe para lectura.
     * Bloquea hasta que al menos un agente abra el extremo escritura.
     * Retornará EOF (read=0) cuando TODOS los agentes cierren su fd. */
    fd_pipe = open(pipe_nom, O_RDONLY);
    if (fd_pipe < 0) {
        fprintf(stderr, "[monitor] Error al abrir pipe '%s': %s\n",
                pipe_nom, strerror(errno));
        exit(EXIT_FAILURE);
    }
}

/* ════════════════════════════════════════════════════════════
 * finalizarMonitor
 * Cierra el archivo consolidado, calcula promedios, emite el
 * reporte final y libera todos los recursos.
 * ════════════════════════════════════════════════════════════ */
static void finalizarMonitor(void) {
    if (archivo_cons) {
        fclose(archivo_cons);
        archivo_cons = NULL;
    }

    /* Calcular promedios globales sobre todas las estaciones */
    long tot_hum = 0, tot_rocio = 0, tot_presion = 0;
    int  total   = 0;

    for (int i = 0; i < n_estaciones; i++) {
        tot_hum     += buffers[i].suma_hum;
        tot_rocio   += buffers[i].suma_rocio;
        tot_presion += buffers[i].suma_presion;
        total       += buffers[i].total_mediciones;
    }

    if (total > 0) {
        double prom_hum     = (double)tot_hum     / total;
        double prom_rocio   = (double)tot_rocio   / total;
        double prom_presion = (double)tot_presion / total;
        const char *cat     = categorizar(prom_hum, prom_rocio, prom_presion);
        printf("\n        Parte Meteorológico Bogotá: \"%s\"\n", cat);
    } else {
        printf("\n        Parte Meteorológico Bogotá: \"Sin datos suficientes\"\n");
    }

    printf("\nFin del Monitor...!!!\n");
    fflush(stdout);

    /* Liberar recursos de cada buffer */
    for (int i = 0; i < n_estaciones; i++) {
        free(buffers[i].datos);
        sem_destroy(&buffers[i].lleno);
        sem_destroy(&buffers[i].vacio);
        pthread_mutex_destroy(&buffers[i].mutex);
    }

    pthread_mutex_destroy(&mutex_archivo);
    pthread_mutex_destroy(&mutex_est);

    close(fd_pipe);
    unlink(pipe_nom);  /* Eliminar el FIFO del sistema de archivos */
}

/* ════════════════════════════════════════════════════════════
 * buscarOCrearBuffer
 * Busca el buffer de la estación indicada. Si no existe, lo
 * crea e inicia su hilo consumidor. Retorna el índice.
 * ════════════════════════════════════════════════════════════ */
static int buscarOCrearBuffer(const char *est) {
    pthread_mutex_lock(&mutex_est);

    for (int i = 0; i < n_estaciones; i++) {
        if (strcmp(buffers[i].nombre, est) == 0) {
            pthread_mutex_unlock(&mutex_est);
            return i;
        }
    }

    if (n_estaciones >= MAX_ESTACIONES) {
        fprintf(stderr, "[monitor] Máximo de estaciones alcanzado (%d)\n", MAX_ESTACIONES);
        pthread_mutex_unlock(&mutex_est);
        return -1;
    }

    int idx = n_estaciones++;
    BufferEstacion *b = &buffers[idx];

    strncpy(b->nombre, est, sizeof(b->nombre) - 1);
    b->nombre[sizeof(b->nombre) - 1] = '\0';

    b->datos = malloc(sizeof(Medicion) * tam_buffer);
    if (!b->datos) {
        fprintf(stderr, "[monitor] Error al asignar buffer para %s\n", est);
        exit(EXIT_FAILURE);
    }

    b->tam               = tam_buffer;
    b->inicio            = 0;
    b->fin               = 0;
    b->count             = 0;
    b->activo            = 1;
    b->suma_hum          = 0;
    b->suma_rocio        = 0;
    b->suma_presion      = 0;
    b->total_mediciones  = 0;
    b->ultima_hora[0]    = '\0';

    /* Inicializar semáforos POSIX y mutex */
    if (sem_init(&b->lleno, 0, 0) != 0 ||
        sem_init(&b->vacio, 0, tam_buffer) != 0 ||
        pthread_mutex_init(&b->mutex, NULL) != 0) {
        fprintf(stderr, "[monitor] Error al inicializar semáforos para %s: %s\n",
                est, strerror(errno));
        exit(EXIT_FAILURE);
    }

    /* Lanzar hilo consumidor para esta estación */
    if (pthread_create(&hilos_consumidor[idx], NULL,
                       hiloConsumidorFn, (void *)(long)idx) != 0) {
        fprintf(stderr, "[monitor] Error al crear hilo consumidor %s: %s\n",
                est, strerror(errno));
        exit(EXIT_FAILURE);
    }

    pthread_mutex_unlock(&mutex_est);
    return idx;
}

/* ════════════════════════════════════════════════════════════
 * bufferProducir
 * Inserta una medición en el buffer circular.
 * Bloquea si el buffer está lleno (sem_wait vacio).
 * ════════════════════════════════════════════════════════════ */
static void bufferProducir(BufferEstacion *b, const Medicion *m) {
    sem_wait(&b->vacio);              /* Esperar espacio libre   */
    pthread_mutex_lock(&b->mutex);

    b->datos[b->fin] = *m;
    b->fin = (b->fin + 1) % b->tam;
    b->count++;

    pthread_mutex_unlock(&b->mutex);
    sem_post(&b->lleno);              /* Señalar dato disponible */
}

/* ════════════════════════════════════════════════════════════
 * bufferConsumir
 * Extrae una medición del buffer circular.
 * Bloquea si el buffer está vacío (sem_wait lleno).
 * Retorna 1 si obtuvo un dato, 0 si debe terminar.
 * ════════════════════════════════════════════════════════════ */
static int bufferConsumir(BufferEstacion *b, Medicion *m) {
    sem_wait(&b->lleno);  /* Bloquear si no hay datos */

    pthread_mutex_lock(&b->mutex);

    /* Despertado para terminar sin dato pendiente */
    if (b->count == 0 && !b->activo) {
        pthread_mutex_unlock(&b->mutex);
        return 0;
    }

    *m = b->datos[b->inicio];
    b->inicio = (b->inicio + 1) % b->tam;
    b->count--;

    pthread_mutex_unlock(&b->mutex);
    return 1;
}

/* ════════════════════════════════════════════════════════════
 * parsearLineaPipe
 * Parsea una línea CSV recibida del pipe.
 * Formato: ESTACION,humedad,rocio,presion,HH:MM:SS\n
 * Retorna 1 si éxito, 0 si mal formada.
 * ════════════════════════════════════════════════════════════ */
static int parsearLineaPipe(const char *linea, Medicion *m) {
    int resultado = sscanf(linea, "%7[^,],%d,%d,%d,%11s",
                           m->estacion,
                           &m->humedad,
                           &m->rocio,
                           &m->presion,
                           m->hora);
    return (resultado == 5) ? 1 : 0;
}

/* ════════════════════════════════════════════════════════════
 * hiloRecolectorFn
 * Hilo productor: lee líneas CSV del FIFO y deposita cada
 * medición en el buffer de la estación correspondiente.
 * Termina cuando el pipe retorna EOF (todos los agentes cerraron).
 * ════════════════════════════════════════════════════════════ */
static void *hiloRecolectorFn(void *arg) {
    (void)arg;

    char    linea[MAX_LINE];
    int     pos = 0;
    char    c;
    ssize_t n;

    /* Leer carácter a carácter para armar líneas completas.
     * read() retorna 0 (EOF) cuando todos los agentes cierran el pipe. */
    while ((n = read(fd_pipe, &c, 1)) > 0) {
        if (c == '\n' || pos == MAX_LINE - 1) {
            linea[pos] = '\0';
            pos = 0;

            /* Eliminar \r si lo hay */
            int len = strlen(linea);
            if (len > 0 && linea[len-1] == '\r') linea[len-1] = '\0';

            if (strlen(linea) == 0) continue;

            Medicion m;
            if (!parsearLineaPipe(linea, &m)) {
                fprintf(stderr, "[recolector] Línea inválida descartada: %s\n", linea);
                continue;
            }

            int idx = buscarOCrearBuffer(m.estacion);
            if (idx >= 0) {
                bufferProducir(&buffers[idx], &m);
            }
        } else {
            linea[pos++] = c;
        }
    }

    if (n < 0 && errno != EINTR) {
        fprintf(stderr, "[recolector] Error leyendo pipe: %s\n", strerror(errno));
    }

    /* n == 0: EOF, todos los agentes cerraron su extremo escritura */
    return NULL;
}

/* ════════════════════════════════════════════════════════════
 * hiloConsumidorFn
 * Hilo consumidor para una estación. Lee del buffer, acumula
 * promedios, escribe en el archivo consolidado y detecta
 * horas consecutivas faltantes.
 * ════════════════════════════════════════════════════════════ */
static void *hiloConsumidorFn(void *arg) {
    int idx = (int)(long)arg;
    BufferEstacion *b = &buffers[idx];
    Medicion m;

    while (1) {
        int ok = bufferConsumir(b, &m);
        if (!ok) break;  /* Buffer vacío y sin más datos */

        /* Detectar horas consecutivas faltantes */
        if (b->ultima_hora[0] != '\0') {
            int diff = diferenciaHoras(b->ultima_hora, m.hora);
            if (diff >= UMBRAL_HORAS) {
                fprintf(stderr,
                    "[ALARMA] Estación %s: datos faltantes entre %s y %s, "
                    "por favor revisar el sensor.\n",
                    b->nombre, b->ultima_hora, m.hora);
            }
        }
        strncpy(b->ultima_hora, m.hora, sizeof(b->ultima_hora) - 1);

        /* Acumular para promedios finales */
        b->suma_hum     += m.humedad;
        b->suma_rocio   += m.rocio;
        b->suma_presion += m.presion;
        b->total_mediciones++;

        /* Escribir en el archivo consolidado — sección crítica */
        pthread_mutex_lock(&mutex_archivo);
        if (archivo_cons) {
            fprintf(archivo_cons, "%s,%d,%d,%d,%s\n",
                    m.estacion, m.humedad, m.rocio, m.presion, m.hora);
            fflush(archivo_cons);
        }
        pthread_mutex_unlock(&mutex_archivo);
    }

    return NULL;
}

/* ════════════════════════════════════════════════════════════
 * categorizar
 * Determina la categoría meteorológica según la Tabla 2.
 * Prioridad: Lluvioso → Nublado → Fresco.
 * Cuando los promedios no caen exactamente en una categoría,
 * se aproxima por el parámetro más dominante (humedad).
 * ════════════════════════════════════════════════════════════ */
static const char *categorizar(double hum, double rocio, double presion) {
    /* Lluvioso: humedad >90, rocío >9, presión <750 */
    if (hum > 90 && rocio > 9 && presion < 750)
        return "Lluvioso";

    /* Nublado: humedad 80-95, rocío >8, presión ~751 */
    if (hum >= 80 && hum <= 95 && rocio > 8 && presion >= 750 && presion <= 753)
        return "Nublado";

    /* Fresco: humedad <80, rocío 5-8, presión >754 */
    if (hum < 80 && rocio >= 5 && rocio <= 8 && presion > 754)
        return "Fresco";

    /* Aproximación cuando no hay match exacto */
    if (hum > 90) return "Lluvioso";
    if (hum >= 80) return "Nublado";
    return "Fresco";
}

/* ════════════════════════════════════════════════════════════
 * diferenciaHoras
 * Calcula la diferencia en horas enteras entre dos timestamps
 * con formato HH:MM:SS.
 * ════════════════════════════════════════════════════════════ */
static int diferenciaHoras(const char *h_ant, const char *h_nueva) {
    int hh1, mm1, ss1, hh2, mm2, ss2;
    if (sscanf(h_ant,   "%d:%d:%d", &hh1, &mm1, &ss1) != 3) return 0;
    if (sscanf(h_nueva, "%d:%d:%d", &hh2, &mm2, &ss2) != 3) return 0;

    int seg1 = hh1 * 3600 + mm1 * 60 + ss1;
    int seg2 = hh2 * 3600 + mm2 * 60 + ss2;
    int diff = seg2 - seg1;
    if (diff < 0) diff += 24 * 3600;  /* cruce de medianoche */

    return diff / 3600;
}
