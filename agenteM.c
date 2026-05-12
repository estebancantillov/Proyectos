/*
 * ============================================================
 * Pontificia Universidad Javeriana
 * Facultad de Ingeniería - Departamento de Ingeniería de Sistemas
 * Curso: Sistemas Operativos 2026-30
 * Proyecto: Sistema de Categorización Meteorológica
 * Archivo: agenteM.c
 * Descripción: Agente de Medición. Lee un archivo CSV de sensores,
 *              filtra mediciones fuera de rango y envía las válidas
 *              al monitor a través de un named pipe (FIFO).
 *              Soporta flags en cualquier orden: -f -t -p
 * Autores: Alejandro Macías, Esteban Cantillo, Daniel Prieto, Jorge Simental
 * Fecha: Mayo 2026
 * Compilación: gcc -Wall -Wextra -o agenteM agenteM.c
 * Uso: ./agenteM -f archivo.csv -t tiempo -p pipeNom
 * ============================================================
 */

#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>      /* sleep(), write(), close() */
#include <fcntl.h>       /* open(), O_WRONLY */
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>

/* ─── Rangos aceptables según Tabla 1 del enunciado ─── */
#define HUMEDAD_MIN   77
#define HUMEDAD_MAX  100
#define PRESION_MIN  740
#define PRESION_MAX  760
#define ROCIO_MIN      3
#define ROCIO_MAX     12

#define MAX_LINE 256

/*
 * Estructura que representa una medición leída del archivo.
 * Corresponde a una fila del CSV: estación, humedad, rocío, presión, hora.
 */
typedef struct {
    char estacion[8];   /* Ej: "EK", "ET", "EU" */
    int  humedad;       /* Humedad relativa en % */
    int  rocio;         /* Punto de rocío en °C  */
    int  presion;       /* Presión atmosférica en hPa */
    char hora[12];      /* Hora de la medición "HH:MM:SS" */
} Medicion;

/* ─── Prototipos ─── */
static void uso(const char *prog);
static int  esMedicionValida(Medicion *m);
static int  parsearLinea(const char *linea, Medicion *m);
static void enviarMedicion(int fdPipe, const Medicion *m);

/* ════════════════════════════════════════════════════════════
 * main
 * ════════════════════════════════════════════════════════════ */
int main(int argc, char *argv[]) {

    char *nombreArchivo = NULL;
    char *pipeNom       = NULL;
    int   tiempo        = -1;
int   opt;

    while ((opt = getopt(argc, argv, "f:t:p:")) != -1) {
        switch (opt) {
            case 'f':
                nombreArchivo = optarg;
                break;
            case 't':
                tiempo = atoi(optarg);
                if (tiempo < 0) {
                    fprintf(stderr, "[agenteM] Error: tiempo debe ser >= 0\n");
                    exit(EXIT_FAILURE);
                }
                break;
            case 'p':
                pipeNom = optarg;
                break;

            default:
                uso(argv[0]);
                exit(EXIT_FAILURE);
        }
    }

    if (nombreArchivo == NULL || pipeNom == NULL || tiempo < 0) {
        fprintf(stderr, "[agenteM] Error: faltan argumentos obligatorios.\n");
        uso(argv[0]);
        exit(EXIT_FAILURE);
    }

    FILE *archivo = fopen(nombreArchivo, "r");
    if (archivo == NULL) {
        fprintf(stderr, "[agenteM] Error al abrir '%s': %s\n",
                nombreArchivo, strerror(errno));
        exit(EXIT_FAILURE);
    }

    /* open() bloquea hasta que el monitor abra el pipe para lectura */
    int fdPipe = open(pipeNom, O_WRONLY);
    if (fdPipe < 0) {
        fprintf(stderr, "[agenteM] Error al abrir pipe '%s': %s\n",
                pipeNom, strerror(errno));
        fclose(archivo);
        exit(EXIT_FAILURE);
    }

    printf("... Agente de Medición en proceso!!!\n");
    fflush(stdout);

    char     linea[MAX_LINE];
    Medicion m;
    char     estacion[8] = "??";

    while (fgets(linea, sizeof(linea), archivo) != NULL) {

        linea[strcspn(linea, "\r\n")] = '\0';

        if (strlen(linea) == 0) continue;

        /* Punto final: esperar t segundos adicionales y terminar */
        if (linea[0] == '.') {
            sleep(tiempo);
            break;
        }

        if (!parsearLinea(linea, &m)) {
            fprintf(stderr, "[agenteM] Línea con formato inválido, ignorada: %s\n", linea);
            sleep(tiempo);
            continue;
        }

        strncpy(estacion, m.estacion, sizeof(estacion) - 1);
        estacion[sizeof(estacion) - 1] = '\0';

        if (esMedicionValida(&m)) {
            enviarMedicion(fdPipe, &m);
        } else {
            /* Medición fuera de rango: se rechaza, NO se envía al monitor */
            printf("[agenteM] Medición rechazada (fuera de rango): %s\n", linea);
            fflush(stdout);
        }

        sleep(tiempo);
    }

    fclose(archivo);
    close(fdPipe);

    printf("\nFin de Lectura de Sensores de la Estación %s!!!\n", estacion);
    fflush(stdout);

    return EXIT_SUCCESS;
}

/* ════════════════════════════════════════════════════════════
 * uso
 * ════════════════════════════════════════════════════════════ */
static void uso(const char *prog) {
    fprintf(stderr,
        "Uso: %s -f nombreArchivo -t tiempo -p pipeNom [-k rm]\n"
        "  -f archivo   CSV de sensores\n"
        "  -t tiempo    segundos entre lecturas\n"
        "  -p pipe      nombre del pipe nominal\n"
        "  -k rm        (opcional) borrar el archivo al terminar\n",
        prog);
}

/* ════════════════════════════════════════════════════════════
 * esMedicionValida
 * Retorna 1 si todos los parámetros están dentro del rango
 * aceptable (Tabla 1), 0 si alguno está fuera.
 * ════════════════════════════════════════════════════════════ */
static int esMedicionValida(Medicion *m) {
    if (m->humedad < HUMEDAD_MIN || m->humedad > HUMEDAD_MAX) return 0;
    if (m->presion < PRESION_MIN || m->presion > PRESION_MAX) return 0;
    if (m->rocio   < ROCIO_MIN   || m->rocio   > ROCIO_MAX)   return 0;
    return 1;
}

/* ════════════════════════════════════════════════════════════
 * parsearLinea
 * Parsea una línea CSV con formato: EK,90,9,750,08:00:00
 * Retorna 1 si éxito, 0 si la línea está mal formada.
 * ════════════════════════════════════════════════════════════ */
static int parsearLinea(const char *linea, Medicion *m) {
    int resultado = sscanf(linea, "%7[^,],%d,%d,%d,%11s",
                           m->estacion,
                           &m->humedad,
                           &m->rocio,
                           &m->presion,
                           m->hora);
    return (resultado == 5) ? 1 : 0;
}

/* ════════════════════════════════════════════════════════════
 * enviarMedicion
 * Serializa la medición como texto CSV y la escribe en el pipe.
 * Formato: ESTACION,humedad,rocio,presion,hora\n
 * La escritura es atómica para mensajes cortos (< PIPE_BUF).
 * ════════════════════════════════════════════════════════════ */
static void enviarMedicion(int fdPipe, const Medicion *m) {
    char    mensaje[MAX_LINE];
    int     len = snprintf(mensaje, sizeof(mensaje),
                           "%s,%d,%d,%d,%s\n",
                           m->estacion, m->humedad, m->rocio, m->presion, m->hora);
    ssize_t escritos = write(fdPipe, mensaje, len);
    if (escritos < 0) {
        fprintf(stderr, "[agenteM] Error al escribir en el pipe: %s\n", strerror(errno));
    }
}
