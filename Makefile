# ============================================================
# Pontificia Universidad Javeriana
# Facultad de Ingeniería - Sistemas Operativos 2026-30
# Proyecto: Sistema de Categorización Meteorológica
# Autores: Alejandro Macías, Esteban Cantillo, Daniel Prieto
# ============================================================

CC      = gcc
CFLAGS  = -Wall -Wextra -g
TARGETS = agenteM monitor

.PHONY: all clean datos help

all: $(TARGETS)

agenteM: agenteM.c
	$(CC) $(CFLAGS) -o agenteM agenteM.c

monitor: monitor.c
	$(CC) $(CFLAGS) -pthread -o monitor monitor.c

datos:
	mkdir -p datos
	@printf "EK,92,10,748,08:00:00\nEK,91,10,747,09:00:00\nEK,93,11,746,10:00:00\nEK,88,9,751,11:00:00\nEK,79,4,720,12:00:00\nEK,95,11,745,13:00:00\n.\n" > datos/kennedy.csv
	@printf "ET,85,9,751,08:00:00\nET,82,8,751,09:00:00\nET,90,10,749,10:00:00\nET,200,15,800,11:00:00\nET,83,9,751,13:00:00\n.\n" > datos/usaquen.csv
	@printf "EU,75,6,756,08:00:00\nEU,78,7,755,09:00:00\nEU,79,5,758,10:00:00\nEU,76,6,757,11:00:00\n.\n" > datos/teusaquillo.csv
	@echo "Archivos de prueba creados en datos/"

clean:
	rm -f $(TARGETS) consolidado.csv pipeNom
	@echo "Limpieza completada."

help:
	@echo "Uso del sistema:"
	@echo ""
	@echo "  1. Compilar:         make"
	@echo "  2. Iniciar monitor:  ./monitor -b <tamBuffer> -p <pipeNom>"
	@echo "  3. Iniciar agentes:  ./agenteM -f <archivo.csv> -t <segundos> -p <pipeNom>"
	@echo ""
	@echo "  Ejemplo:"
	@echo "    Terminal 1:  ./monitor -b 4 -p pipeNom"
	@echo "    Terminal 2:  ./agenteM -f datos/kennedy.csv -t 1 -p pipeNom"
	@echo "    Terminal 3:  ./agenteM -f datos/usaquen.csv -t 1 -p pipeNom"
	@echo "    Terminal 4:  ./agenteM -f datos/teusaquillo.csv -t 1 -p pipeNom"
