# -*- coding: utf-8 -*-

"""
Script para leer archivos JSON línea por línea desde un directorio especificado
y enviar cada línea como un mensaje a un topic de Apache Kafka.

Características:
- Configurable a través de argumentos de línea de comandos.
- Soporte para leer configuración desde variables de entorno.
- Logging avanzado: separa los logs de la aplicación y de las librerías.
  - Logs de la aplicación: se muestran en consola y se guardan en un archivo.
  - Logs de la librería 'kafka': se guardan solo en el archivo para no saturar la consola.
- Manejo robusto de errores y estructura de código modular.

Ejemplo de uso:
python cargar_json_a_kafka.py --directorio /ruta/a/tus/datos/json \
                               --brokers mi-kafka.servidor.com:9092 \
                               --topic mi_topic_json \
                               --log-level INFO \
                               --log-file /var/log/carga_kafka.log
"""

import os
import sys
import argparse
import logging
from typing import Tuple, Optional

# Se asume que kafka-python está instalado.
# Si no, ejecutar: pip install kafka-python
try:
    from kafka import KafkaProducer
    from kafka.errors import NoBrokersAvailable, KafkaConnectionError
except ModuleNotFoundError:
    print("ERROR: La biblioteca 'kafka-python' no está instalada.", file=sys.stderr)
    print("Por favor, instálala ejecutando: pip install kafka-python", file=sys.stderr)
    sys.exit(1)


# --- Funciones Kafka ---

def crear_productor_kafka(broker_list: str) -> Optional[KafkaProducer]:
    """
    Intenta crear y conectar una instancia de KafkaProducer.
    Args:
        broker_list: Cadena con los brokers Kafka.
    Returns:
        Instancia de KafkaProducer o None si falla la conexión.
    """
    productor = None
    try:
        productor = KafkaProducer(
            bootstrap_servers=broker_list,
            value_serializer=lambda v: v.encode('utf-8'),
        )
        logging.info(f"Productor Kafka conectado exitosamente a {broker_list}.")
        return productor
    except (NoBrokersAvailable, KafkaConnectionError) as e:
        logging.critical(f"No se pudo conectar con Kafka brokers en {broker_list}. Error: {e}")
        return None
    except Exception as e:
        logging.critical(f"Error inesperado al crear el productor Kafka: {e}", exc_info=True)
        return None

def cerrar_productor_kafka(productor: Optional[KafkaProducer]):
    """
    Asegura el envío de mensajes pendientes (flush) y cierra la conexión del productor.
    Args:
        productor: La instancia del productor a cerrar (puede ser None).
    """
    if productor:
        logging.info("Asegurando el envío de mensajes restantes (flush)...")
        try:
            productor.flush(timeout=60)
            logging.info("Flush completado.")
        except Exception as e_flush:
            logging.error(f"Ocurrió un error durante el flush final a Kafka: {e_flush}", exc_info=True)
        finally:
            logging.info("Cerrando la conexión del productor Kafka...")
            productor.close()
            logging.info("Conexión cerrada.")


# --- Funciones de Procesamiento ---

def enviar_linea_a_kafka(productor: KafkaProducer, topic: str, linea_contenido: str, nombre_archivo: str, numero_linea: int) -> bool:
    """
    Envía una única línea (si no está vacía) a Kafka y maneja errores de envío.
    Returns:
        True si la línea estaba vacía o se aceptó en el buffer, False si hubo error de envío.
    """
    linea_limpia = linea_contenido.strip()
    if not linea_limpia:
        return True # Consideramos éxito si la línea está vacía

    try:
        productor.send(topic, value=linea_limpia)
        return True
    except Exception as e_kafka:
        logging.error(f"Al enviar línea {numero_linea} de '{nombre_archivo}' a Kafka: {e_kafka}")
        return False

def procesar_contenido_archivo(productor: KafkaProducer, topic: str, archivo_abierto, nombre_archivo: str) -> Tuple[int, int]:
    """
    Itera sobre las líneas de un archivo abierto y las envía a Kafka.
    Returns:
        Tupla (lineas_ok, lineas_error) para este archivo.
    """
    lineas_ok, lineas_error = 0, 0
    for numero_linea, linea in enumerate(archivo_abierto, 1):
        if enviar_linea_a_kafka(productor, topic, linea, nombre_archivo, numero_linea):
            if linea.strip(): lineas_ok += 1
        else:
             if linea.strip(): lineas_error += 1
    return lineas_ok, lineas_error

def procesar_archivo_individual(productor: KafkaProducer, topic: str, ruta_completa: str, nombre_archivo: str) -> Tuple[int, int]:
    """
    Abre un archivo JSON, procesa su contenido y maneja errores a nivel de archivo.
    Returns:
        Tupla (lineas_enviadas_ok, lineas_con_error_envio) para este archivo.
    """
    logging.info(f"Procesando archivo: {ruta_completa}")
    lineas_enviadas_archivo, lineas_fallidas_archivo = 0, 0
    try:
        with open(ruta_completa, 'r', encoding='utf-8') as archivo:
            lineas_enviadas_archivo, lineas_fallidas_archivo = procesar_contenido_archivo(
                productor, topic, archivo, nombre_archivo
            )
        # Loguear resumen del archivo
        if lineas_fallidas_archivo == 0:
            logging.info(f"'{nombre_archivo}': {lineas_enviadas_archivo} líneas enviadas correctamente.")
        else:
            logging.warning(f"'{nombre_archivo}': {lineas_enviadas_archivo} líneas enviadas, {lineas_fallidas_archivo} fallaron.")
    except FileNotFoundError:
        logging.error(f"Archivo no encontrado: '{ruta_completa}'")
    except IOError as e_io:
        logging.error(f"Error de E/S al leer archivo '{nombre_archivo}': {e_io}")
    except UnicodeDecodeError as e_unicode:
        logging.error(f"Error de codificación en archivo '{nombre_archivo}'. ¿Es UTF-8?: {e_unicode}")
    except Exception as e_general:
        logging.error(f"Error inesperado procesando archivo '{nombre_archivo}': {e_general}", exc_info=True)

    return lineas_enviadas_archivo, lineas_fallidas_archivo

def procesar_directorio(productor: KafkaProducer, topic: str, carpeta_entrada: str) -> Tuple[int, int, int]:
    """
    Itera sobre un directorio, procesa archivos .json y acumula resultados.
    Returns:
        Tupla (total_archivos_procesados, total_lineas_enviadas, total_lineas_fallidas).
    """
    logging.info(f"Iniciando procesamiento del directorio: {carpeta_entrada}")
    if not os.path.isdir(carpeta_entrada):
        logging.error(f"La carpeta '{carpeta_entrada}' no existe o no es un directorio válido.")
        return 0, 0, 0 # Retorna ceros si la carpeta no es válida

    total_archivos, total_enviadas, total_fallidas = 0, 0, 0
    for nombre_item in os.listdir(carpeta_entrada):
        ruta_completa = os.path.join(carpeta_entrada, nombre_item)
        if os.path.isfile(ruta_completa) and nombre_item.lower().endswith('.json'):
            total_archivos += 1
            enviadas, fallidas = procesar_archivo_individual(
                productor, topic, ruta_completa, nombre_item
            )
            total_enviadas += enviadas
            total_fallidas += fallidas
        else:
            logging.debug(f"Omitiendo '{nombre_item}' (no es archivo .json o no es archivo).")

    return total_archivos, total_enviadas, total_fallidas


# --- Funciones Auxiliares para Main ---

def parse_arguments() -> argparse.Namespace:
    """
    Configura y parsea los argumentos de línea de comandos.
    Returns:
        Objeto Namespace con los argumentos parseados.
    """
    parser = argparse.ArgumentParser(
        description="Lee archivos JSON línea por línea desde un directorio y los envía a Kafka.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        '-b', '--brokers',
        default=os.environ.get('KAFKA_BROKERS', 'localhost:9092'),
        help="Lista de brokers Kafka (Env: KAFKA_BROKERS)"
    )
    parser.add_argument(
        '-t', '--topic',
        default=os.environ.get('KAFKA_TOPIC', 'json_topic'),
        help="Topic Kafka destino (Env: KAFKA_TOPIC)"
    )
    parser.add_argument(
        '-d', '--directorio',
        required=True,
        help="Directorio de entrada que contiene los archivos JSON."
    )
    parser.add_argument(
        '--log-level',
        default=os.environ.get('LOG_LEVEL', 'INFO'),
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Nivel de detalle del logging para la consola (Env: LOG_LEVEL).'
    )
    parser.add_argument(
        '--log-file',
        default='carga_kafka.log',
        help='Ruta al archivo donde se guardarán todos los logs.'
    )
    return parser.parse_args()

def setup_logging(log_level_name: str, log_file_path: str):
    """
    Configura el logging para la aplicación y bibliotecas.
    - Logs de la app: Van a consola (filtrado por log_level) Y a archivo (DEBUG+).
    - Logs de 'kafka': Van SOLO a archivo (INFO+).
    """
    log_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Handler para escribir a archivo.
    try:
        file_handler = logging.FileHandler(log_file_path, mode='a', encoding='utf-8')
        file_handler.setFormatter(log_formatter)
        file_handler.setLevel(logging.DEBUG)  # Captura todo desde DEBUG hacia arriba en el archivo
    except Exception as e:
        # Si falla la creación del archivo de log, es un problema serio. Informar y salir.
        logging.basicConfig(level=logging.INFO) # Configuración mínima para poder loguear este error
        logging.critical(f"No se pudo crear o escribir en el archivo de log '{log_file_path}': {e}", exc_info=True)
        sys.exit(1)

    # Handler para escribir a consola.
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_formatter)
    console_level = getattr(logging, log_level_name.upper(), logging.INFO)
    console_handler.setLevel(console_level)

    # Configurar el logger raíz (para los logs de nuestra aplicación).
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)  # Poner el nivel base bajo para no filtrar antes de los handlers.
    root_logger.handlers.clear()  # Limpiar handlers previos.
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)

    # Configurar el logger de la biblioteca 'kafka'.
    kafka_logger = logging.getLogger('kafka')
    kafka_logger.setLevel(logging.INFO)  # Nivel para logs de kafka (WARNING es menos verboso).
    kafka_logger.handlers.clear()
    kafka_logger.addHandler(file_handler)  # Solo al archivo.
    kafka_logger.propagate = False  # ¡Importante! Evita que los logs de 'kafka' suban al logger raíz.


def run_processing_pipeline(args: argparse.Namespace) -> Tuple[int, int, int]:
    """
    Orquesta la creación del productor, procesamiento del directorio y cierre final.
    Returns:
        Tupla con (archivos_procesados, lineas_enviadas, lineas_fallidas).
    """
    productor_kafka = None
    archivos_proc, lineas_env, lineas_fall = 0, 0, 0
    try:
        productor_kafka = crear_productor_kafka(args.brokers)
        if not productor_kafka:
            raise ConnectionError(f"No se pudo conectar a los brokers Kafka en {args.brokers}.")

        archivos_proc, lineas_env, lineas_fall = procesar_directorio(
            productor=productor_kafka,
            topic=args.topic,
            carpeta_entrada=args.directorio
        )
    except KeyboardInterrupt:
        logging.warning("Interrupción por teclado detectada durante el procesamiento.")
    except ConnectionError as e_conn:
        logging.error(f"Pipeline detenido por error de conexión: {e_conn}")
    except Exception as e_pipeline:
        logging.critical(f"Error crítico no manejado en el pipeline: {e_pipeline}", exc_info=True)
    finally:
        cerrar_productor_kafka(productor_kafka)

    return archivos_proc, lineas_env, lineas_fall

def log_summary(archivos_proc: int, lineas_env: int, lineas_fall: int):
    """
    Loguea el resumen final del proceso basado en los contadores.
    """
    logging.info("--- Resumen Final del Proceso ---")
    logging.info(f"Archivos JSON encontrados y procesados: {archivos_proc}")
    logging.info(f"Líneas enviadas exitosamente a Kafka: {lineas_env}")
    if lineas_fall > 0:
        logging.warning(f"Líneas que fallaron al enviar a Kafka: {lineas_fall}")
    elif archivos_proc > 0 or lineas_env > 0:
        logging.info("No se reportaron fallos en el envío de líneas individuales.")
    else:
        logging.info("No se procesaron archivos ni líneas.")
    logging.info("--- Script Finalizado ---")


# --- Función Principal (Orquestador) ---

def main():
    """
    Punto de entrada principal: coordina la ejecución del script.
    """
    args = parse_arguments()
    setup_logging(args.log_level, args.log_file)

    logging.info("--- Iniciando Script de Carga de JSON a Kafka ---")
    logging.info(f"Configuración - Brokers: '{args.brokers}', Topic: '{args.topic}', Directorio: '{args.directorio}'")

    archivos_proc, lineas_env, lineas_fall = run_processing_pipeline(args)

    log_summary(archivos_proc, lineas_env, lineas_fall)

    exit_code = 1 if lineas_fall > 0 else 0
    sys.exit(exit_code)


# --- Punto de Entrada Estándar ---
if __name__ == "__main__":
    # Este bloque asegura que main() solo se ejecute cuando
    # el script se corre directamente desde la línea de comandos.
    main()

