# -*- coding: utf-8 -*-

"""
Script para leer archivos JSON (arrays o líneas individuales) desde un directorio
y enviar cada objeto como un mensaje a Apache Kafka.
"""

import os
import sys
import argparse
import logging
import json
from typing import Optional

try:
    from kafka import KafkaProducer
    from kafka.errors import NoBrokersAvailable, KafkaConnectionError
except ModuleNotFoundError:
    print("ERROR: La biblioteca 'kafka-python' no está instalada.", file=sys.stderr)
    print("Por favor, instálala ejecutando: pip install kafka-python", file=sys.stderr)
    sys.exit(1)


# --- Funciones Kafka ---

def crear_productor_kafka(broker_list: str) -> Optional[KafkaProducer]:
    """Crea y conecta una instancia de KafkaProducer."""
    try:
        productor = KafkaProducer(
            bootstrap_servers=broker_list,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
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
    """Asegura el envío de mensajes pendientes y cierra el productor."""
    if productor:
        logging.info("Asegurando el envío de mensajes restantes (flush)...")
        try:
            productor.flush(timeout=60)
            logging.info("Flush completado.")
        except Exception as e_flush:
            logging.error(f"Error durante el flush final a Kafka: {e_flush}", exc_info=True)
        finally:
            logging.info("Cerrando la conexión del productor Kafka...")
            productor.close()
            logging.info("Conexión cerrada.")


# --- Funciones de Procesamiento ---

def procesar_archivo_json(productor: KafkaProducer, topic: str, ruta_archivo: str, nombre_archivo: str) -> tuple:
    """
    Procesa un archivo JSON que puede ser:
    - Un array de objetos JSON: [{"customer_id": 1, ...}, {"customer_id": 2, ...}]
    - Múltiples líneas con un objeto JSON por línea (JSON Lines)
    
    Returns:
        Tupla (objetos_enviados, objetos_fallidos)
    """
    logging.info(f"Procesando archivo: {ruta_archivo}")
    objetos_enviados = 0
    objetos_fallidos = 0
    
    try:
        with open(ruta_archivo, 'r', encoding='utf-8') as archivo:
            contenido = archivo.read().strip()
            
            # Intentar parsear como array JSON
            try:
                datos = json.loads(contenido)
                
                # Si es un array, procesar cada objeto
                if isinstance(datos, list):
                    logging.info(f"Detectado array JSON con {len(datos)} objetos")
                    for idx, objeto in enumerate(datos, 1):
                        try:
                            productor.send(topic, value=objeto)
                            objetos_enviados += 1
                            if idx % 1000 == 0:
                                logging.info(f"  Enviados {idx}/{len(datos)} objetos...")
                        except Exception as e:
                            logging.error(f"Error enviando objeto {idx}: {e}")
                            objetos_fallidos += 1
                
                # Si es un objeto único, enviar directamente
                elif isinstance(datos, dict):
                    logging.info("Detectado objeto JSON único")
                    try:
                        productor.send(topic, value=datos)
                        objetos_enviados += 1
                    except Exception as e:
                        logging.error(f"Error enviando objeto: {e}")
                        objetos_fallidos += 1
                
                else:
                    logging.error(f"Formato JSON no soportado en {nombre_archivo}")
                    
            except json.JSONDecodeError:
                # Si falla, intentar como JSON Lines (una línea = un objeto)
                logging.info("Intentando procesar como JSON Lines (un objeto por línea)")
                archivo.seek(0)  # Volver al inicio
                
                for numero_linea, linea in enumerate(archivo, 1):
                    linea = linea.strip()
                    if not linea or linea in ['[', ']', ',']:
                        continue
                    
                    # Limpiar comas finales
                    if linea.endswith(','):
                        linea = linea[:-1]
                    
                    try:
                        objeto = json.loads(linea)
                        productor.send(topic, value=objeto)
                        objetos_enviados += 1
                        
                        if numero_linea % 1000 == 0:
                            logging.info(f"  Enviados {numero_linea} objetos...")
                            
                    except json.JSONDecodeError as e:
                        logging.warning(f"Línea {numero_linea} no es JSON válido: {e}")
                        objetos_fallidos += 1
                    except Exception as e:
                        logging.error(f"Error enviando línea {numero_linea}: {e}")
                        objetos_fallidos += 1
        
        if objetos_fallidos == 0:
            logging.info(f"✓ '{nombre_archivo}': {objetos_enviados} objetos enviados correctamente.")
        else:
            logging.warning(f"⚠ '{nombre_archivo}': {objetos_enviados} enviados, {objetos_fallidos} fallaron.")
            
    except FileNotFoundError:
        logging.error(f"Archivo no encontrado: '{ruta_archivo}'")
    except IOError as e_io:
        logging.error(f"Error de E/S al leer archivo '{nombre_archivo}': {e_io}")
    except Exception as e_general:
        logging.error(f"Error inesperado procesando archivo '{nombre_archivo}': {e_general}", exc_info=True)
    
    return objetos_enviados, objetos_fallidos


def procesar_directorio(productor: KafkaProducer, topic: str, carpeta_entrada: str) -> tuple:
    """
    Itera sobre un directorio y procesa archivos .json
    
    Returns:
        Tupla (total_archivos, total_enviados, total_fallidos)
    """
    logging.info(f"Iniciando procesamiento del directorio: {carpeta_entrada}")
    
    if not os.path.isdir(carpeta_entrada):
        logging.error(f"La carpeta '{carpeta_entrada}' no existe o no es un directorio válido.")
        return 0, 0, 0
    
    total_archivos = 0
    total_enviados = 0
    total_fallidos = 0
    
    for nombre_item in os.listdir(carpeta_entrada):
        ruta_completa = os.path.join(carpeta_entrada, nombre_item)
        
        if os.path.isfile(ruta_completa) and nombre_item.lower().endswith('.json'):
            total_archivos += 1
            enviados, fallidos = procesar_archivo_json(
                productor, topic, ruta_completa, nombre_item
            )
            total_enviados += enviados
            total_fallidos += fallidos
        else:
            logging.debug(f"Omitiendo '{nombre_item}' (no es archivo .json)")
    
    return total_archivos, total_enviados, total_fallidos


# --- Funciones Auxiliares ---

def parse_arguments() -> argparse.Namespace:
    """Configura y parsea los argumentos de línea de comandos."""
    parser = argparse.ArgumentParser(
        description="Lee archivos JSON y envía cada objeto a Kafka.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        '-b', '--brokers',
        default=os.environ.get('KAFKA_BROKERS', 'localhost:9092'),
        help="Lista de brokers Kafka (Env: KAFKA_BROKERS)"
    )
    parser.add_argument(
        '-t', '--topic',
        default=os.environ.get('KAFKA_TOPIC', 'topic_telecom'),
        help="Topic Kafka destino (Env: KAFKA_TOPIC)"
    )
    parser.add_argument(
        '-d', '--directorio',
        required=True,
        help="Directorio de entrada con archivos JSON."
    )
    parser.add_argument(
        '--log-level',
        default=os.environ.get('LOG_LEVEL', 'INFO'),
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Nivel de detalle del logging (Env: LOG_LEVEL).'
    )
    parser.add_argument(
        '--log-file',
        default='carga_kafka.log',
        help='Ruta al archivo donde se guardarán los logs.'
    )
    return parser.parse_args()


def setup_logging(log_level_name: str, log_file_path: str):
    """Configura el logging para la aplicación."""
    log_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    try:
        file_handler = logging.FileHandler(log_file_path, mode='a', encoding='utf-8')
        file_handler.setFormatter(log_formatter)
        file_handler.setLevel(logging.DEBUG)
    except Exception as e:
        logging.basicConfig(level=logging.INFO)
        logging.critical(f"No se pudo crear el archivo de log '{log_file_path}': {e}")
        sys.exit(1)
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_formatter)
    console_level = getattr(logging, log_level_name.upper(), logging.INFO)
    console_handler.setLevel(console_level)
    
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.handlers.clear()
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)
    
    kafka_logger = logging.getLogger('kafka')
    kafka_logger.setLevel(logging.WARNING)
    kafka_logger.handlers.clear()
    kafka_logger.addHandler(file_handler)
    kafka_logger.propagate = False


def run_processing_pipeline(args: argparse.Namespace) -> tuple:
    """Orquesta el procesamiento completo."""
    productor_kafka = None
    archivos_proc, objetos_env, objetos_fall = 0, 0, 0
    
    try:
        productor_kafka = crear_productor_kafka(args.brokers)
        if not productor_kafka:
            raise ConnectionError(f"No se pudo conectar a Kafka en {args.brokers}.")
        
        archivos_proc, objetos_env, objetos_fall = procesar_directorio(
            productor=productor_kafka,
            topic=args.topic,
            carpeta_entrada=args.directorio
        )
    except KeyboardInterrupt:
        logging.warning("Interrupción por teclado detectada.")
    except ConnectionError as e_conn:
        logging.error(f"Pipeline detenido por error de conexión: {e_conn}")
    except Exception as e_pipeline:
        logging.critical(f"Error crítico no manejado: {e_pipeline}", exc_info=True)
    finally:
        cerrar_productor_kafka(productor_kafka)
    
    return archivos_proc, objetos_env, objetos_fall


def log_summary(archivos_proc: int, objetos_env: int, objetos_fall: int):
    """Loguea el resumen final."""
    logging.info("=" * 60)
    logging.info("RESUMEN FINAL")
    logging.info("=" * 60)
    logging.info(f"Archivos procesados: {archivos_proc}")
    logging.info(f"Objetos enviados exitosamente: {objetos_env}")
    
    if objetos_fall > 0:
        logging.warning(f"⚠ Objetos que fallaron: {objetos_fall}")
    elif archivos_proc > 0 or objetos_env > 0:
        logging.info("✓ No se reportaron fallos.")
    else:
        logging.info("No se procesaron datos.")
    logging.info("=" * 60)


# --- Función Principal ---

def main():
    """Punto de entrada principal."""
    args = parse_arguments()
    setup_logging(args.log_level, args.log_file)
    
    logging.info("=" * 60)
    logging.info("INICIANDO CARGA DE JSON A KAFKA")
    logging.info("=" * 60)
    logging.info(f"Brokers: {args.brokers}")
    logging.info(f"Topic: {args.topic}")
    logging.info(f"Directorio: {args.directorio}")
    logging.info("=" * 60)
    
    archivos_proc, objetos_env, objetos_fall = run_processing_pipeline(args)
    
    log_summary(archivos_proc, objetos_env, objetos_fall)
    
    exit_code = 1 if objetos_fall > 0 else 0
    sys.exit(exit_code)


if __name__ == "__main__":
    main()