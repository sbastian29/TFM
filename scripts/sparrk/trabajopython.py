#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import shutil
import argparse
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, IntegerType
)
from pyspark.sql.functions import from_json, col

# ==============================================================================
# CONFIGURACIÓN DE LOGGING
# ==============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)


# ==============================================================================
# CONSTANTES POR DEFECTO
# ==============================================================================
DEFAULT_KAFKA_BROKER = "172.18.0.12:9092"
DEFAULT_KAFKA_TOPIC = "clientes"
DEFAULT_APP_NAME = "APPCLIENTES"
DEFAULT_KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0"
DEFAULT_OUTPUT_PATH = "output_json/"
DEFAULT_CHECKPOINT_PATH = "checkpoint/"


# ==============================================================================
# FUNCIONES
# ==============================================================================

def crear_spark_session(app_name: str, kafka_package: str) -> SparkSession:
    """
    Inicializa una SparkSession con el conector de Kafka.
    """
    logger.info("Creando SparkSession...")
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", kafka_package)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession creada.")
    return spark


def definir_esquema() -> StructType:
    """
    Devuelve el StructType completo para el JSON de entrada.
    """
    return StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("personal_info", StructType([
            StructField("age", IntegerType(), True),
            StructField("gender", StringType(), True),
            StructField("location", StringType(), True),
        ]), True),
        StructField("subscription", StructType([
            StructField("plan_type", StringType(), True),
            StructField("tenure_months", IntegerType(), True),
            StructField("monthly_charges", DoubleType(), True),
            StructField("billing_cycle", StringType(), True),
        ]), True),
        StructField("usage", StructType([
            StructField("total_call_minutes", IntegerType(), True),
            StructField("data_usage_gb", DoubleType(), True),
            StructField("num_sms_sent", IntegerType(), True),
            StructField("streaming_services", StringType(), True),
            StructField("roaming_usage", DoubleType(), True),
        ]), True),
        StructField("customer_support", StructType([
            StructField("customer_service_calls", IntegerType(), True),
            StructField("customer_feedback_score", DoubleType(), True),
            StructField("network_quality_score", DoubleType(), True),
        ]), True),
        StructField("payments", StructType([
            StructField("payment_method", StringType(), True),
            StructField("payment_failures", IntegerType(), True),
            StructField("discount_applied", DoubleType(), True),
        ]), True),
        StructField("additional_features", StructType([
            StructField("device_type", StringType(), True),
            StructField("multiple_lines", StringType(), True),
            StructField("family_plan", StringType(), True),
            StructField("premium_support", StringType(), True),
            StructField("churn", IntegerType(), True),
        ]), True),
    ])


def leer_stream_kafka(spark: SparkSession, broker: str, topic: str) -> DataFrame:
    """
    Crea un DataFrame de streaming desde Kafka.
    """
    logger.info(f"Conectando a Kafka broker={broker}, topic={topic}")
    df = (
        spark.readStream
             .format("kafka")
             .option("kafka.bootstrap.servers", broker)
             .option("subscribe", topic)
             .option("startingOffsets", "earliest")
             .load()
    )
    return df


def parsear_y_aplanar(df_raw: DataFrame, schema: StructType) -> DataFrame:
    """
    Parsea el JSON y extrae solo los campos necesarios en formato plano.
    """
    logger.info("Parseando y aplanando JSON")
    return (
        df_raw
          .select(from_json(col("value").cast("string"), schema).alias("d"))
          .select(
              col("d.customer_id").alias("customer_id"),
              col("d.personal_info.age").alias("age"),
              col("d.personal_info.gender").alias("gender"),
              col("d.personal_info.location").alias("location"),
              col("d.subscription.plan_type").alias("plan_type"),
              col("d.subscription.monthly_charges").alias("monthly_charges"),
              col("d.additional_features.churn").alias("churn"),
              col("d.customer_support.customer_service_calls")
                 .alias("customer_service_calls"),
              col("d.customer_support.customer_feedback_score")
                 .alias("customer_feedback_score")
          )
    )


def procesar_micro_batch(batch_df: DataFrame, batch_id: int, output_path: str):
    """
    Escribe cada micro-batch en JSON en disco.
    """
    count = batch_df.count()
    logger.info(f"Procesando batch {batch_id}, registros={count}")
    if count > 0:
        batch_df.write.mode("append").json(output_path)
    else:
        logger.info("Batch vacío, no se escribe nada.")


# ==============================================================================
# LÓGICA MAIN
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Job Spark Structured Streaming desde Kafka a JSON"
    )
    parser.add_argument("--broker", default=DEFAULT_KAFKA_BROKER,
                        help="Dirección del broker Kafka")
    parser.add_argument("--topic", default=DEFAULT_KAFKA_TOPIC,
                        help="Topic de Kafka a leer")
    parser.add_argument("--output-path", default=DEFAULT_OUTPUT_PATH,
                        help="Directorio de salida para JSON")
    parser.add_argument("--checkpoint-path", default=DEFAULT_CHECKPOINT_PATH,
                        help="Directorio de checkpoint de Spark")
    args = parser.parse_args()

    # Limpieza de directorios previos
    for path in (args.output_path, args.checkpoint_path):
        if os.path.exists(path):
            logger.info(f"Eliminando directorio existente: {path}")
            shutil.rmtree(path)

    spark = crear_spark_session(DEFAULT_APP_NAME, DEFAULT_KAFKA_PACKAGE)
    schema = definir_esquema()
    df_raw = leer_stream_kafka(spark, args.broker, args.topic)
    df_proc = parsear_y_aplanar(df_raw, schema)

    logger.info("Iniciando streaming query...")
    query = (
        df_proc
          .writeStream
          .option("checkpointLocation", args.checkpoint_path)
          .foreachBatch(lambda df, id: procesar_micro_batch(df, id, args.output_path))
          .start()
    )

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Detenido por el usuario. Cerrando Spark...")
    finally:
        query.stop()
        spark.stop()
        logger.info("Aplicación finalizada.")


if __name__ == "__main__":
    main()
