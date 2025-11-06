#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import shutil
import argparse
import logging
from typing import Dict, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, IntegerType, BooleanType
)
from pyspark.sql.functions import from_json, col, when, lit

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
DEFAULT_KAFKA_TOPIC = "topic_telecom"
DEFAULT_APP_NAME = "TELECOM_TO_POSTGRES"
DEFAULT_KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0"
DEFAULT_POSTGRES_PACKAGE = "org.postgresql:postgresql:42.5.0"
DEFAULT_CHECKPOINT_PATH = "checkpoint/"

# Configuración de PostgreSQL (deberías usar variables de entorno en producción)
POSTGRES_CONFIG = {
    "url": "jdbc:postgresql://localhost:5432/tfm_database",
    "user": "tfm_user",
    "password": "tfm_password_2024",
    "driver": "org.postgresql.Driver"
}

# ==============================================================================
# FUNCIONES
# ==============================================================================

def crear_spark_session(app_name: str, kafka_package: str, postgres_package: str) -> SparkSession:
    """
    Inicializa una SparkSession con los conectores de Kafka y PostgreSQL.
    """
    logger.info("Creando SparkSession...")
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", f"{kafka_package},{postgres_package}")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
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


def transformar_datos(df_parsed: DataFrame) -> Dict[str, DataFrame]:
    """
    Transforma los datos parseados en DataFrames separados para cada tabla.
    """
    # Transformaciones para convertir strings "Yes"/"No" a booleanos
    df_transformed = df_parsed.withColumn(
        "streaming_services_bool",
        when(col("d.usage.streaming_services") == "Yes", True).otherwise(False)
    ).withColumn(
        "multiple_lines_bool",
        when(col("d.additional_features.multiple_lines") == "Yes", True).otherwise(False)
    ).withColumn(
        "family_plan_bool",
        when(col("d.additional_features.family_plan") == "Yes", True).otherwise(False)
    ).withColumn(
        "premium_support_bool",
        when(col("d.additional_features.premium_support") == "Yes", True).otherwise(False)
    ).withColumn(
        "churn_bool",
        when(col("d.additional_features.churn") == 1, True).otherwise(False)
    ).withColumn(
        "discount_applied_bool",
        when(col("d.payments.discount_applied") > 0, True).otherwise(False)
    )

    # DataFrame para la tabla customers
    customers_df = df_transformed.select(
        col("d.customer_id").alias("customer_id"),
        col("d.personal_info.age").alias("age"),
        col("d.personal_info.gender").alias("gender"),
        col("d.personal_info.location").alias("location")
    ).distinct()

    # DataFrame para la tabla subscriptions
    subscriptions_df = df_transformed.select(
        col("d.customer_id").alias("customer_id"),
        col("d.subscription.plan_type").alias("plan_type"),
        col("d.subscription.tenure_months").alias("tenure_months"),
        col("d.subscription.monthly_charges").alias("monthly_charges"),
        col("d.subscription.billing_cycle").alias("billing_cycle")
    )

    # DataFrame para la tabla usage_data
    usage_df = df_transformed.select(
        col("d.customer_id").alias("customer_id"),
        col("d.usage.total_call_minutes").alias("total_call_minutes"),
        col("d.usage.data_usage_gb").alias("data_usage_gb"),
        col("d.usage.num_sms_sent").alias("num_sms_sent"),
        col("streaming_services_bool").alias("streaming_services"),
        col("d.usage.roaming_usage").alias("roaming_usage")
    )

    # DataFrame para la tabla customer_support
    customer_support_df = df_transformed.select(
        col("d.customer_id").alias("customer_id"),
        col("d.customer_support.customer_service_calls").alias("customer_service_calls"),
        col("d.customer_support.customer_feedback_score").cast(IntegerType()).alias("customer_feedback_score"),
        col("d.customer_support.network_quality_score").alias("network_quality_score")
    )

    # DataFrame para la tabla payments
    payments_df = df_transformed.select(
        col("d.customer_id").alias("customer_id"),
        col("d.payments.payment_method").alias("payment_method"),
        col("d.payments.payment_failures").alias("payment_failures"),
        col("discount_applied_bool").alias("discount_applied")
    )

    # DataFrame para la tabla additional_features
    additional_features_df = df_transformed.select(
        col("d.customer_id").alias("customer_id"),
        col("d.additional_features.device_type").alias("device_type"),
        col("multiple_lines_bool").alias("multiple_lines"),
        col("family_plan_bool").alias("family_plan"),
        col("premium_support_bool").alias("premium_support"),
        col("churn_bool").alias("churn")
    )

    return {
        "customers": customers_df,
        "subscriptions": subscriptions_df,
        "usage_data": usage_df,
        "customer_support": customer_support_df,
        "payments": payments_df,
        "additional_features": additional_features_df
    }


def escribir_a_postgres(batch_df: DataFrame, batch_id: int, table_name: str):
    """
    Escribe un batch de datos a PostgreSQL.
    """
    try:
        count = batch_df.count()
        if count > 0:
            logger.info(f"Escribiendo {count} registros en la tabla {table_name}, batch_id={batch_id}")
            
            (batch_df.write
                .format("jdbc")
                .option("url", POSTGRES_CONFIG["url"])
                .option("dbtable", table_name)
                .option("user", POSTGRES_CONFIG["user"])
                .option("password", POSTGRES_CONFIG["password"])
                .option("driver", POSTGRES_CONFIG["driver"])
                .mode("append")
                .save())
            
            logger.info(f"Escritura completada para {table_name}, batch_id={batch_id}")
        else:
            logger.info(f"Batch vacío para {table_name}, batch_id={batch_id}")
    except Exception as e:
        logger.error(f"Error escribiendo en {table_name}, batch_id={batch_id}: {str(e)}")
        # En producción, podrías querer enviar esta excepción a un sistema de monitoreo


def procesar_micro_batch(batch_df: DataFrame, batch_id: int):
    """
    Procesa cada micro-batch y escribe en las tablas de PostgreSQL.
    """
    try:
        # Parsear el JSON
        schema = definir_esquema()
        df_parsed = batch_df.select(
            from_json(col("value").cast("string"), schema).alias("d")
        ).filter(col("d").isNotNull())
        
        if df_parsed.count() == 0:
            logger.info(f"Batch {batch_id} no contiene datos válidos")
            return

        # Transformar datos
        dataframes = transformar_datos(df_parsed)
        
        # Escribir en cada tabla
        for table_name, df_table in dataframes.items():
            escribir_a_postgres(df_table, batch_id, table_name)
            
    except Exception as e:
        logger.error(f"Error procesando batch {batch_id}: {str(e)}")


# ==============================================================================
# LÓGICA MAIN
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Job Spark Structured Streaming desde Kafka a PostgreSQL"
    )
    parser.add_argument("--broker", default=DEFAULT_KAFKA_BROKER,
                        help="Dirección del broker Kafka")
    parser.add_argument("--topic", default=DEFAULT_KAFKA_TOPIC,
                        help="Topic de Kafka a leer")
    parser.add_argument("--checkpoint-path", default=DEFAULT_CHECKPOINT_PATH,
                        help="Directorio de checkpoint de Spark")
    parser.add_argument("--postgres-url", default=POSTGRES_CONFIG["url"],
                        help="URL de conexión a PostgreSQL")
    parser.add_argument("--postgres-user", default=POSTGRES_CONFIG["user"],
                        help="Usuario de PostgreSQL")
    parser.add_argument("--postgres-password", default=POSTGRES_CONFIG["password"],
                        help="Contraseña de PostgreSQL")
    
    args = parser.parse_args()

    # Actualizar configuración de PostgreSQL con argumentos
    POSTGRES_CONFIG.update({
        "url": args.postgres_url,
        "user": args.postgres_user,
        "password": args.postgres_password
    })

    # Limpieza de directorio de checkpoint previo
    if os.path.exists(args.checkpoint_path):
        logger.info(f"Eliminando directorio existente: {args.checkpoint_path}")
        shutil.rmtree(args.checkpoint_path)

    spark = crear_spark_session(DEFAULT_APP_NAME, DEFAULT_KAFKA_PACKAGE, DEFAULT_POSTGRES_PACKAGE)
    
    try:
        df_raw = leer_stream_kafka(spark, args.broker, args.topic)

        logger.info("Iniciando streaming query...")
        query = (
            df_raw
              .writeStream
              .option("checkpointLocation", args.checkpoint_path)
              .foreachBatch(procesar_micro_batch)
              .start()
        )

        query.awaitTermination()
        
    except KeyboardInterrupt:
        logger.info("Detenido por el usuario.")
    except Exception as e:
        logger.error(f"Error en la aplicación: {str(e)}")
    finally:
        spark.stop()
        logger.info("Aplicación finalizada.")


if __name__ == "__main__":
    main()