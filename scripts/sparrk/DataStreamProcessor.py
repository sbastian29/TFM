#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
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
DEFAULT_KAFKA_BROKER = "localhost:9092" 
DEFAULT_KAFKA_TOPIC = "topic_telecom"
DEFAULT_APP_NAME = "TELECOM_TO_POSTGRES"
DEFAULT_KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0"
DEFAULT_POSTGRES_PACKAGE = "org.postgresql:postgresql:42.5.0"
DEFAULT_CHECKPOINT_PATH = "C:/tmp/spark-checkpoints/telecom"  

# Configuración de PostgreSQL
POSTGRES_CONFIG = {
    "url": "jdbc:postgresql://localhost:5433/tfm_database",  
    "user": "tfm_user",
    "password": "tfm_password_2024",
    "driver": "org.postgresql.Driver"
}

# ==============================================================================
# FUNCIONES
# ==============================================================================

def configurar_hadoop_windows():
    """
    Configura HADOOP_HOME si estamos en Windows y no está configurado.
    """
    if sys.platform == "win32":
        if not os.environ.get('HADOOP_HOME'):
            hadoop_home = r'C:\hadoop'
            if os.path.exists(hadoop_home):
                os.environ['HADOOP_HOME'] = hadoop_home
                logger.info(f"HADOOP_HOME configurado a: {hadoop_home}")
            else:
                logger.warning(f"HADOOP_HOME no encontrado en {hadoop_home}")
                logger.warning("Asegúrate de tener winutils.exe instalado")

def crear_spark_session(app_name: str, kafka_package: str, postgres_package: str) -> SparkSession:
    """
    Inicializa una SparkSession con los conectores de Kafka y PostgreSQL.
    Configurado específicamente para Windows.
    """
    logger.info("Creando SparkSession...")
    
    # Configurar Hadoop para Windows
    configurar_hadoop_windows()
    
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", f"{kafka_package},{postgres_package}")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Configuraciones específicas para Windows
        .config("spark.sql.warehouse.dir", "file:///C:/tmp/spark-warehouse")
        .config("spark.driver.host", "localhost")
        .config("spark.driver.bindAddress", "127.0.0.1")
        # Reducir verbosidad
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession creada exitosamente.")
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
             .option("failOnDataLoss", "false")  
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
    # IMPORTANTE: Filtrar registros con customer_id NULL antes de distinct()
    customers_df = df_transformed.select(
        col("d.customer_id").alias("customer_id"),
        col("d.personal_info.age").alias("age"),
        col("d.personal_info.gender").alias("gender"),
        col("d.personal_info.location").alias("location")
    ).filter(col("customer_id").isNotNull()).distinct()

    # DataFrame para la tabla subscriptions
    subscriptions_df = df_transformed.select(
        col("d.customer_id").alias("customer_id"),
        col("d.subscription.plan_type").alias("plan_type"),
        col("d.subscription.tenure_months").alias("tenure_months"),
        col("d.subscription.monthly_charges").alias("monthly_charges"),
        col("d.subscription.billing_cycle").alias("billing_cycle")
    ).filter(col("customer_id").isNotNull())

    # DataFrame para la tabla usage_data
    usage_df = df_transformed.select(
        col("d.customer_id").alias("customer_id"),
        col("d.usage.total_call_minutes").alias("total_call_minutes"),
        col("d.usage.data_usage_gb").alias("data_usage_gb"),
        col("d.usage.num_sms_sent").alias("num_sms_sent"),
        col("streaming_services_bool").alias("streaming_services"),
        col("d.usage.roaming_usage").alias("roaming_usage")
    ).filter(col("customer_id").isNotNull())

    # DataFrame para la tabla customer_support
    customer_support_df = df_transformed.select(
        col("d.customer_id").alias("customer_id"),
        col("d.customer_support.customer_service_calls").alias("customer_service_calls"),
        col("d.customer_support.customer_feedback_score").cast(IntegerType()).alias("customer_feedback_score"),
        col("d.customer_support.network_quality_score").alias("network_quality_score")
    ).filter(col("customer_id").isNotNull())

    # DataFrame para la tabla payments
    payments_df = df_transformed.select(
        col("d.customer_id").alias("customer_id"),
        col("d.payments.payment_method").alias("payment_method"),
        col("d.payments.payment_failures").alias("payment_failures"),
        col("discount_applied_bool").alias("discount_applied")
    ).filter(col("customer_id").isNotNull())

    # DataFrame para la tabla additional_features
    additional_features_df = df_transformed.select(
        col("d.customer_id").alias("customer_id"),
        col("d.additional_features.device_type").alias("device_type"),
        col("multiple_lines_bool").alias("multiple_lines"),
        col("family_plan_bool").alias("family_plan"),
        col("premium_support_bool").alias("premium_support"),
        col("churn_bool").alias("churn")
    ).filter(col("customer_id").isNotNull())

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
            logger.info(f"Batch {batch_id}: Escribiendo {count} registros en '{table_name}'")
            
            (batch_df.write
                .format("jdbc")
                .option("url", POSTGRES_CONFIG["url"])
                .option("dbtable", table_name)
                .option("user", POSTGRES_CONFIG["user"])
                .option("password", POSTGRES_CONFIG["password"])
                .option("driver", POSTGRES_CONFIG["driver"])
                .mode("append")
                .save())
            
            logger.info(f"Batch {batch_id}: ✓ {count} registros escritos en '{table_name}'")
        else:
            logger.debug(f"Batch {batch_id}: Sin datos para '{table_name}'")
    except Exception as e:
        logger.error(f"Batch {batch_id}: ✗ Error en '{table_name}': {str(e)}")


def procesar_micro_batch(batch_df: DataFrame, batch_id: int):
    """
    Procesa cada micro-batch y escribe en las tablas de PostgreSQL.
    """
    try:
        logger.info(f"--- Procesando Batch {batch_id} ---")
        
        # Parsear el JSON
        schema = definir_esquema()
        df_parsed = batch_df.select(
            from_json(col("value").cast("string"), schema).alias("d")
        ).filter(col("d").isNotNull())
        
        count = df_parsed.count()
        if count == 0:
            logger.info(f"Batch {batch_id}: Sin datos válidos")
            return

        logger.info(f"Batch {batch_id}: {count} registros parseados")

        # Transformar datos
        dataframes = transformar_datos(df_parsed)
        
        # Escribir en cada tabla
        for table_name, df_table in dataframes.items():
            escribir_a_postgres(df_table, batch_id, table_name)
        
        logger.info(f"--- Batch {batch_id} completado ---\n")
            
    except Exception as e:
        logger.error(f"Batch {batch_id}: Error crítico: {str(e)}", exc_info=True)


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

    # Crear directorio de checkpoint si no existe
    checkpoint_dir = args.checkpoint_path
    if os.path.exists(checkpoint_dir):
        logger.info(f"Eliminando checkpoint anterior: {checkpoint_dir}")
        try:
            shutil.rmtree(checkpoint_dir)
        except Exception as e:
            logger.warning(f"No se pudo eliminar checkpoint: {e}")

    # Crear directorios necesarios
    os.makedirs(checkpoint_dir, exist_ok=True)
    os.makedirs("C:/tmp/spark-warehouse", exist_ok=True)

    spark = crear_spark_session(DEFAULT_APP_NAME, DEFAULT_KAFKA_PACKAGE, DEFAULT_POSTGRES_PACKAGE)
    
    try:
        df_raw = leer_stream_kafka(spark, args.broker, args.topic)

        logger.info("Iniciando streaming query...")
        logger.info(f"Checkpoint: {checkpoint_dir}")
        logger.info(f"PostgreSQL: {args.postgres_url}")
        logger.info("Presiona Ctrl+C para detener\n")
        
        query = (
            df_raw
              .writeStream
              .option("checkpointLocation", checkpoint_dir)
              .foreachBatch(procesar_micro_batch)
              .trigger(processingTime="10 seconds")  
              .start()
        )

        query.awaitTermination()
        
    except KeyboardInterrupt:
        logger.info("\n⚠ Detenido por el usuario (Ctrl+C)")
    except Exception as e:
        logger.error(f"✗ Error en la aplicación: {str(e)}", exc_info=True)
    finally:
        spark.stop()
        logger.info("✓ Aplicación finalizada correctamente")


if __name__ == "__main__":
    main()