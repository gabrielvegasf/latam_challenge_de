import os
import importlib
from pyspark.sql import SparkSession

# ---------------------
# Modulo de Funciones Comunes del proyecto
# ---------------------

def validate_file(file_path: str):

    # ---------------------
    # Valida el archivo de datos si exsite y si tiene data
    # 
    # Param:
    #   file_path: ruta + nombre archivo 
    # 
    # Retorna:
    #   bRes: boolean - Es Valido el archivo especificado
    #   sMsg: string - Mensaje de error
    # ---------------------

    sMsg = ""
    bRes = True

    if not os.path.isfile(file_path):
        sMsg = "Archivo de datos: " + file_path + " no existe"
        bRes = False
    else :
        if os.stat(file_path).st_size == 0:
            sMsg = "Archivo de datos: " + file_path + " vacio"
            bRes = False

    return bRes, sMsg

# ---------------------


def create_spark_session(): 

    # ---------------------
    # Crea un objeto SparkSession basado en la configuracion del proveedor asociado
    # 
    # Retorna:
    #   spark: SparkSession creado o tomdo si existe.
    # ---------------------

    config = importlib.import_module("config")

    spark_app_name = config.project_name + '_' + config.spark_service_provider

    if config.spark_service_provider == 'local':

        os.environ["SPARK_LOCAL_IP"] = config.SPARK_IP

        spark = SparkSession.builder \
            .appName(spark_app_name) \
            .config("spark.logLevel", "WARN") \
            .getOrCreate()

    elif config.spark_service_provider == 'gcp':

        spark = SparkSession.builder \
            .appName(spark_app_name) \
            .config("spark.jars", config.SPARK_GC_JARS) \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.executor.instances", "2") \
            .config("spark.executor.cores", "2") \
            .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
            .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", config.SPARK_GCP_SERV_ACCOUNT_FILE) \
            .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
            .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
            .getOrCreate()

    elif config.spark_service_provider == 'azure':

        spark = SparkSession.builder \
            .appName(spark_app_name) \
            .config("spark.jars.packages", "com.microsoft.azure:azure-storage:8.6.0") \
            .config("spark.hadoop.fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem") \
            .config("spark.hadoop.fs.azure.account.key." + SPARK_AZURE_STORAGE_ACCOUNT +".blob.core.windows.net", config.SPARK_AZURE_STORAGE_ACCOUNT_KEY) \
            .getOrCreate()

    return spark

# ---------------------


def read_spark_dataframe(sdf, registro_actual, data_size=10000):

    # ---------------------
    # Obtiene un bloque de datos desde la posicion indicada en <registro_actual> y de tamaño <data_size> regs
    # 
    # Param:
    #   sdf: Objeto Spark DataFrame con los data completa
    #   registro_actual: Posicion actual de la data a leer
    #   data_size: Cantidad o tamaño de registros a leer
    # 
    # Retorna:
    #   <List>: Lista con la data del bloque
    # ---------------------

    while True:

        if (registro_actual < sdf.count()):

            df_bloque = sdf.offset(registro_actual).limit(data_size)
            registro_actual += data_size

            yield df_bloque.collect()
        else:
            break

# ---------------------


