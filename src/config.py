# ---------------------
# -- config.py
# -- proyecto: latam_challenge_de
# -- Archivo de configuracion del proyecto
# -- marzo 2024
# ---------------------
project_name = "latam_challenge_de"


data_path = "../data/"
# ---------------------
# -- Ruta abosluta, relatva, o en cloud del Repositorio de datos
# -- Ej: ../repsitorio/ 
# -- 		/ruta/del/repsitorio/
# -- 		gs://usuario-google-bucket/
# -- 		wasbs://azure-container@<azure-storage-account>.blob.core.windows.net/
# -- 		
# ---------------------


data_file = "test_01.json"
# data_file = "farmers-protest-tweets-2021-2-4.json"
# ---------------------
# -- Nombre de archivo de datos 
# ---------------------



data_read_size = 100000
# ---------------------
# -- Indica Cantidad de registros a leer para realizar ejecucion por hilos (Threads).
# ---------------------




use_spark_service = False
# ---------------------
# -- Indica si usa un servicio de Apache Spark.
# -- Si es False , la carga de archivos se realiza directa por filesystem
# ---------------------


spark_service_provider = "local"
# ---------------------
# -- Proveedor del Servicio Apche Spark. Puede ser un servicio local o en cloud
# -- Dependiendo del servicio se configura parametros particulares de cada proveedor en su seccion.
# -- Si la variable <use_spark_service> == False, se realiza una carga directa por filesystem y se asume el valor de <spark_service_provider> como "local"
# -- 
# -- Proveedores:
# -- local:  Servicio SAPRK local en el mismo servidor o mismo segmento de red
# -- gcp: Servicio Google Cloud Platform , Google Cloud Storage (GCS), BigQuery.
# -- azure: Servicio Azure HDInsight.
# ---------------------


# ---------------------
# Configuracion Local
# ---------------------

SPARK_IP = "127.0.0.1"


# ---------------------
# Configuracion GCP
# ---------------------

SPARK_GCP_JARS = "gs://ruta/a/archivos/jar_files/*.jar"
SPARK_GCP_SERV_ACCOUNT_FILE = "/ruta/a/archivo/service-account-file.json"


# ---------------------
# Configuracion Azure
# ---------------------

SPARK_AZURE_STORAGE_ACCOUNT = "cuenta_azure"
SPARK_AZURE_STORAGE_ACCOUNT_KEY = "cuenta_azure_key"




