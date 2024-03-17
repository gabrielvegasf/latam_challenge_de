import os
import importlib
import json
import concurrent.futures
import pandas as pd
import pyspark
from multiprocessing import Pool
from pyspark.sql import SparkSession
from collections import defaultdict
from datetime import datetime

import resource
import humanize

from typing import List, Tuple

#@profile

def procesar_data_file(data_bloque, tweets_fecha, tweets_fecha_user):

    data = data_bloque.collect()

    for row in data:

        tweet = json.loads(row['value'])

        fecha_datetime = tweet['date']
        fecha = fecha_datetime[0:10]

        print((fecha))

        username = tweet['user']['username']

        print((username))
        
        # Incrementa el contador para la fecha correspondiente
        tweets_fecha[fecha] += 1
        tweets_fecha_user[fecha][username] += 1


def read_dataframe(sdf, registro_actual, data_size=10000):

    while True:

        if (registro_actual < sdf.count()):

            df_bloque = sdf.offset(registro_actual).limit(data_size)
            registro_actual += data_size

            yield df_bloque
        else:
            break


def q1_time(file_path: str) -> List[Tuple[datetime.date, str]]:

    data_size = 20  # Registro por bloque
    tweets_fecha = defaultdict(int)
    tweets_fecha_user = defaultdict(lambda: defaultdict(int))
    lista_resultado_tuplas = []

    # Crear una sesión de Spark
    objSpark = SparkSession.builder \
        .appName("q1_time_using_spark") \
        .config("spark.logLevel", "WARN") \
        .getOrCreate()

    # Leer el archivo a un Spark DataFrame 
    sdf = objSpark.read.text(file_path)

    total_registros = sdf.count()
    registro_actual = 0


    with concurrent.futures.ThreadPoolExecutor() as executor:
        
        # Procesar cada segmento del archivo en un hilo separado
        futures = [executor.submit(procesar_data_file, data_bloque, tweets_fecha, tweets_fecha_user) for data_bloque in read_dataframe(sdf, registro_actual, data_size)]

        # Esperar a que todos los hilos terminen
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()  # Obtener el resultado del hilo (si es necesario)
            except Exception as e:
                print(f'Ocurrió un error: {e}')



    # Detener la sesión de Spark
    objSpark.stop()

#    exit(0)

    top_10_tweets_fecha_ordenado = sorted(tweets_fecha.items(), key=lambda item: item[1], reverse=True)[:10]
    print(top_10_tweets_fecha_ordenado)

    # Imprime el total de tweets por fecha
    for fecha, total_tweets in top_10_tweets_fecha_ordenado:
        # print(f"Fecha: {fecha}, Total de tweets: {total_tweets}")

        # print("-----------")

        top_1_tweets_fecha_user = sorted(tweets_fecha_user[fecha].items(), key=lambda item: item[1], reverse=True)[:1]

        top_user_fecha, valor = top_1_tweets_fecha_user[0]

        fecha_date = datetime.strptime(fecha, "%Y-%m-%d").date()

        lista_resultado_tuplas.append((fecha_date, top_user_fecha))

    return lista_resultado_tuplas



if __name__ == "__main__":

    config = importlib.import_module("config")

    if not os.path.isfile(config.data_path + config.data_file):
        print(f"Archivo de datos: {config.data_file} no existe")
        exit(0)
    else :
        if os.stat(config.data_path + config.data_file).st_size == 0:
            print(f"Archivo de datos: {config.data_file} vacio")
            exit(0)

    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"


    resultado = q1_time(config.data_path + config.data_file)

    print(resultado)

    print('File Size ', humanize.naturalsize(os.stat(config.data_path + config.data_file).st_size))
    print('Peak Memory Usage =', humanize.naturalsize(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))
    print('User Mode Time =', resource.getrusage(resource.RUSAGE_SELF).ru_utime)
    print('System Mode Time =', resource.getrusage(resource.RUSAGE_SELF).ru_stime)

    print("done")
