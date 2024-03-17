import os
import importlib
import concurrent.futures
from pyspark.sql import SparkSession
from collections import defaultdict
import json

from datetime import datetime

import resource
import humanize

from typing import List, Tuple

#@profile



def q1_time(file_path: str) -> List[Tuple[datetime.date, str]]:

    # ---------------------
    # Recibe un nombre de archivo y obtiene el top 10 fechas donde hay más tweets. Mencionar el usuario (username) que más publicaciones tiene por cada uno de esos días. 
    # 
    # Param:
    #   file_path: ruta y nombre del archivo a procesar. 
    # 
    # Retorna:
    #   Lista de Tupla [fecha en formato datetime.date , username str]
    #   
    # ---------------------


    def procesar_data_file(data_bloque, tweets_fecha, tweets_fecha_user):

        # ---------------------
        # Recibe un bloque de datos y procesa cada registro preocesa convirtiendo el JSON
        # 
        # Param:
        #   data_bloque: Lista con los data del bloque. Esta lista puede ser de varios tipos: Row de Spark Data Frame o un Objeto File Object
        #   tweets_fecha: defaultdict que alamcena en conteo de tweets por fecha
        #   tweets_fecha_user: defaultdict que alamcena en conteo de tweets por fecha y usuario
        # 
        # Retorna:
        #   
        # ---------------------

        
        for row in data_bloque:

            # Se verifica el tipo de dato, para saber la forma de extraerlo
            if isinstance(row, str):
                tweet = json.loads(row)
            else:
                tweet = json.loads(row['value'])

            fecha_datetime = tweet['date']
            fecha = fecha_datetime[0:10]

            username = tweet['user']['username']

            # Incrementa el contador para la fecha correspondiente
            tweets_fecha[fecha] += 1
            tweets_fecha_user[fecha][username] += 1

    # ---------------------


    tweets_fecha = defaultdict(int)
    tweets_fecha_user = defaultdict(lambda: defaultdict(int))
    lista_resultado_tuplas = []

    config = importlib.import_module("config")
    common_functions = importlib.import_module("common_functions")
    spark_functions = importlib.import_module("spark_functions")


    if file_path == "":
        # Si el archivo no es especificado se usa los indicados en Config
        file_path = config.data_path + config.data_file


    # Validamos el archivo de datos
    bValid, sMsg = common_functions.validate_file(file_path)

    if not bValid:
        print(sMsg)
        exit(0)


    if config.use_spark_service == True:
        # ---------------------
        # Se procesa la data usando el servico Spark Especificado y se realiza con Threads
        # ---------------------

        # Creamos una Session Spark
        objSpark = spark_functions.create_spark_session()

        # Leer el archivo a un Spark DataFrame 
        sdf = objSpark.read.text(file_path)

        registro_actual = 0

        data_size = config.data_read_size  # Registro por bloque 

        # Usamos una estrategia de lectura en bloques para hacer proceamiento en Threads
        with concurrent.futures.ThreadPoolExecutor() as executor:
            
            # Procesar cada bloque del archivo en un hilo separado
            futures = [executor.submit(procesar_data_file, data_bloque, tweets_fecha, tweets_fecha_user) for data_bloque in spark_functions.read_spark_dataframe(sdf, registro_actual, data_size)]

            # Esperar a que todos los hilos terminen
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()  
                except Exception as e:
                    print(f'Ocurrió un error: {e}')

        # Detener la sesión de Spark
        objSpark.stop()

    else :
        # ---------------------
        # Se procesa la data Realizando una lectura del archivo via filesystem
        # ---------------------

        # Abre el archivo JSON en modo lectura
        with open(file_path, 'r') as archivo:

            procesar_data_file(archivo, tweets_fecha, tweets_fecha_user)



    # De cualquier metodo alicado se tiene la data compilada en las estructuras "tweets_fecha" y "tweets_fecha_user"
    # Ordenamos las lista tweets_fecha de forma descendente por su valor y lo restringimos al TOP N

    top_10_tweets_fecha_ordenado = sorted(tweets_fecha.items(), key=lambda item: item[1], reverse=True)[:10]
    print(top_10_tweets_fecha_ordenado)

    # Del Top N obtenido buscamos en tweets_fecha_user para obtner los users que tienen mas tweets esa fecha
    
    for fecha, total_tweets in top_10_tweets_fecha_ordenado:

        # Ordenamos las lista tweets_fecha_user de forma descendente por su valor y lo restringimos al TOP N
        top_1_tweets_fecha_user = sorted(tweets_fecha_user[fecha].items(), key=lambda item: item[1], reverse=True)[:1]

        top_user_fecha, valor = top_1_tweets_fecha_user[0]

        #Formateamos la fecha en el especificado
        fecha_date = datetime.strptime(fecha, "%Y-%m-%d").date()

        #Creamos la Tupla
        lista_resultado_tuplas.append((fecha_date, top_user_fecha))

    return lista_resultado_tuplas



if __name__ == "__main__":

    resultado = q1_time("")

    print(resultado)

    print('Peak Memory Usage =', humanize.naturalsize(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))
    print('User Mode Time =', resource.getrusage(resource.RUSAGE_SELF).ru_utime)
    print('System Mode Time =', resource.getrusage(resource.RUSAGE_SELF).ru_stime)

    print("done")
