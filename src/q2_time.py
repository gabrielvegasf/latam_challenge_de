import os
import importlib
import concurrent.futures
from pyspark.sql import SparkSession
from collections import defaultdict
import json

import emoji

from typing import List, Tuple

import resource
import humanize

def q2_time(file_path: str) -> List[Tuple[str, int]]:

    # ---------------------
    # Recibe un nombre de archivo y obtiene el  top 10 emojis más usados con su respectivo conteo. 
    # 
    # Param:
    #   file_path: ruta y nombre del archivo a procesar. 
    # 
    # Retorna:
    #   Lista de Tupla [emoji str, conteo int]
    #   
    # ---------------------


    def procesar_data_file(data_bloque, emoji_list):

        # ---------------------
        # Recibe un bloque de datos y procesa cada registro preocesa convirtiendo el JSON
        # 
        # Param:
        #   data_bloque: Lista con los data del bloque. Esta lista puede ser de varios tipos: Row de Spark Data Frame o un Objeto File Object
        #   emoji_list: defaultdict que alamcena en conteo de emojis 
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

            content_text = tweet['content']

            if tweet['quotedTweet'] is not None:
                content_text += ' - QT:' + tweet['quotedTweet']['content']

            # Buscamos la lista de emojis hay en un texto
            emojis = list(emoji.emoji_list(content_text))

            if (len(emojis) > 0):
                for emoji_item in emojis:
                    emoji_list[emoji_item['emoji']] += 1            

    # ---------------------

    # Archvo de Configuracion 
    config = importlib.import_module("config")

    # Funciones Comunes
    common_functions = importlib.import_module("common_functions")

    # Funciones para Manejo de Spark
    spark_functions = importlib.import_module("spark_functions")

    # ---------------------

    emoji_list = defaultdict(int)
    
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
            futures = [executor.submit(procesar_data_file, data_bloque, emoji_list) for data_bloque in spark_functions.read_spark_dataframe(sdf, registro_actual, data_size)]

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

            procesar_data_file(archivo, emoji_list)



    # Se tiene la data compilada en la estructura "emoji_list" 
    # Ordenamos las lista emoji_list de forma descendente por su valor y lo restringimos al TOP N

    top_10_emojis_ordenado = sorted(emoji_list.items(), key=lambda item: item[1], reverse=True)[:10]
    
    return top_10_emojis_ordenado



if __name__ == "__main__":

    resultado = q2_time("")

    print(resultado)

    print('Peak Memory Usage =', humanize.naturalsize(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))
    print('User Mode Time =', resource.getrusage(resource.RUSAGE_SELF).ru_utime)
    print('System Mode Time =', resource.getrusage(resource.RUSAGE_SELF).ru_stime)

    print("done")
