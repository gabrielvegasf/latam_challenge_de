import os
import importlib
import json
from collections import defaultdict
from datetime import datetime

import resource
import humanize

from typing import List, Tuple

# @profile


def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:

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


    def read_file(file_path):

        # ---------------------
        # Recibe un nombre de archivo y lee linea por linea 
        # 
        # Param:
        #   file_path: Lista con los data del bloque. Esta lista puede ser de varios tipos: Row de Spark Data Frame o un Objeto File Object
        #   tweets_fecha: defaultdict que alamcena en conteo de tweets por fecha
        #   tweets_fecha_user: defaultdict que alamcena en conteo de tweets por fecha y usuario
        # 
        # Retorna:
        #   
        # ---------------------


        with open(file_path, 'r') as archivo:
            for linea in archivo:
                yield linea

    # ---------------------

    # Importacion de Modulos

    # Archvo de Configuracion 
    config = importlib.import_module("config")

    # Funciones Comunes
    common_functions = importlib.import_module("common_functions")

    # ---------------------

    tweets_fecha = defaultdict(int)
    tweets_fecha_user = defaultdict(lambda: defaultdict(int))
    lista_resultado_tuplas = []


    if file_path == "":
        # Si el archivo no es especificado se usa los indicados en Config
        file_path = config.data_path + config.data_file


    # Validamos el archivo de datos
    bValid, sMsg = common_functions.validate_file(file_path)

    if not bValid:
        print(sMsg)
        exit(0)

    # Leemos el archivo Linea a linea

    for linea in read_file(file_path):

        tweet = json.loads(linea)

        fecha_datetime = tweet['date']
        fecha = fecha_datetime[0:10]

        username = tweet['user']['username']

        # Incrementa el contador para la fecha correspondiente
        tweets_fecha[fecha] += 1
        tweets_fecha_user[fecha][username] += 1


    # Se tiene la data compilada en las estructuras "tweets_fecha" y "tweets_fecha_user"
    # Ordenamos las lista tweets_fecha de forma descendente por su valor y lo restringimos al TOP N

    top_10_tweets_fecha_ordenado = sorted(tweets_fecha.items(), key=lambda item: item[1], reverse=True)[:10]
    
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

    resultado = q1_memory("")

    print(resultado)

    print('Peak Memory Usage =', humanize.naturalsize(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))
    print('User Mode Time =', resource.getrusage(resource.RUSAGE_SELF).ru_utime)
    print('System Mode Time =', resource.getrusage(resource.RUSAGE_SELF).ru_stime)

    print("done")
