import os
import importlib
import json
from collections import defaultdict

from typing import List, Tuple

import resource
import humanize

def q3_memory(file_path: str) -> List[Tuple[str, int]]:

    # ---------------------
    # Recibe un nombre de archivo y obtiene el top 10 histórico de usuarios (username) más influyentes en función del conteo de las menciones (@) que registra cada uno de ellos. 
    # 
    # Param:
    #   file_path: ruta y nombre del archivo a procesar. 
    # 
    # Retorna:
    #   Lista de Tupla [emoji str, conteo int]
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


    mentionedUsers_list = defaultdict(int)

    if file_path == "":
        # Si el archivo no es especificado se usa los indicados en Config
        file_path = config.data_path + config.data_file


    # Validamos el archivo de datos
    bValid, sMsg = common_functions.validate_file(file_path)

    if not bValid:
        print(sMsg)
        exit(0)

    
    # Abre el archivo JSON en modo lectura
    for linea in read_file(file_path):

        tweet = json.loads(linea)

        # Se realiza la busqueda en el campo "mentionedUsers"
        # El campo posee una lista de las menciones

        if tweet['mentionedUsers'] is not None:

            mentionedUsers_tweet = tweet['mentionedUsers']

            for mentionedUsers_item in mentionedUsers_tweet:
                mentionedUsers_list[ mentionedUsers_item['username'] ] += 1

    # Se tiene la data compilada en la estructura "mentionedUsers_list" 
    # Ordenamos las lista mentionedUsers_list de forma descendente por su valor y lo restringimos al TOP N

    top_10_mentionedUsers = sorted(mentionedUsers_list.items(), key=lambda item: item[1], reverse=True)[:10]
    
    return top_10_mentionedUsers


if __name__ == "__main__":

    resultado = q3_memory("")

    print(resultado)

    print('Peak Memory Usage =', humanize.naturalsize(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))
    print('User Mode Time =', resource.getrusage(resource.RUSAGE_SELF).ru_utime)
    print('System Mode Time =', resource.getrusage(resource.RUSAGE_SELF).ru_stime)

    print("done")
    
