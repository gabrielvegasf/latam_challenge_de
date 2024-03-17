import os
import importlib
import json
from collections import defaultdict
from datetime import datetime

import resource
import humanize

from typing import List, Tuple

# @profile

def read_file(file_path):
    with open(file_path, 'r') as f:
        for linea in f:
            yield linea


def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:

    lista_resultado_tuplas = []
    tweets_fecha = defaultdict(int)
    tweets_fecha_user = defaultdict(lambda: defaultdict(int))

    # Abre el archivo JSON en modo lectura
    # with open(file_path, 'r') as archivo:

    #     for linea in archivo:
    for linea in read_file(file_path):
        # Carga la línea como un diccionario JSON
        tweet = json.loads(linea)

        fecha_datetime = tweet['date']
        fecha = fecha_datetime[0:10]

        # print((fecha))

        username = tweet['user']['username']

        # print((username))
        
        # Incrementa el contador para la fecha correspondiente
        tweets_fecha[fecha] += 1
        tweets_fecha_user[fecha][username] += 1


    top_10_tweets_fecha_ordenado = sorted(tweets_fecha.items(), key=lambda item: item[1], reverse=True)[:10]
    print(top_10_tweets_fecha_ordenado)

    # Imprime el total de tweets por fecha
    for fecha, total_tweets in top_10_tweets_fecha_ordenado:

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


    resultado = q1_memory(config.data_path + config.data_file)

    print(resultado)

    print('File Size ', humanize.naturalsize(os.stat(config.data_path + config.data_file).st_size))
    print('Peak Memory Usage =', humanize.naturalsize(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))
    print('User Mode Time =', resource.getrusage(resource.RUSAGE_SELF).ru_utime)
    print('System Mode Time =', resource.getrusage(resource.RUSAGE_SELF).ru_stime)

    print("done")
