import os
import importlib
import json
from collections import defaultdict

from typing import List, Tuple


def q3_time(file_path: str) -> List[Tuple[str, int]]:

    lista_resultado_tuplas = []
    mentionedUsers_list = defaultdict(int)
    
    # Abre el archivo JSON en modo lectura
    with open(file_path, 'r') as archivo:

        for linea in archivo:
            # Carga la línea como un diccionario JSON
            tweet = json.loads(linea)


            if tweet['mentionedUsers'] is not None:

                mentionedUsers_tweet = tweet['mentionedUsers']

                for mentionedUsers_item in mentionedUsers_tweet:
                    mentionedUsers_list[ mentionedUsers_item['username'] ] += 1
    


    top_10_mentionedUsers = sorted(mentionedUsers_list.items(), key=lambda item: item[1], reverse=True)[:10]
    
    return top_10_mentionedUsers



if __name__ == "__main__":

    config = importlib.import_module("config")

    if not os.path.isfile(config.data_path + config.data_file):
        print(f"Archivo de datos: {config.data_file} no existe")
        exit(0)
    else :
        if os.stat(config.data_path + config.data_file).st_size == 0:
            print(f"Archivo de datos: {config.data_file} vacio")
            exit(0)


    resultado = q3_time(config.data_path + config.data_file)

    print(resultado)

