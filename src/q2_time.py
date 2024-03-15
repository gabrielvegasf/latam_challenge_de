import os
import importlib
import json
from collections import defaultdict
import emoji

from typing import List, Tuple



def q2_time(file_path: str) -> List[Tuple[str, int]]:

    lista_resultado_tuplas = []
    emoji_list = defaultdict(int)
    
    # Abre el archivo JSON en modo lectura
    with open(file_path, 'r') as archivo:

        for linea in archivo:
            # Carga la línea como un diccionario JSON
            tweet = json.loads(linea)

            content_text = tweet['content']

            if tweet['quotedTweet'] is not None:
                content_text += ' - QT:' + tweet['quotedTweet']['content']

            emojis = list(emoji.emoji_list(content_text))

            if (len(emojis) > 0):
                for emoji_item in emojis:
                    emoji_list[emoji_item['emoji']] += 1
    


    top_10_emojis_ordenado = sorted(emoji_list.items(), key=lambda item: item[1], reverse=True)[:10]
    
    return top_10_emojis_ordenado



if __name__ == "__main__":

    config = importlib.import_module("config")

    if not os.path.isfile(config.data_path + config.data_file):
        print(f"Archivo de datos: {config.data_file} no existe")
        exit(0)
    else :
        if os.stat(config.data_path + config.data_file).st_size == 0:
            print(f"Archivo de datos: {config.data_file} vacio")
            exit(0)


    resultado = q2_time(config.data_path + config.data_file)

    print(resultado)

