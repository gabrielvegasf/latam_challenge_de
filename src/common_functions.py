import os
import importlib

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
