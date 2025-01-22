import os
import shutil
from csv_to_json_credit import procesar_csv_a_json

def procesar_todos_los_csv(carpeta_entrada, carpeta_salida, carpeta_old):
    if not os.path.exists(carpeta_salida):
        os.makedirs(carpeta_salida)
    if not os.path.exists(carpeta_old):
        os.makedirs(carpeta_old)

    for filename in os.listdir(carpeta_entrada):
        if filename.lower().endswith('.csv'):
            ruta_csv = os.path.join(carpeta_entrada, filename)
            base_name = os.path.splitext(filename)[0]
            ruta_json = os.path.join(carpeta_salida, base_name + '.json')

            print(f"Procesando: {ruta_csv} -> {ruta_json}")
            procesar_csv_a_json(ruta_csv, ruta_json)

            destino = os.path.join(carpeta_old, filename)
            print(f"Moviendo {ruta_csv} a {destino}")
            shutil.move(ruta_csv, destino)

if __name__ == "__main__":
    carpeta_csv = "./data/raw"
    carpeta_salida = "./data/cleaned/Credit"
    carpeta_old = "./data/raw/OldDataCredit"
    
    procesar_todos_los_csv(carpeta_csv, carpeta_salida, carpeta_old)
