import os
import json
import shutil
from pymongo import MongoClient

def load_json_to_mongodb(json_folder, db_name, collection_name, mongo_uri="mongodb://localhost:27017/"):
    # Conexión al servidor de MongoDB
    client = MongoClient(mongo_uri)
    db = client[db_name]  # Crear o acceder a la base de datos
    collection = db[collection_name]  # Crear o acceder a la colección

    # Carpeta para mover los archivos procesados
    old_data_folder = os.path.join(json_folder, "OldData")
    if not os.path.exists(old_data_folder):
        os.makedirs(old_data_folder)

    # Recorrer los archivos JSON en la carpeta
    for file_name in os.listdir(json_folder):
        if file_name.endswith('.json'):
            file_path = os.path.join(json_folder, file_name)
            old_data_path = os.path.join(old_data_folder, file_name)

            # Cargar el archivo JSON
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # Insertar los datos en la colección
            try:
                if isinstance(data, list):
                    collection.insert_many(data)  # Si es una lista de documentos
                else:
                    collection.insert_one(data)  # Si es un solo documento

                # Mover el archivo a la carpeta OldData después de procesarlo
                shutil.move(file_path, old_data_path)
                print(f"Archivo {file_name} procesado y movido a OldData.")
            except Exception as e:
                print(f"Error procesando el archivo {file_name}: {e}")

    # Cerrar la conexión
    client.close()
    print("Proceso completado. Conexión a MongoDB cerrada.")

if __name__ == "__main__":
    json_folder = './data/cleaned/'
    db_name = 'TransaccionesBAC'
    collection_name = 'Transacciones_Tarjeta_Dolares_3193'
    mongo_uri="mongodb://localhost:27017/"
    load_json_to_mongodb(json_folder, db_name, collection_name)
