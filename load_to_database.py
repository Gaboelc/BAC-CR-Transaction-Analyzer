import os
import json
import shutil
from pymongo import MongoClient

def load_json_to_mongodb(json_folder, db_name, collection_name, mongo_uri="mongodb://localhost:27017/"):
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]

    old_data_folder = os.path.join(json_folder, "OldData")
    if not os.path.exists(old_data_folder):
        os.makedirs(old_data_folder)

    for file_name in os.listdir(json_folder):
        if file_name.endswith('.json'):
            file_path = os.path.join(json_folder, file_name)
            old_data_path = os.path.join(old_data_folder, file_name)

            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            try:
                if isinstance(data, list):
                    collection.insert_many(data)
                else:
                    collection.insert_one(data)

                shutil.move(file_path, old_data_path)
                print(f"Archivo {file_name} procesado y movido a OldData.")
            except Exception as e:
                print(f"Error procesando el archivo {file_name}: {e}")

    client.close()
    print("Proceso completado. Conexión a MongoDB cerrada.")

if __name__ == "__main__":
    json_folder = './data/cleaned/'
    db_name = 'TransaccionesBAC'
    collection_name = 'Transacciones_Tarjeta_Dolares_3193'
    mongo_uri="mongodb://localhost:27017/"
    load_json_to_mongodb(json_folder, db_name, collection_name)
