import os
import shutil
from csv_to_json import main

def process_all_csv(input_folder, output_folder):
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    old_raw_data_folder = os.path.join(input_folder, "OldRawData")
    if not os.path.exists(old_raw_data_folder):
        os.makedirs(old_raw_data_folder)

    for file_name in os.listdir(input_folder):
        if file_name.endswith('.csv'):
            input_path = os.path.join(input_folder, file_name)
            output_path = os.path.join(output_folder, file_name.replace('.csv', '.json'))
            old_data_path = os.path.join(old_raw_data_folder, file_name)

            print(f"Procesando archivo: {input_path}")
            try:
                main(input_path, output_path)
                print(f"Archivo procesado y guardado en: {output_path}")

                shutil.move(input_path, old_data_path)
                print(f"Archivo {file_name} movido a {old_raw_data_folder}.")
            except Exception as e:
                print(f"Error procesando el archivo {file_name}: {e}")

if __name__ == "__main__":
    input_folder = '../data/raw/'
    output_folder = '../data/cleaned/'
    process_all_csv(input_folder, output_folder)
