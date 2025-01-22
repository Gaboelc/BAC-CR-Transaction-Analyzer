import os
from csv_to_json import main

def process_all_csv(input_folder, output_folder):
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    for file_name in os.listdir(input_folder):
        if file_name.endswith('.csv'):
            input_path = os.path.join(input_folder, file_name)
            output_path = os.path.join(output_folder, file_name.replace('.csv', '.json'))

            print(f"Procesando archivo: {input_path}")
            try:
                main(input_path, output_path)
                print(f"Archivo procesado y guardado en: {output_path}")
            except Exception as e:
                print(f"Error procesando el archivo {file_name}: {e}")

if __name__ == "__main__":
    input_folder = './data/raw/'
    output_folder = './data/cleaned/'
    process_all_csv(input_folder, output_folder)
