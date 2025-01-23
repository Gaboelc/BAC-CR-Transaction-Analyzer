import os
import json
import shutil
import sqlite3

def load_json_to_sqlite(json_folder, db_path='TransaccionesBAC.db', table_name='Transacciones', final_destination_folder='./data/cleaned/Credit/OldDataCredit'):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        Year INTEGER,
        Month TEXT,
        Product TEXT,
        Minimum_Payment_Due_Date TEXT,
        Minimum_Payment_in_Colones REAL,
        Minimum_Payment_in_Dolars REAL,
        Cash_Payment_Due_Date TEXT,
        Cash_Payment_in_Colones REAL,
        Cash_Payment_in_Dolars REAL,
        Expense_Date TEXT,
        Description TEXT,
        Expense_in_Colones REAL,
        Expense_in_Dolars REAL
    )
    """
    cursor.execute(create_table_query)
    conn.commit()
    processed_files = []
    for file_name in os.listdir(json_folder):
        if file_name.lower().endswith('.json'):
            file_path = os.path.join(json_folder, file_name)
            try:
                with open(file_path, 'r', encoding='latin-1') as f:
                    data = json.load(f)
                if isinstance(data, list):
                    for record in data:
                        cursor.execute(f"""
                            INSERT INTO {table_name} (
                                Year,
                                Month,
                                Product,
                                Minimum_Payment_Due_Date,
                                Minimum_Payment_in_Colones,
                                Minimum_Payment_in_Dolars,
                                Cash_Payment_Due_Date,
                                Cash_Payment_in_Colones,
                                Cash_Payment_in_Dolars,
                                Expense_Date,
                                Description,
                                Expense_in_Colones,
                                Expense_in_Dolars
                            )
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            record.get("Year", None),
                            record.get("Month", None),
                            record.get("Product", None),
                            record.get("Minimum Payment Due Date", None),
                            record.get("Minimum Payment in Colones", 0.0),
                            record.get("Minimum Payment in Dolars", 0.0),
                            record.get("Cash Payment Due Date", None),
                            record.get("Cash Payment in Colones", 0.0),
                            record.get("Cash Payment in Dolars", 0.0),
                            record.get("Expense Date", None),
                            record.get("Description", None),
                            record.get("Expense in Colones", 0.0),
                            record.get("Expense in Dolars", 0.0)
                        ))
                    conn.commit()
                else:
                    record = data
                    cursor.execute(f"""
                        INSERT INTO {table_name} (
                            Year,
                            Month,
                            Product,
                            Minimum_Payment_Due_Date,
                            Minimum_Payment_in_Colones,
                            Minimum_Payment_in_Dolars,
                            Cash_Payment_Due_Date,
                            Cash_Payment_in_Colones,
                            Cash_Payment_in_Dolars,
                            Expense_Date,
                            Description,
                            Expense_in_Colones,
                            Expense_in_Dolars
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        record.get("Year", None),
                        record.get("Month", None),
                        record.get("Product", None),
                        record.get("Minimum Payment Due Date", None),
                        record.get("Minimum Payment in Colones", 0.0),
                        record.get("Minimum Payment in Dolars", 0.0),
                        record.get("Cash Payment Due Date", None),
                        record.get("Cash Payment in Colones", 0.0),
                        record.get("Cash Payment in Dolars", 0.0),
                        record.get("Expense Date", None),
                        record.get("Description", None),
                        record.get("Expense in Colones", 0.0),
                        record.get("Expense in Dolars", 0.0)
                    ))
                    conn.commit()
                processed_files.append(file_path)
            except Exception as e:
                print(f"Error procesando el archivo {file_name}: {e}")
    conn.close()
    if not os.path.exists(final_destination_folder):
        os.makedirs(final_destination_folder)
    for file_path in processed_files:
        file_name = os.path.basename(file_path)
        destination = os.path.join(final_destination_folder, file_name)
        try:
            shutil.move(file_path, destination)
            print(f"Archivo {file_name} movido a {final_destination_folder}.")
        except Exception as e:
            print(f"Error moviendo el archivo {file_name}: {e}")

if __name__ == "__main__":
    json_folder = './data/cleaned/DataCredit/0243'
    db_path = './data/dbs/TransaccionesBAC.db'
    table_name = 'Transacciones_Tarjeta_Credito_0243'
    final_destination_folder = './data/cleaned/DataCredit/0243/OldData0243'
    load_json_to_sqlite(json_folder, db_path, table_name, final_destination_folder)
