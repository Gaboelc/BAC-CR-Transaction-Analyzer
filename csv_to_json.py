import pandas as pd
import json
import re

def extract_month_year(file_name):
    match = re.search(r"Transacciones_(\w+)_(\d{4})\.csv", file_name)
    if not match:
        raise ValueError("El nombre del archivo no sigue el formato esperado: Transacciones_Mes_Año.csv")
    return match.group(1), int(match.group(2))

def load_csv(file_path, encoding='latin1'):
    return pd.read_csv(file_path, encoding=encoding)

def extract_metadata(df):
    account = str(df.iloc[0, 2]).strip()
    currency = str(df.iloc[0, 3]).strip()
    opening_balance = str(df.iloc[0, 4]).strip().replace(",", ".")
    return account, currency, float(opening_balance)

def clean_data(df):
    start_index = df[df["Número de Clientes"] == "Fecha de Transacción"].index[0] + 1
    if "Resumen de Estado Bancario" in df["Número de Clientes"].values:
        end_index = df[df["Número de Clientes"] == "Resumen de Estado Bancario"].index[0]
    else:
        end_index = len(df)
    transaction_data = df.iloc[start_index:end_index, [0, 1, 2, 3, 4, 5, 6]].reset_index(drop=True)
    transaction_data.columns = [
        "Transaction Date",
        "Transaction Reference",
        "Transaction Code",
        "Description",
        "Debit",
        "Credit",
        "Balance",
    ]
    transaction_data = transaction_data.dropna(subset=["Transaction Date"]).reset_index(drop=True)
    transaction_data.fillna("", inplace=True)
    transaction_data["Description"] = (
        transaction_data["Description"]
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )
    for col in ["Debit", "Credit", "Balance"]:
        transaction_data[col] = (
            transaction_data[col]
            .apply(lambda x: str(x).strip().replace(",", "."))
            .astype(float)
        )
    transaction_data["Transaction Reference"] = (
        transaction_data["Transaction Reference"]
        .apply(lambda x: str(x).strip())  # Eliminar espacios
        .apply(lambda x: int(x) if x.isdigit() else x)  # Convertir a entero si es posible
    )
    return transaction_data

def generate_flat_json(transaction_data, account, currency, opening_balance, year, month):
    flat_data = []
    for _, row in transaction_data.iterrows():
        flat_data.append({
            "Year": year,
            "Month": month,
            "Account": account,
            "Currency": currency,
            "Opening Balance": opening_balance,
            "Transaction Date": row["Transaction Date"],
            "Transaction Reference": row["Transaction Reference"],
            "Transaction Code": str(row["Transaction Code"]).strip(),
            "Description": str(row["Description"]).strip(),
            "Debit": row["Debit"],
            "Credit": row["Credit"],
            "Balance": row["Balance"],
        })
    return flat_data

def save_json(data, output_file):
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    print(f"JSON generado y guardado en: {output_file}")

def main(file_path, output_file):
    file_name = file_path.split('/')[-1]
    month, year = extract_month_year(file_name)
    df = load_csv(file_path)
    account, currency, opening_balance = extract_metadata(df)
    transaction_data = clean_data(df)
    json_data = generate_flat_json(transaction_data, account, currency, opening_balance, year, month)
    save_json(json_data, output_file)


if __name__ == "__main__":
    file_path = './data/raw/Transacciones_Enero_2025.csv'
    output_file = 'resultado.json'
    main(file_path, output_file)
