import csv
import json
import os
from datetime import datetime

def month_name_from_number(month_num):
    months = [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    ]
    return months[month_num - 1]

def parse_dd_mm_yyyy(date_str):
    try:
        day, month, year = date_str.split('/')
        return int(day), int(month), int(year)
    except:
        return None, None, None

def procesar_csv_a_json(ruta_csv, ruta_salida_json):
    with open(ruta_csv, mode='r', encoding='utf-8') as f:
        contenido = [line.strip() for line in f]
    if len(contenido) < 2:
        return
    cabecera = contenido[1].split(',')
    if len(cabecera) < 9:
        return
    producto = cabecera[0].strip()
    nombre_cliente = cabecera[1].strip()
    fecha_corte = cabecera[2].strip()
    fecha_pago_minimo = cabecera[3].strip()
    pago_minimo_colones = float(cabecera[4].strip())
    pago_minimo_dolares = float(cabecera[5].strip())
    fecha_pago_contado = cabecera[6].strip()
    pago_contado_colones = float(cabecera[7].strip())
    pago_contado_dolares = float(cabecera[8].strip())
    _, mes_num, anio = parse_dd_mm_yyyy(fecha_pago_minimo)
    nombre_mes = month_name_from_number(mes_num)
    indice_inicio_transacciones = None
    for i, linea in enumerate(contenido):
        if linea.lower().startswith("date,") and "local" in linea.lower() and "dollars" in linea.lower():
            indice_inicio_transacciones = i + 1
            break
    if indice_inicio_transacciones is None:
        return
    indice_fin_transacciones = None
    for i in range(indice_inicio_transacciones, len(contenido)):
        if contenido[i].lower().startswith("current interest month/local"):
            indice_fin_transacciones = i
            break
    if indice_fin_transacciones is None:
        indice_fin_transacciones = len(contenido)
    registros = []
    for i in range(indice_inicio_transacciones, indice_fin_transacciones):
        linea = contenido[i]
        if not linea.strip():
            continue
        cols = linea.split(',')
        if len(cols) < 4:
            continue
        fecha_gasto = cols[0].strip()
        descripcion_raw = cols[1].strip()
        descripcion = ' '.join(descripcion_raw.split())
        try:
            gasto_colones = float(cols[2].strip()) if cols[2].strip() else 0.0
        except:
            gasto_colones = 0.0
        try:
            gasto_dolares = float(cols[3].strip()) if cols[3].strip() else 0.0
        except:
            gasto_dolares = 0.0
        if fecha_gasto == "" and gasto_colones == 0.0 and gasto_dolares == 0.0:
            continue
        registro = {
            "Year": anio,
            "Month": nombre_mes,
            "Product": producto,
            "Minimum Payment Due Date": fecha_pago_minimo,
            "Minimum Payment in Colones": pago_minimo_colones,
            "Minimum Payment in Dolars": pago_minimo_dolares,
            "Cash Payment Due Date": fecha_pago_contado,
            "Cash Payment in Colones": pago_contado_colones,
            "Cash Payment in Dolars": pago_contado_dolares,
            "Expense Date": fecha_gasto,
            "Description": descripcion,
            "Expense in Colones": gasto_colones,
            "Expense in Dolars": gasto_dolares
        }
        registros.append(registro)
    with open(ruta_salida_json, 'w', encoding='utf-8') as f_out:
        json.dump(registros, f_out, ensure_ascii=False, indent=4)


if __name__ == "__main__":
    carpeta_csv = "./data/raw/"
    nombre_archivo_csv = "Transacciones_July_2024.csv"
    ruta_completa_csv = os.path.join(carpeta_csv, nombre_archivo_csv)

    ruta_salida = os.path.join(carpeta_csv, "resultado.json")

    procesar_csv_a_json(ruta_completa_csv, ruta_salida)
