import pandas as pd
import requests
import io
import pdb
import zipfile
import json


url = "https://github.com/sferez/BybitMarketData/raw/main/data/ETH/2024-02-12/trades_ETH_2024-02-12.zip"

response = requests.get(url)
print(response)

if response.status_code == 200:
    print(response.status_code)
    zip_file = io.BytesIO(response.content)

    # Abrir el archivo ZIP
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        # Listar los archivos en el archivo ZIP
        print("Contenido del archivo ZIP:")
        zip_ref.printdir()  # Muestra los archivos en el ZIP

        # Abrir el archivo .jsonl dentro del ZIP
        with zip_ref.open('trades_ETH_2024-02-12.jsonl') as jsonl_file:
            print("Leyendo el archivo .jsonl:")
        
            # # Leer y procesar el contenido del archivo línea por línea
            # for line in jsonl_file:
            #     json_data = json.loads(line.decode('utf-8'))  # Decodificar y cargar cada línea como JSON
            #     print(json_data)  # Muestra el contenido de cada línea JSON
            
            
            # Leer el archivo .jsonl en un DataFrame de pandas
            lines = [json.loads(line.decode('utf-8')) for line in jsonl_file]
            df = pd.DataFrame(lines)
            print(type(df))
            
            
            # Mostrar las primeras filas del DataFrame
            print("Primeras filas del DataFrame:")
            print(df.head())

            # Extraer columnas específicas
            if {'t','d'}.issubset(df.columns):
                df = df[['t','d']]
                print("Datos extraídos del archivo:")
                print(df.head())
            else:
                print("No se encontraron todas las columnas esperadas en el DataFrame.")