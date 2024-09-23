
import requests
import pandas as pd
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
        
            # Leer el archivo .jsonl en una lista de diccionarios
            lines = [json.loads(line.decode('utf-8')) for line in jsonl_file]
            
            # Convertir la lista de diccionarios en un DataFrame de pandas
            df = pd.DataFrame(lines)
            print(type(df))
             # Verificar las columnas del DataFrame original
            print(f"Columnas del DataFrame original: {df.columns}")
            
             # Mostrar el DataFrame original
            # print(f"DataFrame original:\n{df.head()}")
            
            
            # Identificar la columna que contiene las listas de transacciones (normalmente debería ser la segunda columna)
            # Asegúrate de usar el nombre correcto de la columna en lugar de un índice numérico.
            column_with_lists = df.columns[1]  # Esto obtiene el nombre de la segunda columna
            
            
            # Expandir la columna que contiene las listas de diccionarios
            df = df.explode(column_with_lists).reset_index(drop=True)  # Explode la columna de las listas
            
            
            # Convertir las filas de diccionarios en columnas
            df_transacciones = pd.json_normalize(df[column_with_lists])  # Normalizar los diccionarios
            
            # Concatenar con la columna de timestamp original (primera columna)
            df_final = pd.concat([df[df.columns[0]], df_transacciones], axis=1)
            
            # Reemplazar valores nulos en la columna 'BT' con False
            df_final['BT'].fillna(False, inplace=True)
            
            #convertimos los valores null a 0
            df_final.fillna(0, inplace=True)

            
            # Renombrar las columnas para mayor claridad
            df_final.columns = ['t', 'T', 's', 'S', 'v', 'p', 'L', 'i', 'BT']
            
            
             # Configurar pandas para mostrar todas las columnas y filas
            # pd.set_option('display.max_columns', None)  # Mostrar todas las columnas
            # pd.set_option('display.max_rows', None)     # Mostrar todas las filas
            # pd.set_option('display.expand_frame_repr', False)  # No cortar filas al mostrar el DataFrame
            
            # Mostrar el DataFrame final con las columnas expandidas
            # Mostrar el DataFrame final completo
            # print(f"DataFrame final:\n{df_final.head()}")
            print(f"DataFrame final:\n{df_final.head()}")
            
            