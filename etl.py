import requests
import pandas as pd
import io
import zipfile
import json
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker

# Configura la cadena de conexión
DATABASE_URI = 'postgresql+psycopg2://juan:147@localhost:5433/datadb'

# Crea el motor de conexión
engine = create_engine(DATABASE_URI)

# Crea una sesión
Session = sessionmaker(bind=engine)
session = Session()

try:
    # Imprimir las tablas de la base de datos
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    print("Conexión exitosa.")
    print("Tablas en la base de datos:", tables)

except Exception as e:
    print("Error al conectar a la base de datos:", e)

finally:
    # Cerrar la sesión
    session.close()


#
# # URL del archivo ZIP
# url = "https://github.com/sferez/BybitMarketData/raw/main/data/ETH/2024-02-12/trades_ETH_2024-02-12.zip"
#
# response = requests.get(url)
# if response.status_code == 200:
#     zip_file = io.BytesIO(response.content)
#
#     with zipfile.ZipFile(zip_file, 'r') as zip_ref:
#         print("Contenido del archivo ZIP:")
#         zip_ref.printdir()
#
#         with zip_ref.open('trades_ETH_2024-02-12.jsonl') as jsonl_file:
#             print("Leyendo el archivo .jsonl:")
#
#             lines = [json.loads(line.decode('utf-8')) for line in jsonl_file]
#             df = pd.DataFrame(lines)
#
#             # Expandir la columna que contiene las listas de diccionarios
#             column_with_lists = df.columns[1]
#             df = df.explode(column_with_lists).reset_index(drop=True)
#             df_transacciones = pd.json_normalize(df[column_with_lists])
#             df_final = pd.concat([df[df.columns[0]], df_transacciones], axis=1)
#
#             # Reemplazar valores nulos en la columna 'BT' con False
#             df_final['BT'] = df_final['BT'].fillna(False)
#
#             # Convertir los valores null a 0
#             df_final.fillna(0, inplace=True)
#
#             # Renombrar las columnas para mayor claridad
#             df_final.columns = ['t', 'T', 's', 'S', 'v', 'p', 'L', 'i', 'BT']
#
#             # Preparar el DataFrame para la inserción en la tabla Transacciones
#             df_final.rename(columns={
#                 't': 'fecha_id',  # Asegúrate de usar el nombre correcto
#                 's': 'simbolo_id',
#                 'p': 'precio',
#                 'v': 'valor'
#             }, inplace=True)
#
#             # Añadir una columna 'id' con un valor único
#             df_final['id'] = range(1, len(df_final) + 1)
#
#             # Asignar un valor por defecto a transaccion_tipo_id
#             df_final['transaccion_tipo_id'] = 1  # Cambia esto según tu lógica
#
#             # Seleccionar solo las columnas que se van a insertar
#             df_to_insert = df_final[['id', 'fecha_id', 'transaccion_tipo_id', 'simbolo_id', 'precio', 'valor']]
#
#             # Imprimir el DataFrame final para verificar
#             print(f"DataFrame final:\n{df_to_insert.head()}")
#
#             # Insertar los datos en la tabla Transacciones
#             try:
#                 df_to_insert.to_sql('transaction', con=engine, if_exists='append', index=False)
#                 print("Datos insertados correctamente.")
#             except Exception as e:
#                 print(f"Ocurrió un error al insertar los datos: {e}")

