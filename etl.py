import requests
import pandas as pd
import io
import zipfile
import json
from datetime import datetime
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker
import logging  # Asegúrate de importar el módulo logging

# Configura el sistema de logging
logging.basicConfig(level=logging.INFO)

# Configura la cadena de conexión a la base de datos
DATABASE_URI = 'postgresql+psycopg2://juan:147@localhost:5433/datadb'

# Crea el motor de conexión
engine = create_engine(DATABASE_URI)

# Crea una sesión
Session = sessionmaker(bind=engine)
session = Session()

# Inicializa el contador
insert_count = 0

# Limite máximo de inserciones
max_insert_count = 100

try:
    # Imprimir las tablas de la base de datos
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    print("Conexión exitosa.")
    print("Tablas en la base de datos:", tables)

    # URL del archivo ZIP
    url = "https://github.com/sferez/BybitMarketData/raw/main/data/ETH/2024-02-12/trades_ETH_2024-02-12.zip"

    response = requests.get(url)
    if response.status_code == 200:
        zip_file = io.BytesIO(response.content)

        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            print("Contenido del archivo ZIP:")
            zip_ref.printdir()

            with zip_ref.open('trades_ETH_2024-02-12.jsonl') as jsonl_file:
                print("Leyendo el archivo .jsonl:")

                lines = [json.loads(line.decode('utf-8')) for line in jsonl_file]
                df = pd.DataFrame(lines)

                # Expandir la columna que contiene las listas de diccionarios
                column_with_lists = df.columns[1]
                df = df.explode(column_with_lists).reset_index(drop=True)
                df_transacciones = pd.json_normalize(df[column_with_lists])
                df_final = pd.concat([df[df.columns[0]], df_transacciones], axis=1)

                # Eliminar filas que tienen al menos un valor nulo
                df_final = df_final.dropna()

                # Renombrar las columnas para mayor claridad
                df_final.columns = ['t', 'T', 's', 'S', 'v', 'p', 'L', 'i', 'BT']

                # Configurar pandas para mostrar todas las columnas
                pd.set_option('display.max_columns', None)  # Mostrar todas las columnas
                pd.set_option('display.width', None)

                # Mostrar los primeros registros
                print(df_final.head())

                # Insertar los datos en la tabla Fecha y Tipo_transaccion
                for index, row in df_final.iterrows():
                    # Convertir el tiempo de milisegundos a fecha
                    fecha = datetime.fromtimestamp(row['t'] / 1000.0)  # Convertir a segundos
                    dia = fecha.day
                    mes = fecha.month
                    anio = str(fecha.year)

                    # Insertar en la tabla Fecha
                    insert_query_fecha = text(
                        "INSERT INTO Fecha (fecha, dia, mes, anio) VALUES (:fecha, :dia, :mes, :anio) RETURNING id"
                    )
                    fecha_id = session.execute(insert_query_fecha, {'fecha': fecha, 'dia': dia, 'mes': mes, 'anio': anio}).scalar()

                    # Verificar si el tipo de transacción ya existe
                    descripcion = row['S']
                    check_query_tipo = text("SELECT id FROM Tipo_transaccion WHERE descripcion = :descripcion")
                    transaccion_tipo_id = session.execute(check_query_tipo, {'descripcion': descripcion}).scalar()

                    # Si no existe, insertarlo
                    if transaccion_tipo_id is None:
                        insert_query_tipo = text(
                            "INSERT INTO Tipo_transaccion (descripcion) VALUES (:descripcion) RETURNING id"
                        )
                        transaccion_tipo_id = session.execute(insert_query_tipo, {'descripcion': descripcion}).scalar()

                    # Insertar los datos en la tabla Transaccion
                    insert_query_transacciones = text(
                        "INSERT INTO Transacciones (fecha_id, transaccion_tipo_id, simbolo, precio, valor) VALUES (:fecha_id, :transaccion_tipo_id, :simbolo, :precio, :valor)"
                    )
                    session.execute(insert_query_transacciones, {
                        'fecha_id': fecha_id,
                        'transaccion_tipo_id': transaccion_tipo_id,
                        'simbolo': row['s'],
                        'precio': float(row['p']),  # Asegúrate de que el campo de precio está correctamente nombrado
                        'valor': float(row['v'])     # Asegúrate de que el campo de valor está correctamente nombrado
                    })

                    # Incrementar el contador
                    insert_count += 1

                    # Verificar si se ha alcanzado el límite
                    if insert_count >= max_insert_count:
                        logging.info("Se alcanzó el límite de inserciones. Deteniendo el proceso.")
                        break  # Salir del bucle

                # Confirmar los cambios
                session.commit()
                print("Datos insertados correctamente ")

finally:
    # Cerrar la sesión
    session.close()