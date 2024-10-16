import requests
import pandas as pd
import io
import zipfile
import json
from datetime import datetime
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker
import logging

# Configura el sistema de logging
logging.basicConfig(level=logging.INFO)

# Configura la cadena de conexión a la base de datos
DATABASE_URI = 'postgresql+psycopg2://juan:147@localhost:5433/datadb'

# Crea el motor de conexión
engine = create_engine(DATABASE_URI)

# Inicializa el contador
insert_count = 0
chunk_size = 100000  # Número de filas por chunk



try:
    # Imprimir las tablas de la base de datos
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    logging.info("Conexión exitosa.")
    logging.info("Tablas en la base de datos: %s", tables)

    # URL del archivo ZIP
    url = "https://github.com/sferez/BybitMarketData/raw/main/data/ETH/2024-02-12/trades_ETH_2024-02-12.zip"
    response = requests.get(url)
    response.raise_for_status()  # Asegura que la solicitud fue exitosa

    zip_file = io.BytesIO(response.content)
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        logging.info("Contenido del archivo ZIP:")
        zip_ref.printdir()

        with zip_ref.open('trades_ETH_2024-02-12.jsonl') as jsonl_file:
            logging.info("Leyendo el archivo .jsonl:")
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

            # Mostrar los primeros registros
            logging.info("Primeros registros del DataFrame:")
            logging.info("%s", df_final.head())

            # # filas y columnas
            # num_filas, num_columnas = df_final.shape
            # logging.info("El DataFrame tiene %d filas y %d columnas.", num_filas, num_columnas)

            # Usar la sesión de SQLAlchemy
            with sessionmaker(bind=engine)() as session:

                # Iterar sobre el DataFrame en chunks, tecnica de scalin to large datasets
                for start in range(0, len(df_final), chunk_size):
                    chunk = df_final.iloc[start:start + chunk_size]

                    for index, row in chunk.iterrows():
                        # Convertir el tiempo de milisegundos a fecha
                        fecha = datetime.fromtimestamp(row['t'] / 1000.0)
                        dia, mes, anio = fecha.day, fecha.month, str(fecha.year)

                        # Insertar en la tabla Fecha
                        insert_query_fecha = text(
                            "INSERT INTO Fecha (fecha, dia, mes, anio) VALUES (:fecha, :dia, :mes, :anio) RETURNING id"
                        )
                        fecha_id = session.execute(insert_query_fecha,
                                                   {'fecha': fecha, 'dia': dia, 'mes': mes, 'anio': anio}).scalar()

                        # Verificar si el tipo de transacción ya existe
                        descripcion = row['S']
                        id = row['i']
                        check_query_tipo = text(
                            "SELECT id FROM Tipo_transaccion WHERE id = :id AND descripcion = :descripcion"
                        )

                        transaccion_tipo_id = session.execute(check_query_tipo,
                                                              {'id': id, 'descripcion': descripcion}).scalar()

                        # Si no existe, insertarlo
                        if transaccion_tipo_id is None:
                            insert_query_tipo = text(
                                "INSERT INTO Tipo_transaccion (id, descripcion) VALUES (:id, :descripcion) RETURNING id"
                            )
                            transaccion_tipo_id = session.execute(insert_query_tipo,
                                                                  {'id': id, 'descripcion': descripcion}).scalar()

                        # Insertar los datos en la tabla Transaccion
                        insert_query_transacciones = text(
                            "INSERT INTO Transacciones (fecha_id, transaccion_tipo_uuid, simbolo, precio, valor) VALUES (:fecha_id, :transaccion_tipo_id, :simbolo, :precio, :valor)"
                        )
                        session.execute(insert_query_transacciones, {
                            'fecha_id': fecha_id,
                            'transaccion_tipo_id': transaccion_tipo_id,
                            'simbolo': row['s'],
                            'precio': float(row['p']),
                            'valor': float(row['v'])
                        })

                        insert_count += 1


                    session.commit()
                    logging.info("Datos insertados correctamente para los registros %d a %d.", start, start + len(chunk) - 1)

except Exception as e:
    logging.error("Ocurrió un error: %s", e)

finally:
    # No es necesario cerrar la sesión aquí, ya que se utiliza 'with'
    pass