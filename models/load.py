from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
from config import get_connection_string

class DataLoader:
    def __init__(self):
        self.connection_string = get_connection_string()
        self.engine = create_engine(self.connection_string)

    def load_fecha(self, df_chunk):
        """Cargar solo la columna 't' (fecha) en la tabla 'Fecha' a partir de un DataFrame."""
        fecha_values_list = []  # Lista para almacenar los valores a insertar

        for _, row in df_chunk.iterrows():
            try:
                # Convertir 't' (timestamp en milisegundos) a un formato adecuado (timestamp en segundos)
                fecha = datetime.utcfromtimestamp(row["t"] / 1000.0)  # Convertir a datetime
                
                # Agregar solo la fecha convertida a la lista de valores
                fecha_values_list.append({
                    "fecha": fecha
                })

            except Exception as e:
                print(f"Error al procesar la fecha {row['t']}: {e}")

        if fecha_values_list:
            try:
                # Usar text() para la consulta con parámetros
                query = text(
                    "INSERT INTO Fecha (fecha) "
                    "VALUES (:fecha) "
                    "ON CONFLICT (fecha) DO NOTHING;"
                )
                
                # Ejecutar la consulta con los valores usando execute
                with self.engine.connect() as connection:
                    connection.execute(query, fecha_values_list)
                print(f"Se cargaron {len(fecha_values_list)} fechas correctamente.")
            except SQLAlchemyError as e:
                # Imprimir el mensaje completo del error
                print(f"Error al cargar las fechas en la base de datos: {e}")  # Detalle del error en la base de datos

    def load_data_to_db(self, df):
        """Cargar solo las fechas a la base de datos."""
        print("Cargando fechas a la base de datos...")
        self.load_fecha(df)  # Llama al método load_fecha para cargar solo la fecha
