import pandas as pd
from sqlalchemy import create_engine, exc, text
from config import get_connection_string


class DataLoader:
    def __init__(self):
        # Initialize the connection string and engine
        self.connection_string = get_connection_string()
        self.engine = create_engine(self.connection_string)


    def insert_fecha(self, connection, fecha_records):
        fecha_query = """
            INSERT INTO Fecha (fecha, dia, mes, anio)
            VALUES (%s, %s, %s, %s)
            RETURNING id, fecha;
        """
        fecha_id_map = {}
        for record in fecha_records:
            result = connection.execute(fecha_query, record)
            fecha_id = result.fetchone()[0]
            fecha_id_map[record[0]] = fecha_id  # Almacena el ID por fecha

        return fecha_id_map

    def insert_transacciones(self, connection, transacciones_records):
        """Insert records into Transacciones table."""
        transacciones_query = text("""
            INSERT INTO Transacciones (fecha_id, transaccion_tipo_id, simbolo, precio, valor)
            VALUES (:fecha_id, :transaccion_tipo_id, :simbolo, :precio, :valor);
        """)

        for record in transacciones_records:
            fecha_id, transaccion_tipo_id, simbolo, precio, valor = record
            connection.execute(transacciones_query, {
                'fecha_id': fecha_id,
                'transaccion_tipo_id': transaccion_tipo_id,
                'simbolo': simbolo,
                'precio': precio,
                'valor': valor
            })

    def get_transaccion_tipo_id(self, trade_side):
        """Get the UUID for the transaction type."""
        query = f"SELECT id FROM tipo_transaccion WHERE descripcion = '{trade_side}';"
        with self.engine.connect() as connection:
            return connection.execute(query).scalar()