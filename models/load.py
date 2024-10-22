# models/load.py

import pandas as pd
from sqlalchemy import create_engine
from config import get_connection_string

class Loader:
    def __init__(self):
        # Obtener la cadena de conexión
        self.connection_string = get_connection_string()
        self.engine = create_engine(self.connection_string)

    def save_to_postgres(self, dataframe, table_name):
        # Guardar el DataFrame en una tabla de PostgreSQL
        dataframe.to_sql(table_name, self.engine, if_exists='replace', index=False)
