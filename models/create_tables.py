# create_tables.py
from sqlalchemy import create_engine
import sys,os
from sqlalchemy.orm import sessionmaker
from models import Base  # Asegúrate de importar tu Base desde donde esté definido
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DataConexion  # Asegúrate de importar tu clase DataConexion


# Configura la conexión a la base de datos
data_conexion = DataConexion()
engine = data_conexion.engine

# Elimina las tablas existentes (opcional)
Base.metadata.drop_all(engine)  # Esto eliminará las tablas existentes

# Crea todas las tablas en la base de datos
Base.metadata.create_all(engine)  # Esto creará las tablas de nuevo

print("Tablas creadas correctamente.")