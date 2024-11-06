import sys
import os
import argparse
import requests
import pandas as pd
from sqlalchemy import create_engine

# Agregar el directorio raíz del proyecto al sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.extract import Extractor
from models.transform import Transformer
from models.load import DataLoader  # Importar la clase DataLoader desde models.load
from config import DataConexion


class ETLController:
    def __init__(self, API_URL):
        self.API_URL = API_URL
        self.extractor = Extractor(API_URL)  # Instanciamos Extractor
        self.transformer = Transformer(self.extractor)  # Pasamos la instancia de Extractor a Transformer
        self.conexionDB = DataConexion()  # Conexión a la base de datos
        self.loader = DataLoader()  # Instanciamos el cargador de datos

    @staticmethod
    def obtener_url():
        """Obtener la URL completa a partir de los argumentos."""
        parser = argparse.ArgumentParser(description='Script para extraer datos de una API usando una URL construida.')
        parser.add_argument('--base_url', type=str, default='https://github.com/sferez/BybitMarketData/raw/main/data/', help='La URL base para construir la URL completa.')
        parser.add_argument('-r', '--url', type=str, required=True, help='Parte variable de la URL (ejemplo: datafile.json).')
        args = parser.parse_args()
        full_url = args.base_url + args.url
        return full_url

    def conexion_API(self):
        """Verificar conexión a la API."""
        print(f"Verificando la URL: {self.API_URL}")
        response = requests.get(self.API_URL)
        print(f'status de la conexion API => {response.status_code}')
        if response.status_code == 200:
            print("Conexión exitosa a la API")
        else:
            print("No se puede conectar a la API")

    def run_etl_extract(self):
        """Extraer datos de la API y devolver el nombre del archivo JSONL."""
        try:
            jsonl_filename = self.extractor.extract_data_from_api()
            print(f"Nombre del archivo JSONL: {jsonl_filename}")
            return jsonl_filename  # Retornar el nombre del archivo JSONL
        except Exception as e:
            print(f"Error al extraer los datos: {e}")
            return None

    def show_dataframe(self, jsonl_filename2):
        """Mostrar el DataFrame a partir del archivo JSONL."""
        try:
            print(f"Archivo JSONL: {jsonl_filename2}")
            df = self.transformer.jsonl_to_dataframe(jsonl_filename2)  # Convertir JSONL a DataFrame
            self.transformer.show_dataframe_header(df)  # Mostrar el encabezado del DataFrame

            # Imprimir las columnas del DataFrame
            print("Columnas del DataFrame:", df.columns)
            return df
        except Exception as e:
            print(f"Error al procesar el archivo JSONL: {e}")
            return None

    def validate_db_connection(self):
        """Verificar conexión a la base de datos desde DataLoader."""
        try:
            # Crear una instancia de DataConexion
            data_loader = DataConexion()

            # Verificar la conexión
            if data_loader.check_connection():
                print("La conexión a la base de datos fue exitosa.")
            else:
                print("La conexión a la base de datos falló. Por favor, revisa la configuración.")
        except Exception as e:
            print(f"Error al verificar la conexión a la base de datos: {e}")

    def load_data_to_db(self, df):
        """Cargar solo las fechas a la base de datos."""
        if df is not None:
            print("Cargando fechas a la base de datos...")
            self.loader.load_fecha(df)  # Llama al método load_fecha para cargar solo la fecha
        else:
            print("No se pudo cargar los datos, el DataFrame es None.")


if __name__ == "__main__":
    # Obtener la URL completa con los argumentos
    API_URL = ETLController.obtener_url()
    print(f'URL completa: {API_URL}')
    
    # Instanciar y configurar el controlador ETL
    etl_controller = ETLController(API_URL)
    
    # Verificar la conexión con la API
    etl_controller.conexion_API()

    # Extraer y transformar los datos
    jsonl_filename2 = etl_controller.run_etl_extract()
    if jsonl_filename2:
        df = etl_controller.show_dataframe(jsonl_filename2)  # Transformar el JSONL a DataFrame

        # Validar la conexión a la base de datos
        etl_controller.validate_db_connection()  # Aquí se debe llamar a la función

        # Cargar los datos en la base de datos
        etl_controller.load_data_to_db(df)
