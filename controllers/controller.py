import sys
import os
import argparse


# Agregar el directorio raíz del proyecto al sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.extract import Extractor
from models.transform import Transformer
from models.load import DataLoader
from config import get_connection_string
from sqlalchemy import create_engine




class ETLController:
    def __init__(self, api_url):
        self.api_url = api_url
        self.extractor = Extractor(api_url)
        self.transformer = Transformer(api_url)
        self.loader = DataLoader()  # Inicializar el cargador de datos
        self.connection_string = get_connection_string()  # Obtener la cadena de conexión desde config

    def check_connection(self):
        """Verificar si se puede establecer la conexión a la base de datos."""
        try:
            # Crear el motor de conexión con SQLAlchemy
            engine = create_engine(self.connection_string)
            # Intentar conectarse
            with engine.connect() as connection:

                print("Conexión a la base de datos establecida con éxito.")
                return True
        except Exception as e:
            print(f"Error al conectar con la base de datos: {e}")
            return False

    def run_etl_extract(self):
        """Extraer datos de la API y guardarlos en un archivo JSONL."""
        jsonl_filename = self.extractor.extract_data_from_api()
        print(f"Nombre del archivo JSONL: {jsonl_filename}")

    def run_etl_process(self):
        """Ejecutar el proceso ETL: verificar conexión, transformar datos y cargar en la base de datos."""
        if not self.check_connection():  # Llama a la función de verificación de conexión
            print("No se pudo establecer la conexión con la base de datos. Terminando el proceso.")
            return

        dataframe = self.transformer.jsonl_to_dataframe()
        print("Contenido del DataFrame después de la transformación:")
        print(dataframe)

        # Continuar con la carga de datos
        print("Cargando datos en la base de datos...")
        self.loader.load_data(dataframe)
        print("Datos cargados en la base de datos con éxito.")









def obtener_url():
    # Crear el parser
    parser = argparse.ArgumentParser(description='Script para extraer datos de una API usando una URL construida.')

    # Agregar una opción para la URL base con un valor por defecto
    parser.add_argument(
        '--base_url',
        type=str,
        default='https://github.com/sferez/BybitMarketData/raw/main/data/',
        help='La URL base que se usará para construir la URL completa. (por defecto: https://github.com/sferez/BybitMarketData/raw/main/data/)'
    )

    # Agregar una opción para la parte variable de la URL
    parser.add_argument(
        '-r', '--url',
        type=str,
        required=True,
        help='Parte variable de la URL. Debe ser el nombre del archivo que deseas extraer (ejemplo: datafile.json).'
    )

    # Parsear los argumentos
    args = parser.parse_args()

    # Crear la URL completa concatenando la base URL con la parte variable
    full_url = args.base_url + args.url

    # Retornar la URL completa
    return full_url


if __name__ == "__main__":
    # Llamar a la función y obtener la URL
    API_URL = obtener_url()

    # Imprimir la URL completa
    print(f'URL completa: {API_URL}')

    # Iniciar el controlador ETL
    etl_controller = ETLController(API_URL)

    # Ejecutar el proceso de extracción y transformación/carga de datos
    etl_controller.run_etl_extract()
    etl_controller.run_etl_process()
