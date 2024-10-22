import sys
import os

# Agregar el directorio raíz del proyecto al sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from models.extract import Extractor
from models.load import Loader
from models.transform import Transformer

# from models.transform import Transformer  # Comentar temporalmente si no se utiliza
# from models.load import Loader  # Comentar temporalmente si no se utiliza
# from views.report import Report  # Comentar temporalmente si no se utiliza
from config import API_URL  # Cambia esto para que coincida con tu configuración

class ETLController:
    def __init__(self):
        self.extractor = Extractor(API_URL)
        self.transformer = Transformer(API_URL)
        self.loader = Loader()
        # self.loader = Loader(DEST_DB_URL)
        # self.report = Report()

    def run_etl_extract(self):
        # 1. Extraer datos
        response = self.extractor.extract_data_from_api()  # Llama al extractor

        # print(response) # Muestra el contenido del ZIP

        jsonl_filename = self.extractor.extract_data_from_api()  # Llama al extractor y obtiene el nombre del archivo JSONL

        # Mostrar el nombre del archivo JSONL, que el nombre es un str
        print(f"Nombre del archivo JSONL: {jsonl_filename}")

        """
        musestra el contenido del archivo 
        
        """
        # # Leer el contenido del archivo JSONL
        # generador = self.extractor.read_jsonl_file(jsonl_filename)
        #
        # # Iterar sobre el generador para obtener los objetos JSON
        # for objeto in generador:
        #     print(objeto)

    def run_etl_process(self):
        # Validar la conexión a la base de datos
        if not self.loader.check_connection():
            print("No se pudo establecer la conexión con la base de datos. Terminando el proceso.")
            return  # Termina el proceso si la conexión falla

        # Transformar a DataFrame
        content = self.transformer.jsonl_to_dataframe()

        # Mostrar el contenido del DataFrame
        print(content)

        # Guardar el DataFrame en PostgreSQL
        self.loader.save_to_postgres(content, 'experiment')  # Asegúrate de que el nombre de la tabla sea correcto
        print("Datos guardados en PostgreSQL.")




etl_controller = ETLController()


etl_controller.run_etl_extract()
etl_controller.run_etl_process()