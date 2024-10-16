import zipfile
import requests
import io
import  json
# from ..config import API_URL
"""
consultar si uno puedo acceder a un archivo global desde un archivo local

"""
# from urls import API_URL



class Extractor:
    def __init__(self, api_url):
        self.api_url = api_url

    def extract_data_from_api(self,):
        response = requests.get(self.api_url)
        response.raise_for_status()  # Esto lanzará una excepción si la respuesta es un error (código 4xx o 5xx)

        zipfile1 = zipfile.ZipFile(io.BytesIO(response.content))
        zipfile2 = zipfile1.namelist()
        zipfile3 = ' '.join(zipfile2)
        # print(zipfile1)
        # print(zipfile2)
        print("mostrando el type file")
        print(type(zipfile3))
        return zipfile3 # Devuelve la respuesta para su uso posterior



    def read_jsonl_file(self, jsonl_filename):
        # Usamos el objeto zipfile para leer el archivo directamente
        with zipfile.ZipFile(io.BytesIO(requests.get(self.api_url).content)) as zipfile1:
            with zipfile1.open(jsonl_filename) as f:
                for line in f:
                    # Decodificamos la línea a UTF-8 y convertimos a JSON
                    yield json.loads(line.decode('utf-8'))




# # URL de la API
# API_URL = "https://github.com/sferez/BybitMarketData/raw/main/data/ETH/2024-02-12/trades_ETH_2024-02-12.zip"

# # Crear una instancia del extractor
# extractor = Extractor(API_URL)

# # Extraer datos de la API y mostrar el contenido
# response = extractor.extract_data_from_api()



  
    

