import requests
# from ..config import API_URL
"""
consultar si uno puedo acceder a un archivo global desde un archivo local

"""
# from urls import API_URL


class Extractor:
    def __init__(self, api_url):
        self.api_url = api_url

    def extract_data_from_api(self):
        response = requests.get(self.api_url)
        response.raise_for_status()  # Esto lanzará una excepción si la respuesta es un error (código 4xx o 5xx)
        # Accede a status_code sin paréntesis
        print("Código de estado:", response.status_code)  # Muestra el código de estado
        print(response.content)
        
        

        return response  # Devuelve la respuesta para su uso posterior

# # URL de la API
# API_URL = "https://github.com/sferez/BybitMarketData/raw/main/data/ETH/2024-02-12/trades_ETH_2024-02-12.zip"

# # Crear una instancia del extractor
# extractor = Extractor(API_URL)

# # Extraer datos de la API y mostrar el contenido
# response = extractor.extract_data_from_api()



  
    

