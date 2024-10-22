import pandas as pd
from .extract import Extractor
from sqlalchemy import create_engine
from config import  get_connection_string



class Transformer:
    def __init__(self, api_url):
        self.extractor = Extractor(api_url)

    def jsonl_to_dataframe(self):
        # Obtener el nombre del archivo JSONL
        jsonl_filename = self.extractor.extract_data_from_api()

        # Leer el archivo JSONL y convertirlo a una lista de datos
        data = []
        for objeto in self.extractor.read_jsonl_file(jsonl_filename):
            # Agregar cada objeto a la lista de datos
            data.append(objeto)

        # Crear el DataFrame
        df = pd.DataFrame(data)

        # Normalizar la columna 'd' y combinar con 't'
        if 'd' in df.columns:
            # Expandir la columna 'd' si contiene listas
            df_exploded = df.explode('d')
            # Normalizar la columna 'd' (que ahora puede contener diccionarios)
            df_normalized = pd.json_normalize(df_exploded['d'])

            # Combinar el DataFrame normalizado con la columna 't'
            final_df = pd.concat([df_exploded['t'].reset_index(drop=True), df_normalized.reset_index(drop=True)], axis=1)
            # print(type(final_df))
            # print("columns del dataframe que tiene el jsonl el type de dataframe es class 'pandas.core.frame.DataFrame'>")
            # print(final_df.columns)
            """
               Trades
                    The trade data is collected from the trade.{symbol} channel, which provides the following information:
                    T: the timestamp of the trade
                    s: the trading pair
                    S: the side of the trade (Buy or Sell)
                    v: the quantity of the trade
                    p: the price of the trade
                    L: the tick direction of the trade
                    i: the trade ID
                    BT: whether the trade is a block trade 
            """


        else:
            final_df = df  # Si no hay 'd', devolvemos el DataFrame original

        return final_df.columns



    def save_to_postgres(self,dataframe):
        # Obtener la cadena de conexión
        connection_string = get_connection_string()

        # Crear el motor de SQLAlchemy
        engine = create_engine(connection_string)