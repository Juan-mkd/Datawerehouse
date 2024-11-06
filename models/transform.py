import pandas as pd
from .extract import Extractor
from sqlalchemy import create_engine
from config import get_connection_string



class Transformer:
    def __init__(self, api_url):
        self.api_url = api_url
        self.extractor = Extractor(api_url)

    def jsonl_to_dataframe(self):
        """Extract data from JSONL file and convert it into a DataFrame."""
        jsonl_filename = self.extractor.extract_data_from_api()
        data = self.read_jsonl_data(jsonl_filename)
        final_df = self.create_dataframe(data)
        return final_df

    def read_jsonl_data(self, jsonl_filename):
        """Read data from a JSONL file and return it as a list."""
        return [objeto for objeto in self.extractor.read_jsonl_file(jsonl_filename)]

    def create_dataframe(self, data):
        """Create a DataFrame from the extracted data and normalize it."""
        df = pd.DataFrame(data)
        if 'd' in df.columns:
            df = self.normalize_data(df)
        return df

    def normalize_data(self, df):
        """Normalize the 'd' column in the DataFrame."""
        df_exploded = df.explode('d')
        df_normalized = pd.json_normalize(df_exploded['d'])
        final_df = pd.concat([df_exploded['t'].reset_index(drop=True), df_normalized.reset_index(drop=True)], axis=1)
        return self.rename_columns(final_df)

    def rename_columns(self, df):
        """Rename columns based on the specified mapping."""
        column_mapping = {
            'T': 'Timestamp',
            's': 'Trading_pair',
            'S': 'Trade_Side',
            'v': 'Quantity',
            'p': 'Price',
            'L': 'Tick',
            'i': 'Trade',
            'BT': 'Block'
        }
        
        df.rename(columns=column_mapping, inplace=True)
        return df