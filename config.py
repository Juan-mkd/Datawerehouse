import os
# Configuraciones generales del proceso ETL
# SOURCE_DB_URL = "mssql+pyodbc://username:password@source_db?driver=ODBC+Driver+17+for+SQL+Server"
# DEST_DB_URL = "mssql+pyodbc://username:password@dest_db?driver=ODBC+Driver+17+for+SQL+Server"
# https://github.com/sferez/BybitMarketData/raw/main/data/BTC/2024-02-12/trades_BTC_2024-02-12.zip
# https://github.com/sferez/BybitMarketData/raw/main/data/ETH/2024-02-12/trades_ETH_2024-02-12.zip
# https://github.com/sferez/BybitMarketData/raw/main/data/SOL/2024-02-12/trades_SOL_2024-02-12.zip
API_URL = "https://github.com/sferez/BybitMarketData/raw/main/data/SOL/2024-02-12/trades_SOL_2024-02-12.zip"

# config.py

DATABASE_CONFIG = {
    'server': 'localhost',
    'port': '5433',
    'database': 'datadb',
    'username': 'juan',
    'password': '147'
}

def get_connection_string():
    server = DATABASE_CONFIG['server']
    port = DATABASE_CONFIG['port']
    database = DATABASE_CONFIG['database']
    username = DATABASE_CONFIG['username']
    password = DATABASE_CONFIG['password']

    # Usa psycopg2 en la cadena de conexión
    connection_string = (
        f"postgresql+psycopg2://{username}:{password}@{server}:{port}/{database}"
    )

    return connection_string
