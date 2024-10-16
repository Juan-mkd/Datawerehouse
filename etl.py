# etl.py
from controllers.controller import ETLController

if __name__ == "__main__":
    # Crear una instancia del controlador ETL
    etl_controller = ETLController()

    # Ejecutar el proceso ETL completo
    etl_controller.run_etl()
