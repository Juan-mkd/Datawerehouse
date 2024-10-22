from sqlalchemy import create_engine, exc, text
from config import get_connection_string

def test_connection():
    connection_string = get_connection_string()
    engine = create_engine(connection_string)

    try:
        with engine.connect() as connection:
            # Ejecutar la consulta en la tabla 'fecha'
            result = connection.execute(text("SELECT * FROM fecha"))
            for row in result:
                print(row)  # Imprimir cada fila devuelta
    except exc.SQLAlchemyError as e:
        print("Connection failed:", str(e))

if __name__ == "__main__":
    test_connection()
