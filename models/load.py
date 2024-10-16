# models/loader.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

class Loader:
    def __init__(self, db_url):
        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)

    def load_data(self, data):
        session = self.Session()
        # Cambia 'your_dest_table_name' por el nombre de tu tabla de destino
        for item in data:
            session.execute("INSERT INTO your_dest_table_name (name, price_with_tax) VALUES (:name, :price_with_tax)",
                            {"name": item['name'], "price_with_tax": item['price_with_tax']})
        session.commit()
        session.close()
