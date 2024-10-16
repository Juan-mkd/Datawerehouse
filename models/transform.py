# models/transformer.py
class Transformer:
    def transform_data(self, data):
        # Realiza la transformación de los datos (p. ej., normalización)
        transformed_data = []
        for item in data:
            transformed_item = {
                'name': item['name'].upper(),
                'price_with_tax': item['price'] * 1.15  # Ejemplo de transformación
            }
            transformed_data.append(transformed_item)
        return transformed_data
