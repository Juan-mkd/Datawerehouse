import argparse

# Crear el parser
parser = argparse.ArgumentParser(description='url para extraer datos.')

# Agregar un argumento 
parser.add_argument('url', type=str, help='api')

# Agregar una opción para
parser.add_argument('--r', type=str, default='url', help='url del la api')

# Parsear los argumentos
args = parser.parse_args()

# Imprimir 
print(f'{args.saludo}, {args.r}')