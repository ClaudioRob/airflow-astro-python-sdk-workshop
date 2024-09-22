import os

path_pattern = '/usr/local/airflow/dags/dags/data/user/user*'
path = path_pattern.rstrip('*')  # Remove o '*' para verificar o diretório base

print(f"Verificando no diretório: {path}")

# Listar arquivos no diretório base
files = os.listdir(path)
print(f"Arquivos encontrados: {files}")

# Verificar se o padrão de arquivo existe
import glob
matched_files = glob.glob(path_pattern)
print(f"Arquivos que correspondem ao padrão: {matched_files}")
