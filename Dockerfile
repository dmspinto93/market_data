# Usa uma imagem oficial do Python mais leve
FROM python:3.10-slim

# Define a pasta de trabalho dentro do contêiner
WORKDIR /app

# Copia o arquivo de requisitos e instala as bibliotecas
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia todo o resto do seu código para dentro do contêiner
COPY . .

# Comando que será executado quando o contêiner iniciar
CMD ["python", "src/main.py"]