FROM python:3.11-slim

WORKDIR /app

# Instala dependências do sistema para o Firebird
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    wget \
    libfbclient2 && \
    rm -rf /var/lib/apt/lists/*

# Instala o driver Python do Firebird
RUN pip install --no-cache-dir fdb

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"]