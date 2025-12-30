FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libxml2-dev \
    libxslt-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY main.py .

RUN mkdir -p /app/config
COPY config.yaml /app/config/config.yaml

ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "main.py"]

CMD ["/app/config/config.yaml"]
