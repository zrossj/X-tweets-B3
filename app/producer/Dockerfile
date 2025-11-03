FROM python:3.11-slim

WORKDIR /app
COPY . /app

ENV TZ=America/Sao_Paulo

# Install system dependencies for librdkafka
RUN apt-get update && apt-get install -y \
    librdkafka-dev \
    gcc \
    tzdata \
    && rm -rf /var/lib/apt/lists/*


# Then install Python dependencies
RUN pip install -r requirements.txt

CMD ["python", "producer.py"]