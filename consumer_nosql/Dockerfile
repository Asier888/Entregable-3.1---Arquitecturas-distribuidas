FROM python:3.9-slim

WORKDIR /app

# Instalamos pika y pymongo
RUN pip install --no-cache-dir pika pymongo

COPY main.py .

CMD ["python", "-u", "main.py"]