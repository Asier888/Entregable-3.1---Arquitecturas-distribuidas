import pika
import json
import os
import time
from pymongo import MongoClient

# Variables de entorno para las conexiones
RABBIT_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
MONGO_HOST = os.getenv('MONGO_HOST', 'mongodb')

def init_mongo():
    """Conecta a MongoDB y devuelve la colección donde guardaremos los datos."""
    while True:
        try:
            # Conexión a MongoDB (puerto 27017 por defecto)
            client = MongoClient(f'mongodb://{MONGO_HOST}:27017/', serverSelectionTimeoutMS=5000)
            client.server_info() # Fuerza una petición para comprobar que el servidor responde
            
            db = client.eolicos_db
            coleccion = db.telemetria
            print("[INFO] Conectado a MongoDB.")
            return coleccion
        except Exception as e:
            print(f"[WARN] Esperando a MongoDB en {MONGO_HOST}...")
            time.sleep(5)

def main():
    coleccion = init_mongo()

    while True:
        try:
            # Conectamos a RabbitMQ
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))
            channel = connection.channel()
            
            # Mismo exchange 'fanout'
            channel.exchange_declare(exchange='telemetria_eolicos', exchange_type='fanout')
            
            result = channel.queue_declare(queue='', exclusive=True)
            queue_name = result.method.queue
            
            channel.queue_bind(exchange='telemetria_eolicos', queue=queue_name)
            
            print("[INFO] Consumidor NoSQL (BASE) esperando mensajes...")
            
            def procesar_mensaje(ch, method, properties, body):
                try:
                    datos = json.loads(body)
                    # Mongo insertará el documento entero automáticamente
                    coleccion.insert_one(datos)
                    print(f"[NoSQL] Documento guardado en Mongo: Molino {datos.get('id')}")
                except Exception as e:
                    print(f"[ERROR Mongo] No se pudo guardar: {e}")
            
            channel.basic_consume(queue=queue_name, on_message_callback=procesar_mensaje, auto_ack=True)
            channel.start_consuming()
            
        except pika.exceptions.AMQPConnectionError:
            print("[WARN] RabbitMQ no disponible. Reintentando en 5s...")
            time.sleep(5)

if __name__ == '__main__':
    main()