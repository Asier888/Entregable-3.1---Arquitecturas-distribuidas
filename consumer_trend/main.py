import pika
import json
import os
import time
from collections import deque

# Variable de entorno para RabbitMQ
RABBIT_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')

# Diccionario para almacenar el historial en memoria
historial = {}

def procesar_mensaje(ch, method, properties, body):
    try:
        datos = json.loads(body)
        molino_id = datos.get("id")
        vibracion = datos.get("vibracion")

        # Filtramos datos corruptos o vacíos
        if vibracion is None or not isinstance(vibracion, (int, float)) or vibracion > 100:
            return

        # Si el molino no existe en nuestro historial, lo inicializamos
        if molino_id not in historial:
            historial[molino_id] = deque(maxlen=5)

        # Añadimos la lectura actual (si hay más de 5, borra la más antigua)
        historial[molino_id].append(vibracion)

        lecturas = list(historial[molino_id])
        
        # Analizamos la tendencia solo cuando ya tengamos los 5 datos
        if len(lecturas) == 5:
            # Comprobamos si es estrictamente ascendente (v1 < v2 < v3 < v4 < v5)
            es_ascendente = all(lecturas[i] < lecturas[i+1] for i in range(len(lecturas)-1))
            
            # Definimos que es "peligrosa" si la última vibración supera un umbral alto (ej. 5.5)
            es_peligrosa = lecturas[-1] > 5.5

            if es_ascendente and es_peligrosa:
                print(f"[PELIGRO TREND] ¡Alerta en {molino_id}! Tendencia ascendente peligrosa: {lecturas}")

    except json.JSONDecodeError:
        print("[ERROR] Mensaje mal formado.")

def main():
    while True:
        try:
            # Conexión a RabbitMQ
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))
            channel = connection.channel()
            
            # Mismo exchange 'fanout'
            channel.exchange_declare(exchange='telemetria_eolicos', exchange_type='fanout')
            
            result = channel.queue_declare(queue='', exclusive=True)
            queue_name = result.method.queue
            
            channel.queue_bind(exchange='telemetria_eolicos', queue=queue_name)
            
            print("[INFO] Consumidor Trend Analyzer iniciado y esperando mensajes...")
            
            channel.basic_consume(queue=queue_name, on_message_callback=procesar_mensaje, auto_ack=True)
            channel.start_consuming()
            
        except pika.exceptions.AMQPConnectionError:
            print("[WARN] RabbitMQ no disponible. Reintentando en 5s...")
            time.sleep(5)

if __name__ == '__main__':
    main()