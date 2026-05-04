import time
import random
import json
import pika
import os

def conectar_rabbitmq():
    # Usamos variable de entorno para la URL (por defecto 'rabbitmq' para Kubernetes)
    RABBIT_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
    
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))
            channel = connection.channel()
            
            # Declaramos un exchange de tipo 'fanout' para enviar a los 4 consumidores a la vez
            channel.exchange_declare(exchange='telemetria_eolicos', exchange_type='fanout')
            
            print(f"[INFO] Productor conectado a RabbitMQ en {RABBIT_HOST}")
            return connection, channel
        except pika.exceptions.AMQPConnectionError:
            print(f"[WARN] RabbitMQ en {RABBIT_HOST} no disponible, reintentando en 5s...")
            time.sleep(5)

def simulador_parque_eolico():

    parque = [f"MOLINO-{i:02d}" for i in range(1, 21)]
    
    connection, channel = conectar_rabbitmq()
    
    try:
        while True:
            for molino_id in parque:
            
                if random.random() < 0.10:
                    viento = random.choice([None, -999, "ERROR"]) # Datos corruptos
                    vibracion = 999.9
                else:
                    viento = round(random.uniform(5, 125), 2)
                    vibracion = round(random.uniform(0.1, 7.5), 2)
                
                lectura = {
                    "id": molino_id,
                    "viento": viento,
                    "vibracion": vibracion
                }

                mensaje = json.dumps(lectura)
                
                # Publicamos el mensaje en el exchange 'telemetria_eolicos'
                # En tipo fanout no hace falta routing_key
                channel.basic_publish(
                    exchange='telemetria_eolicos',
                    routing_key='',
                    body=mensaje
                )
                
                print(f"[ENVIADO] {mensaje}")

            # Frecuencia de actualización cada 5 segundos
            time.sleep(5) 
            
    except KeyboardInterrupt:
        print("\n[INFO] Generador detenido correctamente.")
    finally:
        if 'connection' in locals() and connection.is_open:
            connection.close()

if __name__ == "__main__":
    simulador_parque_eolico()