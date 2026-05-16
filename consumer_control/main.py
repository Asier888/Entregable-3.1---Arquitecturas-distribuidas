import pika
import json
import os
import time

# Variable de entorno para RabbitMQ
RABBIT_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')

def procesar_mensaje(ch, method, properties, body):
    """Callback que evalúa la vibración y simula la maniobra física."""
    try:
        datos = json.loads(body)
        molino_id = datos.get("id")
        vibracion = datos.get("vibracion")

        # Ignorar datos corruptos donde la vibración no sea un número válido
        if vibracion is None or type(vibracion) is not float:
            return

        print(f"[VISTA] Molino {molino_id} - Vibración: {vibracion}")

        # Reto G3: Simular maniobra física si vibración > 4
        if vibracion > 4:
            print(f"  [!] ALERTA: Vibración alta en {molino_id}. Iniciando maniobra física...")
            # Bloqueamos el hilo durante 3 segundos
            time.sleep(3) 
            print(f"  [OK] Maniobra física en {molino_id} completada.")

    except json.JSONDecodeError:
        print("[ERROR] Mensaje mal formado.")

def main():
    while True:
        try:
            # Conectamos a RabbitMQ
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))
            channel = connection.channel()
            
            # Nos conectamos al mismo exchange 'fanout'
            channel.exchange_declare(exchange='telemetria_eolicos', exchange_type='fanout')
            
            result = channel.queue_declare(queue='', exclusive=True)
            queue_name = result.method.queue
            
            channel.queue_bind(exchange='telemetria_eolicos', queue=queue_name)
            
            print("[INFO] Consumidor de Control (G3) iniciado y esperando mensajes...")
            
            channel.basic_consume(queue=queue_name, on_message_callback=procesar_mensaje, auto_ack=True)
            channel.start_consuming()
            
        except pika.exceptions.AMQPConnectionError:
            print("[WARN] RabbitMQ no disponible. Reintentando en 5s...")
            time.sleep(5)

if __name__ == '__main__':
    main()