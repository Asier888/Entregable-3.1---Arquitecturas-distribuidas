import pika
import json
import os
import time
import psycopg2
from datetime import datetime

# Configuraciones por variables de entorno (con valores por defecto para Kubernetes)
RABBIT_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
PG_HOST = os.getenv('POSTGRES_HOST', 'postgresql')
PG_USER = os.getenv('POSTGRES_USER', 'operadores')
PG_PASS = os.getenv('POSTGRES_PASSWORD', 'password123')
PG_DB = os.getenv('POSTGRES_DB', 'alertas_db')

# Diccionario en memoria para rastrear qué molinos están actualmente en alerta
alertas_activas = {}

def init_db():
    """Conecta a PostgreSQL y crea la tabla si no existe."""
    while True:
        try:
            conn = psycopg2.connect(host=PG_HOST, user=PG_USER, password=PG_PASS, dbname=PG_DB)
            cursor = conn.cursor()
            # Enfoque ACID: Tabla relacional estructurada
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS registro_alertas (
                    id SERIAL PRIMARY KEY,
                    molino_id VARCHAR(50),
                    estado VARCHAR(50),
                    fecha_hora TIMESTAMP
                )
            ''')
            conn.commit()
            print("[INFO] Conectado a PostgreSQL y tabla preparada.")
            return conn, cursor
        except psycopg2.OperationalError:
            print(f"[WARN] Esperando a PostgreSQL en {PG_HOST}...")
            time.sleep(5)

def registrar_evento(conn, cursor, molino_id, estado):
    """Inserta el evento de activación/restablecimiento en la base de datos."""
    try:
        ahora = datetime.now()
        cursor.execute(
            "INSERT INTO registro_alertas (molino_id, estado, fecha_hora) VALUES (%s, %s, %s)",
            (molino_id, estado, ahora)
        )
        conn.commit()
        print(f"[SQL G3] Guardado: {molino_id} -> {estado} a las {ahora}")
    except Exception as e:
        print(f"[ERROR SQL] {e}")
        conn.rollback() # ACID: rollback en caso de error

def procesar_mensaje(ch, method, properties, body, conn_db, cursor):
    """Callback que se ejecuta con cada mensaje recibido de RabbitMQ."""
    try:
        datos = json.loads(body)
        molino_id = datos.get("id")
        vibracion = datos.get("vibracion")

        if vibracion is None:
            return

        # Comprobamos el estado actual guardado en nuestro diccionario
        en_alerta = alertas_activas.get(molino_id, False)

        # Lógica del Grupo 3: Si la vibración supera 4 y no estaba en alerta, se activa.
        if vibracion > 4 and not en_alerta:
            alertas_activas[molino_id] = True
            registrar_evento(conn_db, cursor, molino_id, "ALERTA ACTIVADA")
        
        # Si la vibración baja de 4 (o igual) y SÍ estaba en alerta, se restablece.
        elif vibracion <= 4 and en_alerta:
            alertas_activas[molino_id] = False
            registrar_evento(conn_db, cursor, molino_id, "ALERTA RESTABLECIDA")

    except json.JSONDecodeError:
        print("[ERROR] Mensaje mal formado.")

def main():
    conn_db, cursor = init_db()

    while True:
        try:
            # Conexión a RabbitMQ
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))
            channel = connection.channel()
            
            # Nos conectamos al mismo exchange 'fanout' del productor
            channel.exchange_declare(exchange='telemetria_eolicos', exchange_type='fanout')
            
            # Creamos una cola exclusiva temporal para este consumidor
            result = channel.queue_declare(queue='', exclusive=True)
            queue_name = result.method.queue
            
            channel.queue_bind(exchange='telemetria_eolicos', queue=queue_name)
            
            print("[INFO] Consumidor SQL (G3) esperando mensajes...")
            
            # Pasamos la conexión DB a la función que procesa los mensajes usando un lambda
            callback = lambda ch, method, properties, body: procesar_mensaje(ch, method, properties, body, conn_db, cursor)
            
            channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
            channel.start_consuming()
            
        except Exception as e:
            print(f"[ERROR CRÍTICO] Fallo en la conexión: {e}. Reiniciando contenedor...")
            time.sleep(5)
            os._exit(1)

if __name__ == '__main__':
    main()