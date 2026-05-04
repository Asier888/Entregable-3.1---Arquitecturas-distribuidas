import time
import random
import json

def simulador_parque_eolico():

    parque = [f"MOLINO-{i:02d}" for i in range(1, 21)]
    
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

                print(json.dumps(lectura))
                print("-" * 50)

            # Frecuencia de actualización cada 5 segundos
            time.sleep(5) 
            
    except KeyboardInterrupt:
        print("\n[INFO] Generador detenido correctamente.")

if __name__ == "__main__":
    simulador_parque_eolico()