from kafka import KafkaProducer
import json
import time

# Configuración de Kafka
KAFKA_BROKER_IP = "localhost:9092"  # Cambia si tu entorno usa otra IP o puerto
KAFKA_TOPIC = "clientes"            # Usa el nombre del topic que te haya indicado el profesor
INPUT_FILE = "data.json"            # Archivo JSON con los datos de entrada

# Creación del productor
productor = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER_IP,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Productor de Kafka iniciado correctamente")

# Lectura del archivo de datos
with open(INPUT_FILE, "r") as file:
    datos = json.load(file)

print(f"Enviando {len(datos)} mensajes al topic '{KAFKA_TOPIC}'...")

# Envío de los mensajes al topic
for registro in datos:
    productor.send(KAFKA_TOPIC, value=registro)
    print(f"Mensaje enviado: {registro}")
    time.sleep(0.2)  # Simula un flujo en streaming

# Finalización
productor.flush()
print("Envío completado. Todos los mensajes han sido publicados.")
