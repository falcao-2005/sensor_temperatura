import json
import time
import random
import numpy as np
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    # Converte dicionário Python para JSON antes de enviar
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC = 'sensor-data'

print("Produtor iniciado. Enviando dados...")

while True:
    if(random.random() < 0.95):
        temperatura = random.gauss(mu=22.0, sigma=1.0)
    else:
        temperatura = random.choice([
            random.gauss(mu=50.0, sigma=2.0),
            random.gauss(mu=10.0, sigma=2.0),
        ])

    mensagem = {
        'sensor_id': f'sensor+{random.randint(1, 3)}',
        'temperatura': round(temperatura, 2),
        'timestamp': time.time()
    }

    producer.send(TOPIC, value=mensagem)
    print(f"Enviado: {mensagem}")

    time.sleep(0.5)