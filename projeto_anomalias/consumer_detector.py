import json
import numpy as np
from collections import deque
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'sensor-data',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

historico = {}
JANELA = 50        
LIMIAR_ZSCORE = 3  

def detectar_anomalia(sensor_id, valor):
    """
    Usa Z-Score para detectar anomalias.
    Z-Score = (valor - média) / desvio_padrão
    Se |Z-Score| > 3, o ponto está a mais de 3 desvios da média → anomalia.
    """
    if sensor_id not in historico:
        historico[sensor_id] = deque(maxlen=JANELA)

    janela = historico[sensor_id]
    janela.append(valor)

    if len(janela) < 10:
        return False, 0.0

    media = np.mean(janela)
    desvio = np.std(janela)

    if desvio == 0:
        return False, 0.0

    z_score = (valor - media) / desvio
    return abs(z_score) > LIMIAR_ZSCORE, round(z_score, 2)

print("Detector iniciado. Aguardando mensagens...\n")

for mensagem in consumer:
    dados = mensagem.value
    sensor_id = dados['sensor_id']
    temperatura = dados['temperatura']
    timestamp = dados['timestamp']

    eh_anomalia, z_score = detectar_anomalia(sensor_id, temperatura)

    if eh_anomalia:
        print(f"ANOMALIA! Sensor: {sensor_id} | "
              f"Temp: {temperatura}°C | Z-Score: {z_score}")
    else:
        print(f"   Normal   | Sensor: {sensor_id} | "
              f"Temp: {temperatura}°C | Z-Score: {z_score}")