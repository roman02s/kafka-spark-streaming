import json
import time

import pandas as pd
from kafka import KafkaProducer

# Конфигурация Kafka
kafka_server = "localhost:9092"  # Адрес Kafka сервера
topic_name = "house-data"  # Топик для отправки данных

# Инициализация продюсера Kafka
producer = KafkaProducer(
    bootstrap_servers=[kafka_server],
    value_serializer=lambda x: x,
)
# df = pd.read_csv("data/train.csv")

for index, data_row in enumerate(pd.read_csv("data/test.csv", chunksize=1), start=1):
    print(f"{index=}")
    data_row = data_row.iloc[0]
    print("data: ", data_row)
    # print(type(data_row))
    # print(data_row.to_dict())
    for_send = json.dumps(data_row.to_dict())
    print(for_send)
    producer.send(topic_name, bytes(for_send, encoding="utf-8"))
    time.sleep(5)
