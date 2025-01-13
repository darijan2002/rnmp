#!/usr/bin/env python3

"""
Скрипта која прима пораки од топикот `health_data_predicted` и ги печати на
екран. Секоја порака е оригиналниот примерок од датасетот `online.csv` со
додадена колона за предвидувањето на моделот.
"""
from kafka import KafkaConsumer
import json

# Kafka configuration
bootstrap_servers = 'localhost:9092'
topics = ['health_data_predicted']

# Create a Kafka consumer
consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers,
                         group_id='my_consumer_group',
                        #  auto_offset_reset='earliest',  
                         auto_offset_reset='latest',  
                         enable_auto_commit=False)  

consumer.subscribe(topics)

while True:
    messages = consumer.poll(1.0)
    for t, m in messages.items():
        print("-"*10)
        print(f"Topic {t}:")
        for x in m:
            j = x.value.decode('utf-8')
            o = json.loads(j)
            print(f"[{t.topic}] {o}")
