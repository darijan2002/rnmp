#!/usr/bin/env python3

"""
Скрипта која на топикот `health_data` ги праќа сите редови од датасетот
`online.csv` во JSON формат.
"""

import json
import time

from kafka import KafkaProducer

import random
import pandas as pd

producer = KafkaProducer(
    bootstrap_servers='localhost:9092', security_protocol="PLAINTEXT")

dataset = pd.read_csv("data/online.csv")

data_json = dataset.drop(columns=["Diabetes_binary"]).apply(
    lambda x: x.to_json(), axis=1
).to_list()

for idx, row in enumerate(data_json):
    producer.send(
        topic="health_data",
        value=row.encode("utf-8"),
    )
    time.sleep(random.randint(500, 2000) / 1000.0)

