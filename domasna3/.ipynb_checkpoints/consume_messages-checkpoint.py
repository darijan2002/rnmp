from kafka import KafkaConsumer

# Kafka configuration
bootstrap_servers = 'localhost:9092'
topics = ['sensors', 'result1', 'result2']
# TODO change the name of the topic here (result1 or result2) to verify that Flink produces the wanted results

# Create a Kafka consumer
consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers,
                         group_id='my_consumer_group',
                         auto_offset_reset='latest',  # Start reading from the beginning if no offset is stored
                         enable_auto_commit=False)  # Disable auto-commit to manually control offsets

consumer.subscribe(topics[1:])
"""
try:
	for message in consumer:
		print(f"[{message.timestamp}] Received message: {message.value.decode('utf-8')}")
except KeyboardInterrupt:
	pass
finally:
	# Close the consumer
	consumer.close()
"""


while True:
    messages = consumer.poll(1.0)
    for t, m in messages.items():
        print("-"*10)
        print(f"Topic {t}:")
        for x in m:
            print(f"[{t.topic}] {x.value.decode('utf-8')}")
