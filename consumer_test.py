from kafka import KafkaConsumer


KAFKA_HOST = "192.168.166.4"
KAFKA_PORT = "9092"
KAFKA_TOPIC = "user-behavior"

GROUP_ID = "test"

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    group_id=None,
    bootstrap_servers=[f"{KAFKA_HOST}:{KAFKA_PORT}"],
)


for msg in consumer:
    print(msg)
