import fileinput
import json

from kafka import KafkaProducer


KAFKA_HOST = "192.168.2.115"
KAFKA_PORT = "9092"
KAFKA_TOPIC = "user-behavior"

producer = KafkaProducer(
    bootstrap_servers=[f"{KAFKA_HOST}:{KAFKA_PORT}"],
    value_serializer=lambda m: json.dumps(m, ensure_ascii=False).encode()
)

for line in fileinput.input("./data/UserBehavior.csv"):
    line_splited = line.strip().split(",")
    data = {
        "userId": line_splited[0],
        "itemId": line_splited[1],
        "categoryId": line_splited[2],
        "bahavior": line_splited[3],
        "timestamp": line_splited[4]
    }
    producer.send(KAFKA_TOPIC, data)
    print("Success", data)
